package sqlite

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"time"

	"crawshaw.io/sqlite"
	"crawshaw.io/sqlite/sqlitex"
	minio "github.com/minio/minio/cmd"
	"github.com/minio/minio/cmd/debug"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/madmin"
	"github.com/minio/minio/pkg/policy"
	"github.com/skyrings/skyring-common/tools/uuid"
)

// TODO: etags
// TODO: parse metadata

// TODO: copied
const minioMetaBucket = ".minio.sys"

func init() {
	// TODO: remove awful hack
	minio.NewSQLiteLayer = NewSQLiteLayer
}

type SQLite struct {
	pool   *sqlitex.Pool
	closed uint32
}

var (
	errDBClosed = errors.New("db closed")
)

func NewSQLiteLayer(uri *url.URL) (minio.ObjectLayer, error) {
	if uri.Scheme == "sqlite" {
		uri.Scheme = "file"
	}
	const poolSize = 100 // TODO: what size?
	pool, err := sqlitex.Open(uri.String(), 0, poolSize)
	if err != nil {
		return nil, err
	}

	s := &SQLite{
		pool: pool,
	}

	err = s.init()
	if err != nil {
		return nil, err
	}

	return s, nil
	return &debug.DebugLayer{Wrapped: s, LogReturns: false, LogCallers: 1}, nil
}

// Multipart uploads are maintained as individual parts to avoid
// the overhead of recombining on completion. This also allows objects
// larger than SQLITE_MAX_LENGTH to be stored.

// timestamps are nanoseconds since unix epoch
const sqlInit = `
	CREATE TABLE IF NOT EXISTS buckets (
		bucket  TEXT PRIMARY KEY NOT NULL,
		policy  BLOB,
		created INTEGER NOT NULL
	);
	CREATE TABLE IF NOT EXISTS blobs (
		id      INTEGER NOT NULL,
		part_id INTEGER NOT NULL DEFAULT 0,
		data    BLOB NOT NULL,
		PRIMARY KEY (id, part_id)
	);
	CREATE TABLE IF NOT EXISTS objects (
		bucket   TEXT REFERENCES buckets ON DELETE CASCADE ON UPDATE CASCADE,
		object   TEXT NOT NULL,
		metadata TEXT,
		size     INTEGER NOT NULL,
		modified INTEGER NOT NULL,
		blob_id  INTEGER NOT NULL,
		PRIMARY KEY (bucket, object)
	) WITHOUT ROWID;
	CREATE TABLE IF NOT EXISTS uploads (
		id       TEXT PRIMARY KEY NOT NULL,
		bucket   TEXT REFERENCES buckets ON DELETE CASCADE ON UPDATE CASCADE,
		object   TEXT NOT NULL,
		metadata TEXT,
		blob_id  INTEGER NOT NULL
	);
	CREATE TRIGGER IF NOT EXISTS remove_object AFTER DELETE ON objects
	BEGIN
		DELETE FROM blobs WHERE id = OLD.blob_id AND NOT EXISTS (SELECT 1 FROM objects WHERE blob_id = OLD.blob_id);
	END;
	CREATE TRIGGER IF NOT EXISTS update_object AFTER UPDATE OF blob_id ON objects
	BEGIN
		DELETE FROM blobs WHERE id = OLD.blob_id AND NOT EXISTS (SELECT 1 FROM objects WHERE blob_id = OLD.blob_id);
	END;`

func columnTime(stmt *sqlite.Stmt, col int) time.Time {
	return time.Unix(0, stmt.ColumnInt64(col))
}

func (s *SQLite) init() error {
	ctx := context.Background()

	conn, err := s.getConn(ctx)
	if err != nil {
		return err
	}
	defer s.pool.Put(conn)

	err = sqlitex.ExecScript(conn, sqlInit)
	if err != nil {
		return err
	}

	// TODO: is this necessary?
	_, err = s.GetBucketInfo(ctx, minioMetaBucket)
	if err == nil {
		return nil
	}

	return s.MakeBucketWithLocation(ctx, minioMetaBucket, "")
}

func (s *SQLite) Shutdown(context.Context) error {
	atomic.StoreUint32(&s.closed, 1)
	return s.pool.Close()
}

func (s *SQLite) StorageInfo(ctx context.Context) minio.StorageInfo {
	size, _ := s.dbSize(ctx)

	si := minio.StorageInfo{Used: uint64(size)}
	si.Backend.Type = minio.BackendSQLite

	return si
}

// dbSize reports the size of the database or 0 if there is an error.
func (s *SQLite) dbSize(ctx context.Context) (int64, error) {
	conn, err := s.getConn(ctx)
	if err != nil {
		return 0, err
	}
	defer s.pool.Put(conn)

	return sqlitex.ResultInt64(conn.Prep("SELECT page_count * page_size FROM pragma_page_count(), pragma_page_size();"))
}

func (s *SQLite) getConn(ctx context.Context) (*sqlite.Conn, error) {
	conn := s.pool.Get(ctx)
	if conn == nil {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return nil, errDBClosed // TODO: minio error
	}
	// Set very long busy timeout. The context done channel passed when getting
	// the connection from the pool should always apply.
	conn.SetBusyTimeout(5 * time.Minute)

	err := sqlitex.ExecTransient(conn, "PRAGMA foreign_keys = ON;", nil)
	if err != nil {
		return nil, err
	}

	return conn, nil
}

const (
	errSourceUnknown = iota
	errSourceBuckets
	errSourceUploads
	errSourceParts
	errSourceObjects
)

func toMinioError(err error, source int, srcLabel string) error {
	switch err := err.(type) {
	case nil:
		return nil
	case sqlite.Error:
		switch err.Code {
		case sqlite.SQLITE_CONSTRAINT_PRIMARYKEY:
			switch source {
			case errSourceBuckets:
				return minio.BucketExists{Bucket: srcLabel}
			case errSourceObjects:
				return minio.ObjectNotFound{Object: srcLabel} // TODO: include bucket
			}
		case sqlite.SQLITE_FULL:
			return minio.StorageFull{}
		case sqlite.SQLITE_BUSY:
			return minio.OperationTimedOut{}
		}
	}
	return err
}

// Bucket operations.
func (s *SQLite) MakeBucketWithLocation(ctx context.Context, bucket string, location string) error {
	if !minio.IsValidBucketName(bucket) {
		return minio.BucketNameInvalid{Bucket: bucket}
	}

	conn, err := s.getConn(ctx)
	if err != nil {
		return err
	}
	defer s.pool.Put(conn)

	stmt := conn.Prep("INSERT INTO buckets (bucket, created) VALUES (?, ?);")
	stmt.BindText(1, bucket)
	stmt.BindInt64(2, time.Now().UnixNano())
	_, err = stmt.Step()
	return toMinioError(err, errSourceBuckets, bucket)
}

func (s *SQLite) GetBucketInfo(ctx context.Context, bucket string) (minio.BucketInfo, error) {
	conn, err := s.getConn(ctx)
	if err != nil {
		return minio.BucketInfo{}, err
	}
	defer s.pool.Put(conn)

	stmt := conn.Prep("SELECT created FROM buckets WHERE bucket = ?;")
	stmt.BindText(1, bucket)

	bi := minio.BucketInfo{Name: bucket}
	for {
		hasRow, err := stmt.Step()
		if err != nil {
			return bi, err
		}
		if !hasRow {
			break
		}

		bi.Created = columnTime(stmt, 0)
	}

	if bi.Created.IsZero() {
		return bi, minio.BucketNotFound{Bucket: bucket}
	}

	return bi, nil
}
func (s *SQLite) ListBuckets(ctx context.Context) ([]minio.BucketInfo, error) {
	conn, err := s.getConn(ctx)
	if err != nil {
		return nil, err
	}
	defer s.pool.Put(conn)

	stmt := conn.Prep("SELECT bucket, created FROM buckets;")

	var buckets []minio.BucketInfo
	for {
		hasRow, err := stmt.Step()
		if err != nil {
			return nil, err
		}
		if !hasRow {
			break
		}

		// TODO: filter reserved/invalid bucket names

		buckets = append(buckets, minio.BucketInfo{
			Name:    stmt.GetText("bucket"),
			Created: time.Unix(0, stmt.GetInt64("created")),
		})
	}

	return buckets, nil
}

func (s *SQLite) DeleteBucket(ctx context.Context, bucket string) (err error) {
	conn, err := s.getConn(ctx)
	if err != nil {
		return err
	}
	defer s.pool.Put(conn)

	stmt := conn.Prep("DELETE FROM buckets WHERE bucket = ?;")
	stmt.BindText(1, bucket)
	_, err = stmt.Step()
	return toMinioError(err, errSourceBuckets, bucket)
}

func (s *SQLite) ListObjects(ctx context.Context, bucket, prefix, marker, delimiter string, maxKeys int) (minio.ListObjectsInfo, error) {
	conn, err := s.getConn(ctx)
	if err != nil {
		return minio.ListObjectsInfo{}, err
	}
	defer s.pool.Put(conn)

	// TODO: all fields
	// TODO: is "object > marker" adequate?
	var stmt *sqlite.Stmt
	if delimiter == "" {
		stmt = conn.Prep("SELECT object as name, size, modified FROM objects WHERE bucket = ? AND object LIKE ? || '%' AND object > ? ORDER BY object LIMIT ?;")
		stmt.BindText(1, bucket)
		stmt.BindText(2, prefix)
		stmt.BindText(3, marker)
		stmt.BindInt64(4, int64(maxKeys)+1)
	} else {
		stmt = conn.Prep(`
		SELECT DISTINCT $prefix || substr(ltrim(object, $prefix), 1, instr(ltrim(object, $prefix), $delimiter)) as name, 0 as size, 0 as modified, 1 as is_prefix
		FROM objects
		WHERE bucket = $bucket AND object LIKE $prefix || '%' AND object > $marker AND name <> $prefix
		UNION
		SELECT object as name, size, modified, 0 as is_prefix
		FROM objects
		WHERE bucket = $bucket AND object LIKE $prefix || '%' AND object > $marker AND instr(ltrim(object, $prefix), $delimiter) = 0
		ORDER BY object
		LIMIT $max_keys;`)
		stmt.SetText("$bucket", bucket)
		stmt.SetText("$prefix", prefix)
		stmt.SetText("$marker", marker)
		stmt.SetText("$delimiter", delimiter)
		stmt.SetInt64("$max_keys", int64(maxKeys)+1)
	}

	var objects minio.ListObjectsInfo
	var count int
	for {
		hasRow, err := stmt.Step()
		if err != nil {
			return objects, err
		}
		if !hasRow {
			break
		}

		count++
		if count > maxKeys {
			objects.IsTruncated = true
			continue
		}

		var (
			name     = stmt.GetText("name")
			size     = stmt.GetInt64("size")
			modified = time.Unix(0, stmt.GetInt64("modified"))
			isPrefix = stmt.GetInt64("is_prefix") == 1
		)
		objects.NextMarker = name
		if isPrefix {
			objects.Prefixes = append(objects.Prefixes, name)
			continue
		}
		objects.Objects = append(objects.Objects, minio.ObjectInfo{
			Bucket:  bucket,
			Name:    name,
			Size:    size,
			ModTime: modified,
		})
	}

	return objects, nil
}
func (s *SQLite) ListObjectsV2(ctx context.Context, bucket, prefix, continuationToken, delimiter string, maxKeys int, fetchOwner bool, startAfter string) (minio.ListObjectsV2Info, error) {
	// TODO: this implementation copied from FS, safe to assume it's correct?
	marker := continuationToken
	if marker == "" {
		marker = startAfter
	}

	v1, err := s.ListObjects(ctx, bucket, prefix, marker, delimiter, maxKeys)
	if err != nil {
		return minio.ListObjectsV2Info{}, err
	}

	return minio.ListObjectsV2Info{
		IsTruncated:           v1.IsTruncated,
		ContinuationToken:     continuationToken,
		NextContinuationToken: v1.NextMarker,
		Objects:               v1.Objects,
		Prefixes:              v1.Prefixes,
	}, err
}

// Object operations.

// GetObjectNInfo returns a minio.GetObjectReader that satisfies the
// ReadCloser interface. The Close method unlocks the object
// after reading, so it must always be called after usage.
//
// IMPORTANTLY, when implementations return err != nil, this
// function MUST NOT return a non-nil ReadCloser.
func (s *SQLite) GetObjectNInfo(ctx context.Context, bucket, object string, rs *minio.HTTPRangeSpec, h http.Header, lockType minio.LockType, opts minio.ObjectOptions) (*minio.GetObjectReader, error) {
	conn, err := s.getConn(ctx)
	if err != nil {
		return nil, err
	}
	connInUse := false
	defer func() {
		if !connInUse {
			s.pool.Put(conn)
		}
	}()

	stmt := conn.Prep("SELECT object, blob_id, modified FROM objects WHERE bucket = ? AND object LIKE ? || '%' ORDER BY object LIMIT 1;")
	stmt.BindText(1, bucket)
	stmt.BindText(2, object)

	var (
		rowID    int64
		name     string
		modified time.Time
	)
	for {
		hasRow, err := stmt.Step()
		if err != nil {
			return nil, err
		}
		if !hasRow {
			break
		}

		name = stmt.ColumnText(0)
		rowID = stmt.ColumnInt64(1)
		modified = columnTime(stmt, 2)
	}

	switch {
	// object
	case name == object:
		info := minio.ObjectInfo{
			Bucket:  bucket,
			Name:    object,
			ModTime: modified,
		}

		stmt := conn.Prep("SELECT rowid FROM blobs WHERE id = ? ORDER BY part_id;")
		stmt.BindInt64(1, rowID)

		var blobs []*sqlite.Blob
		defer func() {
			if connInUse {
				return
			}
			for _, blob := range blobs {
				blob.Close()
			}
		}()
		for {
			hasRow, err := stmt.Step()
			if err != nil {
				return nil, err
			}
			if !hasRow {
				break
			}

			rowID := stmt.ColumnInt64(0)
			blob, err := conn.OpenBlob("", "blobs", "data", rowID, false)
			if err != nil {
				return nil, err
			}

			info.Size += blob.Size()
			blobs = append(blobs, blob)
		}

		startOffset, length, err := rs.GetOffsetLength(info.Size)
		if err != nil {
			return nil, err
		}

		rdr, err := newBlobReader(bucket, object, blobs, startOffset, length)
		if err != nil {
			return nil, err
		}

		connInUse = true
		closer := func() {
			for _, b := range blobs {
				b.Close()
			}
			s.pool.Put(conn)
		}
		return minio.NewGetObjectReaderFromReader(rdr, info, closer), nil

	// directory
	case strings.HasSuffix(object, "/") || strings.HasPrefix(strings.TrimPrefix(name, object), "/"):
		info := minio.ObjectInfo{
			Bucket: bucket,
			Name:   object,
			IsDir:  true,
		}
		return minio.NewGetObjectReaderFromReader(bytes.NewReader(nil), info, func() {}), nil

	default:
		return nil, minio.ObjectNotFound{
			Bucket: bucket,
			Object: object,
		}
	}
}
func (s *SQLite) GetObject(ctx context.Context, bucket, object string, startOffset, length int64, writer io.Writer, etag string, opts minio.ObjectOptions) error {
	conn, err := s.getConn(ctx)
	if err != nil {
		return err
	}
	defer s.pool.Put(conn)

	stmt := conn.Prep("SELECT rowid FROM blobs WHERE id = (SELECT blob_id FROM objects WHERE bucket = ? AND object = ?) ORDER BY part_id;")
	stmt.BindText(1, bucket)
	stmt.BindText(2, object)

	var blobs []*sqlite.Blob
	defer func() {
		for _, blob := range blobs {
			blob.Close()
		}
	}()
	for {
		hasRow, err := stmt.Step()
		if err != nil {
			return err
		}
		if !hasRow {
			break
		}

		rowID := stmt.ColumnInt64(0)
		blob, err := conn.OpenBlob("", "blobs", "data", rowID, false)
		if err != nil {
			return err
		}

		blobs = append(blobs, blob)
	}

	rdr, err := newBlobReader(bucket, object, blobs, startOffset, length)
	if err != nil {
		return err
	}

	_, err = io.Copy(writer, rdr)
	return err
}

func newBlobReader(bucket, object string, blobs []*sqlite.Blob, startOffset, length int64) (io.Reader, error) {
	var r io.Reader
	switch len(blobs) {
	// no blobs found
	case 0:
		return nil, minio.ObjectNotFound{Bucket: bucket, Object: object}

	// fast path for single blob
	// TODO: does this really make any difference?
	case 1:
		r = blobs[0]
		if startOffset != 0 {
			_, err := blobs[0].Seek(startOffset, io.SeekStart)
			if err != nil {
				return nil, err
			}
		}

		if length > -1 {
			// TODO: check startOffset+length>blob.Size()?
			r = io.LimitReader(r, length)
		}

	// multiple blobs
	default:
		r = &multiBlobReader{
			blobs:       blobs,
			startOffset: startOffset,
			length:      length,
		}
	}

	return r, nil
}

type multiBlobReader struct {
	idx         int
	blobs       []*sqlite.Blob
	startOffset int64
	length      int64
}

func (br *multiBlobReader) Read(p []byte) (int, error) {
	for {
		if br.idx >= len(br.blobs) {
			return 0, io.EOF
		}

		blob := br.blobs[br.idx]
		size := blob.Size()

		if br.startOffset > 0 {
			if br.startOffset >= size {
				br.startOffset -= size
				blob.Close()
				br.idx++
				continue
			}
			_, err := blob.Seek(br.startOffset, io.SeekStart)
			if err != nil {
				return 0, err
			}
			br.startOffset = 0
		}

		limit := int64(len(p))
		if br.length > -1 && limit > br.length {
			limit = br.length
		}
		n, err := blob.Read(p[:limit])
		br.length -= int64(n)
		if err == io.EOF && br.length > 0 && len(br.blobs) > 1 {
			blob.Close()
			br.idx++
			err = nil
		}
		return n, err
	}
}

func (s *SQLite) GetObjectInfo(ctx context.Context, bucket, object string, opts minio.ObjectOptions) (minio.ObjectInfo, error) {
	conn, err := s.getConn(ctx)
	if err != nil {
		return minio.ObjectInfo{}, err
	}
	defer s.pool.Put(conn)

	stmt := conn.Prep("SELECT object, size, modified FROM objects WHERE bucket = ? AND object LIKE ? || '%' ORDER BY object LIMIT 1;")
	stmt.BindText(1, bucket)
	stmt.BindText(2, object)

	var (
		name     string
		size     int64
		modified time.Time
	)
	for {
		hasRow, err := stmt.Step()
		if err != nil {
			return minio.ObjectInfo{}, err
		}
		if !hasRow {
			break
		}

		name = stmt.ColumnText(0)
		size = stmt.ColumnInt64(1)
		modified = columnTime(stmt, 2)
	}

	switch {
	// object
	case name == object:
		return minio.ObjectInfo{
			Bucket:  bucket,
			Name:    object,
			Size:    size,
			ModTime: modified,
		}, nil

	// directory
	case strings.HasSuffix(object, "/") || strings.HasPrefix(strings.TrimPrefix(name, object), "/"):
		return minio.ObjectInfo{
			Bucket: bucket,
			Name:   object,
			IsDir:  true,
		}, nil

	default:
		return minio.ObjectInfo{}, minio.ObjectNotFound{
			Bucket: bucket,
			Object: object,
		}
	}
}
func (s *SQLite) PutObject(ctx context.Context, bucket, object string, data *minio.PutObjReader, metadata map[string]string, opts minio.ObjectOptions) (_ minio.ObjectInfo, err error) {
	mdJSON, err := json.Marshal(metadata)
	if err != nil {
		return minio.ObjectInfo{}, err
	}

	conn, err := s.getConn(ctx)
	if err != nil {
		return minio.ObjectInfo{}, err
	}
	defer s.pool.Put(conn)

	defer sqlitex.Save(conn)(&err)

	stmt := conn.Prep("INSERT INTO blobs (id, data) VALUES ((SELECT COALESCE(MAX(id), 0)+1 FROM blobs), ?);")
	stmt.BindZeroBlob(1, data.Size())
	_, err = stmt.Step()
	if err != nil {
		return minio.ObjectInfo{}, err
	}

	rowID := conn.LastInsertRowID()
	blob, err := conn.OpenBlob("", "blobs", "data", rowID, true)
	if err != nil {
		return minio.ObjectInfo{}, err
	}

	_, err = io.Copy(blob, data)
	blob.Close()
	if err != nil {
		return minio.ObjectInfo{}, err
	}

	// TODO: check if bucket exists?
	// TODO: handle removal of previous blob
	stmt = conn.Prep(`
		INSERT INTO objects (bucket, object, size, metadata, modified, blob_id) VALUES (?, ?, ?, ?, ?, ?)
		ON CONFLICT (bucket, object) DO UPDATE SET size=excluded.size, metadata=excluded.metadata, modified=excluded.modified, blob_id=excluded.blob_id;`)
	stmt.BindText(1, bucket)
	stmt.BindText(2, object)
	stmt.BindInt64(3, data.Size())
	stmt.BindBytes(4, mdJSON)
	stmt.BindInt64(5, time.Now().UnixNano())
	stmt.BindInt64(6, rowID)
	_, err = stmt.Step()
	if err != nil {
		return minio.ObjectInfo{}, err
	}

	return minio.ObjectInfo{
		Bucket: bucket,
		Name:   object,
	}, nil
}
func (s *SQLite) CopyObject(ctx context.Context, srcBucket, srcObject, destBucket, destObject string, srcInfo minio.ObjectInfo, srcOpts, dstOpts minio.ObjectOptions) (minio.ObjectInfo, error) {
	// TODO: metadata only update

	conn, err := s.getConn(ctx)
	if err != nil {
		return minio.ObjectInfo{}, err
	}
	defer s.pool.Put(conn)

	stmt := conn.Prep(`
	INSERT INTO objects(bucket, object, metadata, size, blob_id, modified)
	SELECT ?, ?, metadata, size, blob_id, ? FROM objects WHERE bucket = ? AND object = ?
	ON CONFLICT (bucket, object) DO UPDATE SET metadata = excluded.metadata, size = excluded.size, blob_id = excluded.blob_id, modified = excluded.modified;`)
	stmt.BindText(1, destBucket)
	stmt.BindText(2, destObject)
	stmt.BindInt64(3, time.Now().UnixNano())
	stmt.BindText(4, srcBucket)
	stmt.BindText(5, srcObject)
	_, err = stmt.Step()
	if err != nil {
		return minio.ObjectInfo{}, err
	}

	return minio.ObjectInfo{
		Bucket: destBucket,
		Name:   destObject,
	}, nil
}
func (s *SQLite) DeleteObject(ctx context.Context, bucket, object string) (err error) {
	conn, err := s.getConn(ctx)
	if err != nil {
		return nil
	}
	defer s.pool.Put(conn)

	sqlitex.Save(conn)(&err)

	err = bucketExists(conn, bucket)
	if err != nil {
		return err
	}

	stmt := conn.Prep("DELETE FROM objects WHERE bucket = ? AND object = ?;")
	stmt.BindText(1, bucket)
	stmt.BindText(2, object)
	_, err = stmt.Step()
	if err != nil {
		return err
	}

	if conn.Changes() == 0 {
		return minio.ObjectNotFound{
			Bucket: bucket,
			Object: object,
		}
	}

	return nil
}

// TODO: copied from cmd package
func mustGetUUID() string {
	uuid, err := uuid.New()
	if err != nil {
		logger.CriticalIf(context.Background(), err)
	}

	return uuid.String()
}

// Multipart operations.
func (s *SQLite) ListMultipartUploads(ctx context.Context, bucket, prefix, keyMarker, uploadIDMarker, delimiter string, maxUploads int) (_ minio.ListMultipartsInfo, err error) {
	res := minio.ListMultipartsInfo{
		KeyMarker:      keyMarker,
		UploadIDMarker: uploadIDMarker,
		MaxUploads:     maxUploads,
		Prefix:         prefix,
		Delimiter:      delimiter,
		// EncodingType
	}

	conn, err := s.getConn(ctx)
	if err != nil {
		return res, nil
	}
	defer s.pool.Put(conn)

	sqlitex.Save(conn)(&err)

	err = bucketExists(conn, bucket)
	if err != nil {
		return res, err
	}

	var stmt *sqlite.Stmt
	if delimiter == "" {
		stmt = conn.Prep("SELECT id, object as name FROM uploads WHERE bucket = ? AND object LIKE ? || '%' AND object > ? AND id > ? ORDER BY object LIMIT ?;")
		stmt.BindText(1, bucket)
		stmt.BindText(2, prefix)
		stmt.BindText(3, keyMarker)
		stmt.BindText(4, uploadIDMarker)
		stmt.BindInt64(5, int64(maxUploads)+1)
	} else {
		stmt = conn.Prep(`
		SELECT DISTINCT id, $prefix || substr(ltrim(object, $prefix), 1, instr(ltrim(object, $prefix), $delimiter)) as name, 1 as is_prefix
		FROM uploads
		WHERE bucket = $bucket AND object LIKE $prefix || '%' AND object > $key_marker AND id > $id_marker AND name <> $prefix
		UNION
		SELECT id, object as name, 0 as is_prefix
		FROM uploads
		WHERE bucket = $bucket AND object LIKE $prefix || '%' AND object > $key_marker AND id > $id_marker AND instr(ltrim(object, $prefix), $delimiter) = 0
		ORDER BY object
		LIMIT $max_keys;`)
		stmt.SetText("$bucket", bucket)
		stmt.SetText("$prefix", prefix)
		stmt.SetText("$key_marker", keyMarker)
		stmt.SetText("$id_marker", uploadIDMarker)
		stmt.SetText("$delimiter", delimiter)
		stmt.SetInt64("$max_keys", int64(maxUploads)+1)
	}

	var count int
	for {
		hasRow, err := stmt.Step()
		if err != nil {
			return res, err
		}
		if !hasRow {
			break
		}

		count++
		if count > maxUploads {
			res.IsTruncated = true
			continue
		}

		var (
			id       = stmt.GetText("id")
			name     = stmt.GetText("name")
			isPrefix = stmt.GetInt64("is_prefix") == 1
		)
		res.NextKeyMarker = name
		res.NextUploadIDMarker = id
		if isPrefix {
			res.CommonPrefixes = append(res.CommonPrefixes, name)
			continue
		}
		res.Uploads = append(res.Uploads, minio.MultipartInfo{
			Object:   name,
			UploadID: id,
			// TODO: Initiated
		})
	}

	return res, nil
}

func bucketExists(conn *sqlite.Conn, bucket string) error {
	stmt := conn.Prep("SELECT count(*) FROM buckets WHERE bucket = ?;")
	stmt.BindText(1, bucket)
	count, err := sqlitex.ResultInt64(stmt)
	if err != nil {
		return toMinioError(err, errSourceBuckets, bucket)
	}
	if count == 0 {
		return minio.BucketNotFound{Bucket: bucket}
	}
	return nil
}

func (s *SQLite) NewMultipartUpload(ctx context.Context, bucket, object string, metadata map[string]string, opts minio.ObjectOptions) (_ string, err error) {
	id := mustGetUUID()
	mdJSON, err := json.Marshal(metadata)
	if err != nil {
		return "", err
	}

	conn, err := s.getConn(ctx)
	if err != nil {
		return "", err
	}
	defer s.pool.Put(conn)

	sqlitex.Save(conn)(&err)

	err = bucketExists(conn, bucket)
	if err != nil {
		return "", err
	}

	// reserve a blob_id
	stmt := conn.Prep("INSERT INTO blobs (id, part_id, data) VALUES ((SELECT COALESCE(MAX(id), 0)+1 FROM blobs), -1, 0);")
	_, err = stmt.Step()
	if err != nil {
		return "", err
	}
	rowID := conn.LastInsertRowID()

	stmt = conn.Prep("INSERT INTO uploads (id, bucket, object, metadata, blob_id) VALUES (?, ?, ?, ?, (SELECT id FROM blobs WHERE rowid = ?))")
	stmt.BindText(1, id)
	stmt.BindText(2, bucket)
	stmt.BindText(3, object)
	stmt.BindBytes(4, mdJSON)
	stmt.BindInt64(5, rowID)
	_, err = stmt.Step()
	if err != nil {
		return "", toMinioError(err, errSourceUploads, "")
	}

	return id, nil
}

func (s *SQLite) CopyObjectPart(ctx context.Context, srcBucket, srcObject, destBucket, destObject string, uploadID string, partID int, startOffset int64, length int64, srcInfo minio.ObjectInfo, srcOpts, dstOpts minio.ObjectOptions) (minio.PartInfo, error) {
	return s.PutObjectPart(ctx, destBucket, destObject, uploadID, partID, srcInfo.PutObjReader, dstOpts)
}

func getBlobID(conn *sqlite.Conn, bucket, object, uploadID string) (int64, error) {
	stmt := conn.Prep("SELECT blob_id FROM uploads WHERE id = ? AND bucket = ? AND object = ?")
	stmt.BindText(1, uploadID)
	stmt.BindText(2, bucket)
	stmt.BindText(3, object)

	blobID := int64(-1)
	for {
		hasRow, err := stmt.Step()
		if err != nil {
			return -1, toMinioError(err, errSourceUploads, uploadID)
		}
		if !hasRow {
			break
		}

		blobID = stmt.ColumnInt64(0)
	}

	if blobID == -1 {
		return -1, minio.InvalidUploadID{UploadID: uploadID}
	}
	return blobID, nil
}

func (s *SQLite) PutObjectPart(ctx context.Context, bucket, object, uploadID string, partID int, data *minio.PutObjReader, opts minio.ObjectOptions) (_ minio.PartInfo, err error) {
	conn, err := s.getConn(ctx)
	if err != nil {
		return minio.PartInfo{}, err
	}
	defer s.pool.Put(conn)

	sqlitex.Save(conn)(&err)

	blobID, err := getBlobID(conn, bucket, object, uploadID)
	if err != nil {
		return minio.PartInfo{}, err
	}

	stmt := conn.Prep("INSERT INTO blobs (id, part_id, data) VALUES (?, ?, ?) ON CONFLICT (id, part_id) DO UPDATE SET data = excluded.data;")
	stmt.BindInt64(1, blobID)
	stmt.BindInt64(2, int64(partID))
	stmt.BindZeroBlob(3, data.Size())
	_, err = stmt.Step()
	if err != nil {
		return minio.PartInfo{}, err
	}

	// must query rowid since last insert will be incorrect on upsert
	stmt = conn.Prep("SELECT rowid FROM blobs WHERE id = ? AND part_id = ?")
	stmt.BindInt64(1, blobID)
	stmt.BindInt64(2, int64(partID))
	rowID, err := sqlitex.ResultInt64(stmt)
	if err != nil {
		return minio.PartInfo{}, err
	}

	blob, err := conn.OpenBlob("", "blobs", "data", rowID, true)
	if err != nil {
		return minio.PartInfo{}, err
	}
	defer blob.Close()

	_, err = io.Copy(blob, data)
	if err != nil {
		return minio.PartInfo{}, err
	}

	return minio.PartInfo{PartNumber: partID}, nil
}

func (s *SQLite) ListObjectParts(ctx context.Context, bucket, object, uploadID string, partNumberMarker int, maxParts int) (minio.ListPartsInfo, error) {
	res := minio.ListPartsInfo{
		Bucket:           bucket,
		Object:           object,
		UploadID:         uploadID,
		PartNumberMarker: partNumberMarker,
		MaxParts:         maxParts,
		// UserDefined
		// EncodingType
	}

	conn, err := s.getConn(ctx)
	if err != nil {
		return res, err
	}
	defer s.pool.Put(conn)

	blobID, err := getBlobID(conn, bucket, object, uploadID)
	if err != nil {
		return minio.ListPartsInfo{}, err
	}

	stmt := conn.Prep("SELECT part_id, length(data) FROM blobs WHERE id = ? AND part_id <> -1 ORDER BY part_id LIMIT ?;")
	stmt.BindInt64(1, blobID)
	stmt.BindInt64(2, int64(maxParts)+1)

	var count int
	for {
		hasRow, err := stmt.Step()
		if err != nil {
			return res, toMinioError(err, errSourceParts, uploadID)
		}
		if !hasRow {
			break
		}

		count++
		if count > maxParts {
			res.IsTruncated = true
			continue
		}

		var (
			partID = stmt.ColumnInt(0)
			size   = stmt.ColumnInt64(1)
		)

		res.NextPartNumberMarker = partID
		res.Parts = append(res.Parts, minio.PartInfo{
			PartNumber: partID,
			Size:       size,
		})
	}

	return res, nil
}
func (s *SQLite) AbortMultipartUpload(ctx context.Context, bucket, object, uploadID string) (err error) {
	conn, err := s.getConn(ctx)
	if err != nil {
		return err
	}
	defer s.pool.Put(conn)

	sqlitex.Save(conn)(&err)

	blobID, err := getBlobID(conn, bucket, object, uploadID)
	if err != nil {
		return err
	}

	stmt := conn.Prep("DELETE FROM uploads WHERE id = ?;")
	stmt.BindText(1, uploadID)
	_, err = stmt.Step()
	if err != nil {
		return toMinioError(err, errSourceUploads, uploadID)
	}

	stmt = conn.Prep("DELETE FROM blobs WHERE id = ?;")
	stmt.BindInt64(1, blobID)
	_, err = stmt.Step()
	return toMinioError(err, errSourceUploads, uploadID)
}

func (s *SQLite) CompleteMultipartUpload(ctx context.Context, bucket, object, uploadID string, uploadedParts []minio.CompletePart, opts minio.ObjectOptions) (_ minio.ObjectInfo, err error) {
	conn, err := s.getConn(ctx)
	if err != nil {
		return minio.ObjectInfo{}, err
	}
	defer s.pool.Put(conn)

	defer sqlitex.Save(conn)(&err)

	blobID, err := getBlobID(conn, bucket, object, uploadID)
	if err != nil {
		return minio.ObjectInfo{}, err
	}

	// get existing parts
	stmt := conn.Prep("SELECT part_id FROM blobs WHERE id = ?;")
	if err != nil {
		return minio.ObjectInfo{}, err
	}
	stmt.BindInt64(1, blobID)

	dbPartIDs := make(map[int]struct{})
	for {
		hasRow, err := stmt.Step()
		if err != nil {
			return minio.ObjectInfo{}, err
		}
		if !hasRow {
			break
		}

		id := stmt.ColumnInt(0)
		dbPartIDs[id] = struct{}{}
	}

	// validate part numbers
	// TODO: validate etags
	for _, part := range uploadedParts {
		if _, ok := dbPartIDs[part.PartNumber]; !ok {
			return minio.ObjectInfo{}, minio.InvalidPart{PartNumber: part.PartNumber}
		}
		delete(dbPartIDs, part.PartNumber)
	}

	// remove any extraneous parts, including marker part_id of -1
	for partID := range dbPartIDs {
		stmt = conn.Prep("DELETE FROM blobs WHERE id = ? AND part_id = ?;")
		stmt.BindInt64(1, blobID)
		stmt.BindInt64(2, int64(partID))
		_, err = stmt.Step()
		if err != nil {
			return minio.ObjectInfo{}, toMinioError(err, errSourceUploads, uploadID)
		}
	}

	// create object
	stmt = conn.Prep(`
		INSERT INTO objects (bucket, object, size, metadata, modified, blob_id) VALUES (
			?1,
			?2,
			(SELECT SUM(LENGTH(data)) FROM blobs WHERE id = ?3),
			(SELECT metadata FROM uploads WHERE id = ?4),
			?5,
			?3
		)
		ON CONFLICT (bucket, object) DO UPDATE SET size=excluded.size, metadata=excluded.metadata, modified=excluded.modified, blob_id=excluded.blob_id;`)
	stmt.BindText(1, bucket)
	stmt.BindText(2, object)
	stmt.BindInt64(3, blobID)
	stmt.BindText(4, uploadID)
	stmt.BindInt64(5, time.Now().UnixNano())
	_, err = stmt.Step()
	if err != nil {
		return minio.ObjectInfo{}, toMinioError(err, errSourceObjects, object)
	}

	// remove upload
	stmt = conn.Prep("DELETE FROM uploads WHERE id = ?;")
	stmt.BindText(1, uploadID)
	_, err = stmt.Step()
	if err != nil {
		return minio.ObjectInfo{}, toMinioError(err, errSourceUploads, uploadID)
	}

	return minio.ObjectInfo{
		Bucket: bucket,
		Name:   object,
	}, nil
}

// Healing operations.
func (s *SQLite) ReloadFormat(ctx context.Context, dryRun bool) error {
	return minio.NotImplemented{}
}
func (s *SQLite) HealFormat(ctx context.Context, dryRun bool) (madmin.HealResultItem, error) {
	return madmin.HealResultItem{}, minio.NotImplemented{}
}
func (s *SQLite) HealBucket(ctx context.Context, bucket string, dryRun bool) ([]madmin.HealResultItem, error) {
	return nil, minio.NotImplemented{}
}
func (s *SQLite) HealObject(ctx context.Context, bucket, object string, dryRun bool) (madmin.HealResultItem, error) {
	return madmin.HealResultItem{}, minio.NotImplemented{}
}
func (s *SQLite) ListBucketsHeal(ctx context.Context) ([]minio.BucketInfo, error) {
	return nil, minio.NotImplemented{}
}
func (s *SQLite) ListObjectsHeal(ctx context.Context, bucket, prefix, marker, delimiter string, maxKeys int) (minio.ListObjectsInfo, error) {
	return minio.ListObjectsInfo{}, minio.NotImplemented{}
}

// Policy operations
func (s *SQLite) SetBucketPolicy(ctx context.Context, bucket string, policy *policy.Policy) error {
	policyJSON, err := json.Marshal(policy)
	if err != nil {
		return err
	}

	conn, err := s.getConn(ctx)
	if err != nil {
		return err
	}
	defer s.pool.Put(conn)

	stmt := conn.Prep("UPDATE buckets SET policy = ? WHERE bucket = ?;")
	stmt.BindBytes(1, policyJSON)
	stmt.BindText(2, bucket)
	_, err = stmt.Step()
	if err != nil {
		return toMinioError(err, errSourceBuckets, bucket)
	}

	if conn.Changes() == 0 {
		return minio.BucketNotFound{Bucket: bucket}
	}

	return nil
}

func (s *SQLite) GetBucketPolicy(ctx context.Context, bucket string) (*policy.Policy, error) {
	conn, err := s.getConn(ctx)
	if err != nil {
		return nil, err
	}
	defer s.pool.Put(conn)

	stmt := conn.Prep("SELECT rowid, policy IS NOT NULL FROM buckets WHERE bucket = ?;")
	stmt.BindText(1, bucket)

	var (
		rowID     int64
		hasPolicy bool
	)
	for {
		hasRow, err := stmt.Step()
		if err != nil {
			return nil, toMinioError(err, errSourceBuckets, bucket)
		}
		if !hasRow {
			break
		}

		rowID = stmt.ColumnInt64(0)
		hasPolicy = stmt.ColumnInt64(1) == 1
	}

	if !hasPolicy {
		return nil, minio.BucketPolicyNotFound{Bucket: bucket}
	}

	blob, err := conn.OpenBlob("", "buckets", "policy", rowID, false)
	if err != nil {
		return nil, toMinioError(err, errSourceBuckets, bucket)
	}
	defer blob.Close()

	return policy.ParseConfig(blob, bucket)
}

func (s *SQLite) DeleteBucketPolicy(ctx context.Context, bucket string) error {
	conn, err := s.getConn(ctx)
	if err != nil {
		return err
	}
	defer s.pool.Put(conn)

	stmt := conn.Prep("UPDATE buckets SET policy = NULL WHERE bucket = ?;")
	stmt.BindText(1, bucket)
	_, err = stmt.Step()

	return err
}

// Supported operations check
func (s *SQLite) IsNotificationSupported() bool { return true }
func (s *SQLite) IsEncryptionSupported() bool   { return true }
func (s *SQLite) IsCompressionSupported() bool  { return true }
