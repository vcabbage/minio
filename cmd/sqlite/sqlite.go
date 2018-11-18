package sqlite

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"crawshaw.io/sqlite"
	"crawshaw.io/sqlite/sqliteutil"
	minio "github.com/minio/minio/cmd"
	"github.com/minio/minio/cmd/debug"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/madmin"
	"github.com/minio/minio/pkg/policy"
	"github.com/skyrings/skyring-common/tools/uuid"
)

// TODO: timestamps
// TODO: etags

// TODO: copied
const (
	minioMetaBucket = ".minio.sys"
)

func init() {
	// TODO: remove awful hack
	minio.NewSQLiteLayer = NewSQLiteLayer
}

type SQLite struct {
	pool   *sqlite.Pool
	closed uint32
}

var (
	errDBClosed = errors.New("db closed")
)

func NewSQLiteLayer(uri *url.URL) (minio.ObjectLayer, error) {
	if uri.Scheme == "sqlite" {
		uri.Scheme = "file"
	}
	const poolSize = 10 // TODO: what size?
	pool, err := sqlite.Open(uri.String(), 0, poolSize)
	if err != nil {
		return nil, err
	}

	s := &SQLite{pool: pool}

	err = s.init()
	if err != nil {
		return nil, err
	}

	return &debug.DebugLayer{Wrapped: s, LogCallers: 1}, nil
}

// timestamps are nanoseconds since unix epoch
const sqlInit = `
	CREATE TABLE IF NOT EXISTS buckets (
		bucket  TEXT PRIMARY KEY,
		policy  BLOB,
		created INTEGER
	);
	CREATE TABLE IF NOT EXISTS objects (
		bucket   TEXT,
		object   TEXT,
		metadata TEXT,
		data     BLOB,
		modified INTEGER,
		PRIMARY KEY (bucket, object)
	);
	CREATE TABLE IF NOT EXISTS uploads (
		id       TEXT PRIMARY KEY,
		bucket   TEXT,
		object   TEXT,
		metadata TEXT
	);
	CREATE TABLE IF NOT EXISTS parts (
		upload_id TEXT,
		part_id   INTEGER,
		data      BLOB,
		PRIMARY KEY (upload_id, part_id)
	);`

func (s *SQLite) init() error {
	ctx := context.Background()

	conn, err := s.getConn(ctx)
	if err != nil {
		return err
	}
	defer s.pool.Put(conn)

	err = sqliteutil.ExecScript(conn, sqlInit)
	if err != nil {
		return err
	}

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

	return sqliteutil.ResultInt64(conn.Prep("SELECT page_count * page_size FROM pragma_page_count(), pragma_page_size();"))
}

func (s *SQLite) getConn(ctx context.Context) (*sqlite.Conn, error) {
	conn := s.pool.Get(ctx.Done())
	if conn == nil {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return nil, errDBClosed // TODO: minio error
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
			}
		case sqlite.SQLITE_FULL:
			return minio.StorageFull{}
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
		if !bi.Created.IsZero() {
			panic("too many results for " + bucket) // TODO: error
		}

		bi.Created = time.Unix(0, stmt.GetInt64("created"))
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

	defer sqliteutil.Save(conn)(&err)

	stmt := conn.Prep("DELETE FROM buckets WHERE bucket = ?;")
	stmt.BindText(1, bucket)
	_, err = stmt.Step()
	if err != nil {
		return toMinioError(err, errSourceBuckets, bucket)
	}

	stmt = conn.Prep("DELETE FROM objects WHERE bucket = ?;")
	stmt.BindText(1, bucket)
	_, err = stmt.Step()
	return toMinioError(err, errSourceObjects, bucket)
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
		stmt = conn.Prep("SELECT object as name, length(data) as size, modified FROM objects WHERE bucket = ? AND object LIKE ? || '%' AND object > ? ORDER BY object LIMIT ?;")
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
		SELECT object as name, length(data) as size, modified, 0 as is_prefix
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

	stmt := conn.Prep("SELECT rowid, object, modified FROM objects WHERE bucket = ? AND object LIKE ? || '%' ORDER BY object LIMIT 1;")
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

		rowID = stmt.GetInt64("rowid")
		name = stmt.GetText("object")
		modified = time.Unix(0, stmt.GetInt64("modified"))
	}

	switch {
	// object
	case name == object:
		blob, err := conn.OpenBlob("", "objects", "data", rowID, false)
		if err != nil {
			return nil, err
		}

		info := minio.ObjectInfo{
			Bucket:  bucket,
			Name:    object,
			Size:    blob.Size(),
			ModTime: modified,
		}

		startOffset, length, err := rs.GetOffsetLength(info.Size)
		if err != nil {
			blob.Close()
			return nil, err
		}

		if startOffset != 0 {
			_, err = blob.Seek(startOffset, io.SeekStart)
			if err != nil {
				blob.Close()
				return nil, err
			}
		}

		rdr := io.Reader(blob)
		if length > -1 {
			rdr = io.LimitReader(rdr, length)
		}

		connInUse = true
		closer := func() {
			blob.Close()
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
func (s *SQLite) GetObject(ctx context.Context, bucket, object string, startOffset int64, length int64, writer io.Writer, etag string, opts minio.ObjectOptions) error {
	conn, err := s.getConn(ctx)
	if err != nil {
		return err
	}
	defer s.pool.Put(conn)

	rowID, err := getObjectRowID(conn, bucket, object)
	if err != nil {
		return err
	}

	blob, err := conn.OpenBlob("", "objects", "data", rowID, false)
	if err != nil {
		return err
	}
	defer blob.Close()

	if startOffset != 0 {
		_, err = blob.Seek(startOffset, io.SeekStart)
		if err != nil {
			return err
		}
	}

	rdr := io.Reader(blob)
	if length > -1 {
		rdr = io.LimitReader(rdr, length)
	}

	_, err = io.Copy(writer, rdr)
	return err
}
func (s *SQLite) GetObjectInfo(ctx context.Context, bucket, object string, opts minio.ObjectOptions) (minio.ObjectInfo, error) {
	conn, err := s.getConn(ctx)
	if err != nil {
		return minio.ObjectInfo{}, err
	}
	defer s.pool.Put(conn)

	stmt := conn.Prep("SELECT object, length(data), modified FROM objects WHERE bucket = ? AND object LIKE ? || '%' ORDER BY object LIMIT 1;")
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
		modified = time.Unix(0, stmt.ColumnInt64(2))
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

	defer sqliteutil.Save(conn)(&err)

	// TODO: check if bucket exists?
	stmt := conn.Prep(`
		INSERT INTO objects (bucket, object, data, metadata, modified) VALUES (?, ?, ?, ?, ?)
		ON CONFLICT (bucket, object) DO UPDATE SET data=excluded.data, metadata=excluded.metadata, modified=excluded.modified;`)
	stmt.BindText(1, bucket)
	stmt.BindText(2, object)
	stmt.BindZeroBlob(3, data.Size())
	stmt.BindBytes(4, mdJSON)
	stmt.BindInt64(5, time.Now().UnixNano())
	_, err = stmt.Step()
	if err != nil {
		return minio.ObjectInfo{}, err
	}

	rowID := conn.LastInsertRowID()
	if rowID < 1 {
		// No last insert on upsert
		rowID, err = getObjectRowID(conn, bucket, object)
		if err != nil {
			return minio.ObjectInfo{}, err
		}
	}

	blob, err := conn.OpenBlob("", "objects", "data", rowID, true)
	if err != nil {
		return minio.ObjectInfo{}, err
	}
	defer blob.Close()

	_, err = io.Copy(blob, data)
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
	INSERT INTO objects SELECT ?, ?, metadata, data, ? FROM objects WHERE bucket = ? AND object = ?
	ON CONFLICT (bucket, object) DO UPDATE SET metadata = excluded.metadata, data = excluded.data, modified = excluded.modified;`)
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
func (s *SQLite) DeleteObject(ctx context.Context, bucket, object string) error {
	conn, err := s.getConn(ctx)
	if err != nil {
		return nil
	}
	defer s.pool.Put(conn)

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
func (s *SQLite) ListMultipartUploads(ctx context.Context, bucket, prefix, keyMarker, uploadIDMarker, delimiter string, maxUploads int) (minio.ListMultipartsInfo, error) {
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
func (s *SQLite) NewMultipartUpload(ctx context.Context, bucket, object string, metadata map[string]string, opts minio.ObjectOptions) (string, error) {
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

	stmt := conn.Prep("INSERT INTO uploads (id, bucket, object, metadata) VALUES (?, ?, ?, ?)")
	stmt.BindText(1, id)
	stmt.BindText(2, bucket)
	stmt.BindText(3, object)
	stmt.BindBytes(4, mdJSON)
	_, err = stmt.Step()
	if err != nil {
		return "", toMinioError(err, errSourceUploads, "")
	}

	return id, nil
}
func (s *SQLite) CopyObjectPart(ctx context.Context, srcBucket, srcObject, destBucket, destObject string, uploadID string, partID int, startOffset int64, length int64, srcInfo minio.ObjectInfo, srcOpts, dstOpts minio.ObjectOptions) (minio.PartInfo, error) {
	return s.PutObjectPart(ctx, destBucket, destObject, uploadID, partID, srcInfo.PutObjReader, dstOpts)
}
func (s *SQLite) PutObjectPart(ctx context.Context, bucket, object, uploadID string, partID int, data *minio.PutObjReader, opts minio.ObjectOptions) (_ minio.PartInfo, err error) {
	conn, err := s.getConn(ctx)
	if err != nil {
		return minio.PartInfo{}, err
	}
	defer s.pool.Put(conn)

	sqliteutil.Save(conn)(&err)

	stmt := conn.Prep("INSERT INTO parts (upload_id, part_id, data) VALUES (?, ?, ?) ON CONFLICT (upload_id, part_id) DO UPDATE SET data = excluded.data;")
	stmt.BindText(1, uploadID) // TODO: validate
	stmt.BindInt64(2, int64(partID))
	stmt.BindZeroBlob(3, data.Size())
	_, err = stmt.Step()
	if err != nil {
		return minio.PartInfo{}, err
	}

	rowID, err := lastPartUpsertRowID(conn, uploadID, int64(partID))
	if err != nil {
		return minio.PartInfo{}, err
	}

	blob, err := conn.OpenBlob("", "parts", "data", rowID, true)
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

	// TODO: validate upload exists

	stmt := conn.Prep("SELECT part_id, length(data) FROM parts WHERE upload_id = ? ORDER BY part_id LIMIT ?;")
	stmt.BindText(1, uploadID)
	stmt.BindInt64(2, int64(maxParts)+1)

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
func (s *SQLite) AbortMultipartUpload(ctx context.Context, bucket, object, uploadID string) error {
	conn, err := s.getConn(ctx)
	if err != nil {
		return err
	}
	defer s.pool.Put(conn)

	stmt := conn.Prep("DELETE FROM parts WHERE upload_id = ?;")
	stmt.BindText(1, uploadID)
	_, err = stmt.Step()
	if err != nil {
		return toMinioError(err, errSourceParts, uploadID)
	}
	stmt = conn.Prep("DELETE FROM uploads WHERE id = ?;")
	stmt.BindText(1, uploadID)
	_, err = stmt.Step()
	if err != nil {
		return toMinioError(err, errSourceUploads, uploadID)
	}
	return nil
}
func (s *SQLite) CompleteMultipartUpload(ctx context.Context, bucket, object, uploadID string, uploadedParts []minio.CompletePart, opts minio.ObjectOptions) (_ minio.ObjectInfo, err error) {
	conn, err := s.getConn(ctx)
	if err != nil {
		return minio.ObjectInfo{}, err
	}
	defer s.pool.Put(conn)

	defer sqliteutil.Save(conn)(&err)

	// determine complete size
	var size int64
	partRowIDs := make([]int64, len(uploadedParts))
	for i, part := range uploadedParts {
		stmt := conn.Prep("SELECT rowid, length(data) FROM parts WHERE upload_id = ? AND part_id = ? ORDER BY part_id")
		stmt.BindText(1, uploadID)
		stmt.BindInt64(2, int64(part.PartNumber))

		for {
			hasRow, err := stmt.Step()
			if err != nil {
				return minio.ObjectInfo{}, err
			}
			if !hasRow {
				break
			}

			partRowIDs[i] = stmt.ColumnInt64(0)
			size += stmt.ColumnInt64(1)
		}
	}

	// create object
	// TODO: check if bucket exists?
	stmt := conn.Prep(`
		INSERT INTO objects (bucket, object, data, metadata, modified) VALUES (?, ?, ?, (SELECT metadata FROM uploads WHERE id = ?), ?)
		ON CONFLICT (bucket, object) DO UPDATE SET data=excluded.data, metadata=excluded.metadata;`)
	stmt.BindText(1, bucket)
	stmt.BindText(2, object)
	stmt.BindZeroBlob(3, size)
	stmt.BindText(4, uploadID)
	stmt.BindInt64(5, time.Now().UnixNano())
	_, err = stmt.Step()
	if err != nil {
		return minio.ObjectInfo{}, err
	}

	rowID := conn.LastInsertRowID()
	if rowID < 1 {
		// No last insert on upsert
		rowID, err = getObjectRowID(conn, bucket, object)
		if err != nil {
			return minio.ObjectInfo{}, err
		}
	}

	// open object blob
	objBlob, err := conn.OpenBlob("", "objects", "data", rowID, true)
	if err != nil {
		return minio.ObjectInfo{}, err
	}
	defer objBlob.Close()

	// concat parts into blob
	for _, rowID := range partRowIDs {
		partBlob, err := conn.OpenBlob("", "parts", "data", rowID, false)
		if err != nil {
			return minio.ObjectInfo{}, err
		}

		_, err = io.Copy(objBlob, partBlob)
		partBlob.Close()
		if err != nil {
			return minio.ObjectInfo{}, err
		}
	}

	// remove upload
	stmt = conn.Prep("DELETE FROM parts WHERE upload_id = ?;")
	stmt.BindText(1, uploadID)
	_, err = stmt.Step()
	if err != nil {
		return minio.ObjectInfo{}, err
	}
	stmt = conn.Prep("DELETE FROM uploads WHERE id = ?;")
	stmt.BindText(1, uploadID)
	_, err = stmt.Step()
	if err != nil {
		return minio.ObjectInfo{}, err
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
		return err // TODO: translate into approriate error
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
			return nil, err
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
		return nil, err
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

func getObjectRowID(conn *sqlite.Conn, bucket, object string) (int64, error) {
	stmt := conn.Prep("SELECT rowid FROM objects WHERE bucket = ? AND object = ?;")
	stmt.BindText(1, bucket)
	stmt.BindText(2, object)

	rowID := int64(-1)
	for {
		hasRow, err := stmt.Step()
		if err != nil {
			return rowID, err
		}
		if !hasRow {
			break
		}
		if rowID != -1 {
			panic("too many results for " + bucket + "/" + object) // TODO: error
		}

		rowID = stmt.GetInt64("rowid")
	}

	if rowID == -1 {
		return rowID, minio.ObjectNotFound{
			Bucket: bucket,
			Object: object,
		}
	}
	return rowID, nil
}

func lastPartUpsertRowID(conn *sqlite.Conn, uploadID string, partNumber int64) (int64, error) {
	// TODO: is this safe for upsert? will it return 0 or that last successful insert?
	rowID := conn.LastInsertRowID()
	if rowID > 0 {
		return rowID, nil
	}

	return getPartRowID(conn, uploadID, partNumber)
}

func getPartRowID(conn *sqlite.Conn, uploadID string, partNumber int64) (int64, error) {
	stmt := conn.Prep("SELECT rowid FROM parts WHERE upload_id = ? AND part_id = ?;")
	stmt.BindText(1, uploadID)
	stmt.BindInt64(2, partNumber)

	rowID := int64(-1)
	for {
		hasRow, err := stmt.Step()
		if err != nil {
			return rowID, err
		}
		if !hasRow {
			break
		}
		if rowID != -1 {
			panic("too many results for " + uploadID + " " + strconv.FormatInt(partNumber, 10)) // TODO: error
		}

		rowID = stmt.GetInt64("rowid")
	}

	if rowID == -1 {
		return 0, errors.New("part not found")
	}
	return rowID, nil
}
