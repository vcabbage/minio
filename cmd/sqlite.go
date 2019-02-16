package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"crawshaw.io/sqlite"
	"crawshaw.io/sqlite/sqlitex"
	"github.com/minio/minio/pkg/madmin"
	"github.com/minio/minio/pkg/mimedb"
	"github.com/minio/minio/pkg/policy"
)

// TODO: parse metadata

const (
	bufferSize        = 16 * 1024 * 1024
	tempFileThreshold = 16 * 1024 * 1024
)

type SQLite struct {
	readers *sqlitex.Pool
	closed  uint32
	tempDir string

	mu         sync.Mutex
	writer     *sqlite.Conn
	copyBuffer []byte
}

var (
	errDBClosed = errors.New("db closed")
)

func NewSQLiteLayer(uri *url.URL) (ObjectLayer, error) {
	if uri.Scheme == "sqlite" {
		uri.Scheme = "file"
	}
	file := uri.String()
	conn, err := sqlite.OpenConn(file, 0)
	if err != nil {
		return nil, err
	}

	// Set very long busy timeout. The context done channel passed when getting
	// the connection from the pool should always apply.
	conn.SetBusyTimeout(5 * time.Minute)
	err = sqlitex.ExecTransient(conn, "PRAGMA foreign_keys = ON;", nil)
	if err != nil {
		return nil, err
	}

	const poolSize = 10 // TODO: what size?
	const roFlags = sqlite.SQLITE_OPEN_READONLY | sqlite.SQLITE_OPEN_URI | sqlite.SQLITE_OPEN_NOMUTEX
	pool, err := sqlitex.Open(file, roFlags, poolSize)
	if err != nil {
		return nil, err
	}

	tempDir, err := ioutil.TempDir("", ".minio_sqlite")
	if err != nil {
		return nil, err
	}

	s := &SQLite{
		tempDir:    tempDir,
		readers:    pool,
		writer:     conn,
		copyBuffer: make([]byte, bufferSize),
	}

	err = s.init()
	if err != nil {
		return nil, err
	}

	return s, nil
	return &DebugLayer{Wrapped: s, LogReturns: true, LogCallers: 1}, nil
}

// Multipart uploads are maintained as individual parts to avoid
// the overhead of recombining on completion. This also allows objects
// larger than SQLITE_MAX_LENGTH to be stored.

// objects table uses "WITHOUT ROWID" because early experimentation found it to
// be faster for listing objects in a bucket. Since there are tradeoffs to this
// the results should be checked again once the implementation is complete.
// https://www.sqlite.org/withoutrowid.html

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
		etag    TEXT,
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
		etag     TEXT NOT NULL,
		content_type TEXT NOT NULL,
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

	err := sqlitex.ExecScript(s.writer, sqlInit)
	if err != nil {
		return err
	}

	err = bucketExists(s.writer, minioMetaBucket)
	if err == nil {
		return nil
	}

	return s.MakeBucketWithLocation(ctx, minioMetaBucket, "")
}

func (s *SQLite) Shutdown(context.Context) error {
	atomic.StoreUint32(&s.closed, 1)
	// TODO: synchronize with in progress operations
	err1 := s.readers.Close()
	err2 := s.writer.Close()
	err3 := os.RemoveAll(s.tempDir)

	err := err1
	if err1 == nil {
		err = err2
	}
	if err2 == nil {
		err = err3
	}
	return err
}

func (s *SQLite) StorageInfo(ctx context.Context) StorageInfo {
	size, _ := s.dbSize(ctx)

	si := StorageInfo{Used: uint64(size)}
	si.Backend.Type = BackendSQLite

	return si
}

// dbSize reports the size of the database or 0 if there is an error.
func (s *SQLite) dbSize(ctx context.Context) (int64, error) {
	conn, err := s.getConn(ctx)
	if err != nil {
		return 0, err
	}
	defer s.readers.Put(conn)

	return sqlitex.ResultInt64(conn.Prep("SELECT page_count * page_size FROM pragma_page_count(), pragma_page_size();"))
}

func (s *SQLite) getConn(ctx context.Context) (*sqlite.Conn, error) {
	conn := s.readers.Get(ctx)
	if conn == nil {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return nil, errDBClosed // TODO: minio error
	}
	// Set very long busy timeout. The context passed when getting
	// the connection from the pool should always apply.
	conn.SetBusyTimeout(5 * time.Minute)

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
				return BucketExists{Bucket: srcLabel}
			case errSourceObjects:
				return ObjectNotFound{Object: srcLabel} // TODO: include bucket
			}
		case sqlite.SQLITE_FULL:
			return StorageFull{}
		case sqlite.SQLITE_BUSY:
			return OperationTimedOut{}
		}
	}
	return err
}

// Bucket operations.
func (s *SQLite) MakeBucketWithLocation(ctx context.Context, bucket string, location string) error {
	if !IsValidBucketName(bucket) {
		return BucketNameInvalid{Bucket: bucket}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.writer.SetInterrupt(ctx.Done())

	err := bucketExists(s.writer, bucket)
	if err == nil {
		return BucketExists{Bucket: bucket}
	}

	stmt := s.writer.Prep("INSERT INTO buckets (bucket, created) VALUES (?, ?);")
	stmt.BindText(1, bucket)
	stmt.BindInt64(2, time.Now().UnixNano())
	_, err = stmt.Step()
	return toMinioError(err, errSourceBuckets, bucket)
}

func (s *SQLite) GetBucketInfo(ctx context.Context, bucket string) (BucketInfo, error) {
	conn, err := s.getConn(ctx)
	if err != nil {
		return BucketInfo{}, err
	}
	defer s.readers.Put(conn)

	stmt := conn.Prep("SELECT created FROM buckets WHERE bucket = ?;")
	stmt.BindText(1, bucket)

	bi := BucketInfo{Name: bucket}
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
		return bi, BucketNotFound{Bucket: bucket}
	}

	return bi, nil
}

func (s *SQLite) ListBuckets(ctx context.Context) ([]BucketInfo, error) {
	conn, err := s.getConn(ctx)
	if err != nil {
		return nil, err
	}
	defer s.readers.Put(conn)

	stmt := conn.Prep("SELECT bucket, created FROM buckets;")

	var buckets []BucketInfo
	for {
		hasRow, err := stmt.Step()
		if err != nil {
			return nil, err
		}
		if !hasRow {
			break
		}

		b := BucketInfo{
			Name:    stmt.ColumnText(0),
			Created: columnTime(stmt, 1),
		}

		if isMinioMetaBucketName(b.Name) {
			continue
		}

		buckets = append(buckets, b)
	}

	return buckets, nil
}

func (s *SQLite) DeleteBucket(ctx context.Context, bucket string) (err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.writer.SetInterrupt(ctx.Done())

	stmt := s.writer.Prep("DELETE FROM buckets WHERE bucket = ?;")
	stmt.BindText(1, bucket)
	_, err = stmt.Step()
	return toMinioError(err, errSourceBuckets, bucket)
}

func (s *SQLite) ListObjects(ctx context.Context, bucket, prefix, marker, delimiter string, maxKeys int) (ListObjectsInfo, error) {
	err := checkListObjsArgs(ctx, bucket, prefix, marker, delimiter, s)
	if err != nil {
		return ListObjectsInfo{}, err
	}

	switch {
	case maxKeys < 0:
		maxKeys = maxObjectList
	case maxKeys == 0:
		return ListObjectsInfo{}, nil
	}

	conn, err := s.getConn(ctx)
	if err != nil {
		return ListObjectsInfo{}, err
	}
	defer s.readers.Put(conn)

	// TODO: all fields
	// TODO: is "object > marker" adequate?
	var stmt *sqlite.Stmt
	if delimiter == "" {
		stmt = conn.Prep("SELECT object as name, size, modified, etag FROM objects WHERE bucket = ? AND object LIKE ? || '%' AND object > ? ORDER BY object LIMIT ?;")
		stmt.BindText(1, bucket)
		stmt.BindText(2, prefix)
		stmt.BindText(3, marker)
		stmt.BindInt64(4, int64(maxKeys)+1)
	} else {
		stmt = conn.Prep(`
		SELECT DISTINCT $prefix || substr(ltrim(object, $prefix), 1, instr(ltrim(object, $prefix), $delimiter)) as name, 0 as size, 0 as modified, '' as etag, 1 as is_prefix
		FROM objects
		WHERE bucket = $bucket AND object LIKE $prefix || '%' AND object > $marker AND name <> $prefix
		UNION
		SELECT object as name, size, modified, etag, 0 as is_prefix
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

	var objects ListObjectsInfo
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
			etag     = stmt.GetText("etag")
			isPrefix = stmt.GetInt64("is_prefix") == 1 // TODO: this isn't set when no delimiter
		)
		objects.NextMarker = name
		if isPrefix {
			objects.Prefixes = append(objects.Prefixes, name)
			continue
		}
		objects.Objects = append(objects.Objects, ObjectInfo{
			Bucket:  bucket,
			Name:    name,
			Size:    size,
			ModTime: modified,
			ETag:    etag,
		})
	}

	return objects, nil
}
func (s *SQLite) ListObjectsV2(ctx context.Context, bucket, prefix, continuationToken, delimiter string, maxKeys int, fetchOwner bool, startAfter string) (ListObjectsV2Info, error) {
	// TODO: this implementation copied from FS, safe to assume it's correct?
	marker := continuationToken
	if marker == "" {
		marker = startAfter
	}

	v1, err := s.ListObjects(ctx, bucket, prefix, marker, delimiter, maxKeys)
	if err != nil {
		return ListObjectsV2Info{}, err
	}

	return ListObjectsV2Info{
		IsTruncated:           v1.IsTruncated,
		ContinuationToken:     continuationToken,
		NextContinuationToken: v1.NextMarker,
		Objects:               v1.Objects,
		Prefixes:              v1.Prefixes,
	}, err
}

// Object operations.

// GetObjectNInfo returns a GetObjectReader that satisfies the
// ReadCloser interface. The Close method unlocks the object
// after reading, so it must always be called after usage.
//
// IMPORTANTLY, when implementations return err != nil, this
// function MUST NOT return a non-nil ReadCloser.
func (s *SQLite) GetObjectNInfo(ctx context.Context, bucket, object string, rs *HTTPRangeSpec, h http.Header, lockType LockType, opts ObjectOptions) (*GetObjectReader, error) {
	if !IsValidBucketName(bucket) {
		return nil, BucketNameInvalid{Bucket: bucket}
	}
	if !IsValidObjectName(object) {
		return nil, ObjectNameInvalid{Bucket: bucket, Object: object}
	}

	conn, err := s.getConn(ctx)
	if err != nil {
		return nil, err
	}
	connInUse := false
	defer func() {
		if !connInUse {
			s.readers.Put(conn)
		}
	}()

	stmt := conn.Prep("SELECT object, blob_id, modified, size, metadata, etag FROM objects WHERE bucket = ? AND object LIKE ? || '%' ORDER BY object LIMIT 1;")
	stmt.BindText(1, bucket)
	stmt.BindText(2, object)

	var (
		name     string
		blobID   int64
		modified time.Time
		size     int64
		metadata map[string]string
		etag     string
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
		blobID = stmt.ColumnInt64(1)
		modified = columnTime(stmt, 2)
		size = stmt.ColumnInt64(3)
		err = json.NewDecoder(stmt.ColumnReader(4)).Decode(&metadata)
		if err != nil {
			return nil, err
		}
		etag = stmt.ColumnText(5)
	}

	switch {
	// object
	case name == object:
		startOffset, length, err := rs.GetOffsetLength(size)
		if err != nil {
			return nil, err
		}

		if startOffset > size || startOffset+length > size {
			return nil, InvalidRange{
				OffsetBegin:  startOffset,
				OffsetEnd:    length,
				ResourceSize: size,
			}
		}

		info := ObjectInfo{
			Bucket:  bucket,
			Name:    object,
			ModTime: modified,
			Size:    size,
			ETag:    etag,
		}

		stmt := conn.Prep("SELECT rowid FROM blobs WHERE id = ? ORDER BY part_id;")
		stmt.BindInt64(1, blobID)

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

			blobs = append(blobs, blob)
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
			s.readers.Put(conn)
		}
		return NewGetObjectReaderFromReader(rdr, info, closer), nil

	// directory
	case strings.HasSuffix(object, "/") || strings.HasPrefix(strings.TrimPrefix(name, object), "/"):
		info := ObjectInfo{
			Bucket: bucket,
			Name:   object,
			IsDir:  true,
		}
		return NewGetObjectReaderFromReader(bytes.NewReader(nil), info, func() {}), nil

	default:
		return nil, ObjectNotFound{
			Bucket: bucket,
			Object: object,
		}
	}
}
func (s *SQLite) GetObject(ctx context.Context, bucket, object string, startOffset, length int64, writer io.Writer, etag string, opts ObjectOptions) error {
	err := checkGetObjArgs(ctx, bucket, object)
	if err != nil {
		return err
	}
	if startOffset < 0 || writer == nil {
		return errUnexpected
	}

	conn, err := s.getConn(ctx)
	if err != nil {
		return err
	}
	defer s.readers.Put(conn)

	// Check if directory.
	if hasSuffix(object, slashSeparator) {
		stmt := conn.Prep("SELECT 1 FROM objects WHERE bucket = ? AND object LIKE ? || '%' LIMIT 1;")
		stmt.BindText(1, bucket)
		stmt.BindText(2, object)
		var exists bool
		for {
			hasRow, err := stmt.Step()
			if err != nil {
				return err
			}
			if !hasRow {
				break
			}

			exists = stmt.ColumnInt64(0) == 1
		}
		if !exists {
			return ObjectNotFound{Bucket: bucket, Object: object}
		}
		return nil
	}

	stmt := conn.Prep("SELECT blob_id, size FROM objects WHERE bucket = ? AND object = ?;")
	stmt.BindText(1, bucket)
	stmt.BindText(2, object)

	var (
		blobID int64
		size   int64
	)
	for {
		hasRow, err := stmt.Step()
		if err != nil {
			return err
		}
		if !hasRow {
			break
		}

		blobID = stmt.ColumnInt64(0)
		size = stmt.ColumnInt64(1)
	}

	if startOffset > size || startOffset+length > size {
		return InvalidRange{
			OffsetBegin:  startOffset,
			OffsetEnd:    length,
			ResourceSize: size,
		}
	}

	stmt = conn.Prep("SELECT rowid FROM blobs WHERE id = ? ORDER BY part_id;")
	stmt.BindInt64(1, blobID)

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
		return nil, ObjectNotFound{Bucket: bucket, Object: object}

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

func (s *SQLite) GetObjectInfo(ctx context.Context, bucket, object string, opts ObjectOptions) (ObjectInfo, error) {
	err := checkGetObjArgs(ctx, bucket, object)
	if err != nil {
		return ObjectInfo{}, err
	}

	conn, err := s.getConn(ctx)
	if err != nil {
		return ObjectInfo{}, err
	}
	defer s.readers.Put(conn)

	// TODO: run all multiquery in transaction?
	err = bucketExists(conn, bucket)
	if err != nil {
		return ObjectInfo{}, err
	}

	stmt := conn.Prep("SELECT object, size, modified, etag, content_type, metadata FROM objects WHERE bucket = ? AND object LIKE ? || '%' ORDER BY object LIMIT 1;")
	stmt.BindText(1, bucket)
	stmt.BindText(2, object)

	var (
		name        string
		size        int64
		modified    time.Time
		etag        string
		contentType string
		metadata    map[string]string
	)
	for {
		hasRow, err := stmt.Step()
		if err != nil {
			return ObjectInfo{}, err
		}
		if !hasRow {
			break
		}

		name = stmt.ColumnText(0)
		size = stmt.ColumnInt64(1)
		modified = columnTime(stmt, 2)
		etag = stmt.ColumnText(3)
		contentType = stmt.ColumnText(4)
		err = json.NewDecoder(stmt.ColumnReader(5)).Decode(&metadata)
		if err != nil {
			return ObjectInfo{}, err
		}
	}

	isDir := strings.HasSuffix(object, "/")

	switch {
	// object
	case name == object && !isDir:
		return ObjectInfo{
			Bucket:      bucket,
			Name:        object,
			Size:        size,
			ModTime:     modified,
			ETag:        etag,
			ContentType: contentType,
			UserDefined: metadata,
		}, nil

	// directory
	case isDir || strings.HasPrefix(strings.TrimPrefix(name, object), "/"): // handle directories that were not explicitly created
		return ObjectInfo{
			Bucket:      bucket,
			Name:        object,
			IsDir:       true,
			ContentType: "application/octet-stream",
		}, nil

	default:
		return ObjectInfo{}, ObjectNotFound{
			Bucket: bucket,
			Object: object,
		}
	}
}

func (s *SQLite) PutObject(ctx context.Context, bucket, object string, data *PutObjReader, metadata map[string]string, opts ObjectOptions) (_ ObjectInfo, err error) {
	size := data.Size()
	err = checkPutObjectArgs(ctx, bucket, object, s, size)
	if err != nil {
		return ObjectInfo{}, err
	}

	mdJSON, err := json.Marshal(metadata)
	if err != nil {
		return ObjectInfo{}, err
	}

	var (
		src io.Reader
		n   int64
	)
	if size > tempFileThreshold {
		f, err := ioutil.TempFile(s.tempDir, "")
		if err != nil {
			return ObjectInfo{}, err
		}
		defer f.Close()
		defer os.Remove(f.Name())

		n, err = io.Copy(f, data)
		if err != nil {
			return ObjectInfo{}, err
		}
		if n < size {
			return ObjectInfo{}, IncompleteBody{}
		}
		_, err = f.Seek(0, 0)
		if err != nil {
			return ObjectInfo{}, err
		}
		src = f
	} else {
		b := make([]byte, size)
		_, err = io.ReadFull(data, b)
		if err != nil {
			if err == io.ErrUnexpectedEOF {
				err = IncompleteBody{}
			}
			return ObjectInfo{}, err
		}
		src = bytes.NewReader(b)
	}

	// TODO: apply to copy object
	// TODO: ensure multipart checks mime
	etag := data.MD5CurrentHexString()
	contentType := mimedb.TypeByExtension(path.Ext(object))

	s.mu.Lock()
	defer s.mu.Unlock()
	s.writer.SetInterrupt(ctx.Done())

	defer sqlitex.Save(s.writer)(&err)

	stmt := s.writer.Prep("INSERT INTO blobs (id, data) VALUES ((SELECT COALESCE(MAX(id), 0)+1 FROM blobs), ?);")
	stmt.BindZeroBlob(1, size)
	_, err = stmt.Step()
	if err != nil {
		return ObjectInfo{}, err
	}

	rowID := s.writer.LastInsertRowID()
	blob, err := s.writer.OpenBlob("", "blobs", "data", rowID, true)
	if err != nil {
		return ObjectInfo{}, err
	}

	_, err = io.CopyBuffer(blob, src, s.copyBuffer)
	blob.Close()
	if err != nil {
		return ObjectInfo{}, err
	}

	// TODO: handle removal of previous blob
	stmt = s.writer.Prep(`
		INSERT INTO objects (bucket, object, size, metadata, modified, blob_id, etag, content_type) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (bucket, object) DO UPDATE SET size=excluded.size, metadata=excluded.metadata, modified=excluded.modified, blob_id=excluded.blob_id, etag=excluded.etag;`)
	stmt.BindText(1, bucket)
	stmt.BindText(2, object)
	stmt.BindInt64(3, size)
	stmt.BindBytes(4, mdJSON)
	stmt.BindInt64(5, time.Now().UnixNano())
	stmt.BindInt64(6, rowID)
	stmt.BindText(7, etag)
	stmt.BindText(8, contentType)
	_, err = stmt.Step()
	if err != nil {
		return ObjectInfo{}, err
	}

	return ObjectInfo{
		Bucket: bucket,
		Name:   object,
		ETag:   etag,
	}, nil
}
func (s *SQLite) CopyObject(ctx context.Context, srcBucket, srcObject, destBucket, destObject string, srcInfo ObjectInfo, srcOpts, dstOpts ObjectOptions) (ObjectInfo, error) {
	// TODO: metadata only update

	s.mu.Lock()
	defer s.mu.Unlock()
	s.writer.SetInterrupt(ctx.Done())

	stmt := s.writer.Prep(`
	INSERT INTO objects(bucket, object, metadata, size, blob_id, modified)
	SELECT ?, ?, metadata, size, blob_id, ? FROM objects WHERE bucket = ? AND object = ?
	ON CONFLICT (bucket, object) DO UPDATE SET metadata = excluded.metadata, size = excluded.size, blob_id = excluded.blob_id, modified = excluded.modified;`)
	stmt.BindText(1, destBucket)
	stmt.BindText(2, destObject)
	stmt.BindInt64(3, time.Now().UnixNano())
	stmt.BindText(4, srcBucket)
	stmt.BindText(5, srcObject)
	_, err := stmt.Step()
	if err != nil {
		return ObjectInfo{}, err
	}

	return ObjectInfo{
		Bucket: destBucket,
		Name:   destObject,
	}, nil
}
func (s *SQLite) DeleteObject(ctx context.Context, bucket, object string) (err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.writer.SetInterrupt(ctx.Done())

	defer sqlitex.Save(s.writer)(&err)

	err = bucketExists(s.writer, bucket)
	if err != nil {
		return err
	}

	stmt := s.writer.Prep("DELETE FROM objects WHERE bucket = ? AND object = ?;")
	stmt.BindText(1, bucket)
	stmt.BindText(2, object)
	_, err = stmt.Step()
	if err != nil {
		return err
	}

	if s.writer.Changes() == 0 {
		return ObjectNotFound{
			Bucket: bucket,
			Object: object,
		}
	}

	return nil
}

// Multipart operations.
func (s *SQLite) ListMultipartUploads(ctx context.Context, bucket, prefix, keyMarker, uploadIDMarker, delimiter string, maxUploads int) (_ ListMultipartsInfo, err error) {
	err = checkListMultipartArgs(ctx, bucket, prefix, keyMarker, uploadIDMarker, delimiter, s)
	if err != nil {
		return ListMultipartsInfo{}, err
	}

	res := ListMultipartsInfo{
		KeyMarker:      keyMarker,
		UploadIDMarker: uploadIDMarker,
		MaxUploads:     maxUploads,
		Prefix:         prefix,
		Delimiter:      delimiter,
		// EncodingType
	}

	conn, err := s.getConn(ctx)
	if err != nil {
		return res, err
	}
	defer s.readers.Put(conn)

	defer sqlitex.Save(conn)(&err)

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
		res.Uploads = append(res.Uploads, MultipartInfo{
			Object:   name,
			UploadID: id,
			// TODO: Initiated
		})
	}

	return res, nil
}

func bucketExists(conn *sqlite.Conn, bucket string) error {
	stmt := conn.Prep("SELECT 1 FROM buckets WHERE bucket = ?;")
	stmt.BindText(1, bucket)

	var exists bool
	for {
		hasRow, err := stmt.Step()
		if err != nil {
			return err
		}
		if !hasRow {
			break
		}

		exists = stmt.ColumnInt64(0) == 1
	}

	if !exists {
		return BucketNotFound{Bucket: bucket}
	}
	return nil
}

func (s *SQLite) NewMultipartUpload(ctx context.Context, bucket, object string, metadata map[string]string, opts ObjectOptions) (_ string, err error) {
	err = checkNewMultipartArgs(ctx, bucket, object, s)
	if err != nil {
		return "", err
	}

	id := MustGetUUID()
	mdJSON, err := json.Marshal(metadata)
	if err != nil {
		return "", err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.writer.SetInterrupt(ctx.Done())

	defer sqlitex.Save(s.writer)(&err)

	// reserve a blob_id
	stmt := s.writer.Prep("INSERT INTO blobs (id, part_id, data) VALUES ((SELECT COALESCE(MAX(id), 0)+1 FROM blobs), -1, 0);")
	_, err = stmt.Step()
	if err != nil {
		return "", err
	}
	rowID := s.writer.LastInsertRowID()

	stmt = s.writer.Prep("INSERT INTO uploads (id, bucket, object, metadata, blob_id) VALUES (?, ?, ?, ?, (SELECT id FROM blobs WHERE rowid = ?))")
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

func (s *SQLite) CopyObjectPart(ctx context.Context, srcBucket, srcObject, destBucket, destObject string, uploadID string, partID int, startOffset int64, length int64, srcInfo ObjectInfo, srcOpts, dstOpts ObjectOptions) (PartInfo, error) {
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
		return -1, InvalidUploadID{UploadID: uploadID}
	}
	return blobID, nil
}

func (s *SQLite) PutObjectPart(ctx context.Context, bucket, object, uploadID string, partID int, data *PutObjReader, opts ObjectOptions) (_ PartInfo, err error) {
	err = checkPutObjectPartArgs(ctx, bucket, object, s)
	if err != nil {
		return PartInfo{}, err
	}

	size := data.Size()

	var src io.Reader
	if size > tempFileThreshold {
		f, err := ioutil.TempFile(s.tempDir, "")
		if err != nil {
			return PartInfo{}, err
		}
		defer f.Close()
		defer os.Remove(f.Name())

		_, err = io.Copy(f, data)
		if err != nil {
			return PartInfo{}, err
		}
		_, err = f.Seek(0, 0)
		if err != nil {
			return PartInfo{}, err
		}
		src = f
	} else {
		b, err := ioutil.ReadAll(data)
		if err != nil {
			return PartInfo{}, err
		}
		src = bytes.NewReader(b)
	}

	etag := data.MD5CurrentHexString()

	s.mu.Lock()
	defer s.mu.Unlock()
	s.writer.SetInterrupt(ctx.Done())

	defer sqlitex.Save(s.writer)(&err)

	blobID, err := getBlobID(s.writer, bucket, object, uploadID)
	if err != nil {
		return PartInfo{}, err
	}

	stmt := s.writer.Prep("INSERT INTO blobs (id, part_id, data, etag) VALUES (?, ?, ?, ?) ON CONFLICT (id, part_id) DO UPDATE SET data = excluded.data;")
	stmt.BindInt64(1, blobID)
	stmt.BindInt64(2, int64(partID))
	stmt.BindZeroBlob(3, size)
	stmt.BindText(4, etag)
	_, err = stmt.Step()
	if err != nil {
		return PartInfo{}, err
	}

	// must query rowid since last insert will be incorrect on upsert
	stmt = s.writer.Prep("SELECT rowid FROM blobs WHERE id = ? AND part_id = ?")
	stmt.BindInt64(1, blobID)
	stmt.BindInt64(2, int64(partID))
	rowID, err := sqlitex.ResultInt64(stmt)
	if err != nil {
		return PartInfo{}, err
	}

	blob, err := s.writer.OpenBlob("", "blobs", "data", rowID, true)
	if err != nil {
		return PartInfo{}, err
	}
	defer blob.Close()

	_, err = io.CopyBuffer(blob, src, s.copyBuffer)
	if err != nil {
		return PartInfo{}, err
	}

	return PartInfo{PartNumber: partID, ETag: etag}, nil
}

func (s *SQLite) ListObjectParts(ctx context.Context, bucket, object, uploadID string, partNumberMarker int, maxParts int) (ListPartsInfo, error) {
	err := checkListPartsArgs(ctx, bucket, object, s)
	if err != nil {
		return ListPartsInfo{}, err
	}

	res := ListPartsInfo{
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
	defer s.readers.Put(conn)

	blobID, err := getBlobID(conn, bucket, object, uploadID)
	if err != nil {
		return ListPartsInfo{}, err
	}

	stmt := conn.Prep("SELECT part_id, length(data), etag FROM blobs WHERE id = ? AND part_id <> -1 AND part_id > ? ORDER BY part_id LIMIT ?;")
	stmt.BindInt64(1, blobID)
	stmt.BindInt64(2, int64(partNumberMarker))
	stmt.BindInt64(3, int64(maxParts)+1)

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
			etag   = stmt.ColumnText(2)
		)

		res.NextPartNumberMarker = partID
		res.Parts = append(res.Parts, PartInfo{
			PartNumber: partID,
			Size:       size,
			ETag:       etag,
		})
	}

	if !res.IsTruncated {
		res.NextPartNumberMarker = 0
	}

	return res, nil
}
func (s *SQLite) AbortMultipartUpload(ctx context.Context, bucket, object, uploadID string) (err error) {
	err = checkAbortMultipartArgs(ctx, bucket, object, s)
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.writer.SetInterrupt(ctx.Done())

	defer sqlitex.Save(s.writer)(&err)

	err = bucketExists(s.writer, bucket)
	if err != nil {
		return err
	}

	blobID, err := getBlobID(s.writer, bucket, object, uploadID)
	if err != nil {
		return err
	}

	stmt := s.writer.Prep("DELETE FROM uploads WHERE id = ?;")
	stmt.BindText(1, uploadID)
	_, err = stmt.Step()
	if err != nil {
		return toMinioError(err, errSourceUploads, uploadID)
	}

	stmt = s.writer.Prep("DELETE FROM blobs WHERE id = ?;")
	stmt.BindInt64(1, blobID)
	_, err = stmt.Step()
	return toMinioError(err, errSourceUploads, uploadID)
}

func (s *SQLite) CompleteMultipartUpload(ctx context.Context, bucket, object, uploadID string, uploadedParts []CompletePart, opts ObjectOptions) (_ ObjectInfo, err error) {
	err = checkCompleteMultipartArgs(ctx, bucket, object, s)
	if err != nil {
		return ObjectInfo{}, err
	}

	contentType := mimedb.TypeByExtension(path.Ext(object))
	etag, err := getCompleteMultipartMD5(ctx, uploadedParts)
	if err != nil {
		return ObjectInfo{}, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.writer.SetInterrupt(ctx.Done())

	defer sqlitex.Save(s.writer)(&err)

	blobID, err := getBlobID(s.writer, bucket, object, uploadID)
	if err != nil {
		return ObjectInfo{}, err
	}

	// get existing parts
	stmt := s.writer.Prep("SELECT part_id, etag, length(data) FROM blobs WHERE id = ? ORDER BY part_id;")
	if err != nil {
		return ObjectInfo{}, err
	}
	stmt.BindInt64(1, blobID)

	type partInfo struct {
		etag string
		size int64
	}
	dbPartIDs := make(map[int]partInfo)
	for {
		hasRow, err := stmt.Step()
		if err != nil {
			return ObjectInfo{}, err
		}
		if !hasRow {
			break
		}

		id := stmt.ColumnInt(0)
		etag := stmt.ColumnText(1)
		size := stmt.ColumnInt64(2)
		dbPartIDs[id] = partInfo{
			etag: etag,
			size: size,
		}
	}

	// validate part numbers
	lastPart := len(uploadedParts) - 1
	for i, part := range uploadedParts {
		dbPart, ok := dbPartIDs[part.PartNumber]
		if !ok || part.ETag != dbPart.etag {
			return ObjectInfo{}, InvalidPart{PartNumber: part.PartNumber}
		}
		if !isMinAllowedPartSize(dbPart.size) && i != lastPart {
			return ObjectInfo{}, PartTooSmall{
				PartNumber: part.PartNumber,
				PartSize:   dbPart.size,
				PartETag:   part.ETag,
			}
		}
		delete(dbPartIDs, part.PartNumber)
	}

	// remove any extraneous parts, including marker part_id of -1
	for partID := range dbPartIDs {
		stmt = s.writer.Prep("DELETE FROM blobs WHERE id = ? AND part_id = ?;")
		stmt.BindInt64(1, blobID)
		stmt.BindInt64(2, int64(partID))
		_, err = stmt.Step()
		if err != nil {
			return ObjectInfo{}, toMinioError(err, errSourceUploads, uploadID)
		}
	}

	// create object
	stmt = s.writer.Prep(`
		INSERT INTO objects (bucket, object, size, metadata, modified, blob_id, etag, content_type) VALUES (
			?1,
			?2,
			(SELECT SUM(LENGTH(data)) FROM blobs WHERE id = ?3),
			(SELECT metadata FROM uploads WHERE id = ?4),
			?5,
			?3,
			?6,
			?7
		)
		ON CONFLICT (bucket, object) DO UPDATE SET size=excluded.size, metadata=excluded.metadata, modified=excluded.modified, blob_id=excluded.blob_id;`)
	stmt.BindText(1, bucket)
	stmt.BindText(2, object)
	stmt.BindInt64(3, blobID)
	stmt.BindText(4, uploadID)
	stmt.BindInt64(5, time.Now().UnixNano())
	stmt.BindText(6, etag)
	stmt.BindText(7, contentType)
	_, err = stmt.Step()
	if err != nil {
		return ObjectInfo{}, toMinioError(err, errSourceObjects, object)
	}

	// remove upload
	stmt = s.writer.Prep("DELETE FROM uploads WHERE id = ?;")
	stmt.BindText(1, uploadID)
	_, err = stmt.Step()
	if err != nil {
		return ObjectInfo{}, toMinioError(err, errSourceUploads, uploadID)
	}

	return ObjectInfo{
		Bucket: bucket,
		Name:   object,
		ETag:   etag,
	}, nil
}

// Healing operations.
func (s *SQLite) ReloadFormat(ctx context.Context, dryRun bool) error {
	return NotImplemented{}
}
func (s *SQLite) HealFormat(ctx context.Context, dryRun bool) (madmin.HealResultItem, error) {
	return madmin.HealResultItem{}, NotImplemented{}
}
func (s *SQLite) HealBucket(ctx context.Context, bucket string, dryRun bool) ([]madmin.HealResultItem, error) {
	return nil, NotImplemented{}
}
func (s *SQLite) HealObject(ctx context.Context, bucket, object string, dryRun bool) (madmin.HealResultItem, error) {
	return madmin.HealResultItem{}, NotImplemented{}
}
func (s *SQLite) ListBucketsHeal(ctx context.Context) ([]BucketInfo, error) {
	return nil, NotImplemented{}
}
func (s *SQLite) ListObjectsHeal(ctx context.Context, bucket, prefix, marker, delimiter string, maxKeys int) (ListObjectsInfo, error) {
	return ListObjectsInfo{}, NotImplemented{}
}

// Policy operations
func (s *SQLite) SetBucketPolicy(ctx context.Context, bucket string, policy *policy.Policy) error {
	policyJSON, err := json.Marshal(policy)
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.writer.SetInterrupt(ctx.Done())

	stmt := s.writer.Prep("UPDATE buckets SET policy = ? WHERE bucket = ?;")
	stmt.BindBytes(1, policyJSON)
	stmt.BindText(2, bucket)
	_, err = stmt.Step()
	if err != nil {
		return toMinioError(err, errSourceBuckets, bucket)
	}

	if s.writer.Changes() == 0 {
		return BucketNotFound{Bucket: bucket}
	}

	return nil
}

func (s *SQLite) GetBucketPolicy(ctx context.Context, bucket string) (*policy.Policy, error) {
	conn, err := s.getConn(ctx)
	if err != nil {
		return nil, err
	}
	defer s.readers.Put(conn)

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
		return nil, BucketPolicyNotFound{Bucket: bucket}
	}

	blob, err := conn.OpenBlob("", "buckets", "policy", rowID, false)
	if err != nil {
		return nil, toMinioError(err, errSourceBuckets, bucket)
	}
	defer blob.Close()

	return policy.ParseConfig(blob, bucket)
}

func (s *SQLite) DeleteBucketPolicy(ctx context.Context, bucket string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.writer.SetInterrupt(ctx.Done())

	stmt := s.writer.Prep("UPDATE buckets SET policy = NULL WHERE bucket = ?;")
	stmt.BindText(1, bucket)
	_, err := stmt.Step()

	return err
}

// Supported operations check
func (s *SQLite) IsNotificationSupported() bool { return true }
func (s *SQLite) IsEncryptionSupported() bool   { return true }
func (s *SQLite) IsCompressionSupported() bool  { return true }
