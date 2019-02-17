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
	"time"

	"crawshaw.io/sqlite"
	"crawshaw.io/sqlite/sqlitex"
	"github.com/minio/minio/pkg/madmin"
	"github.com/minio/minio/pkg/mimedb"
	"github.com/minio/minio/pkg/policy"
)

// TODO: add error logging
// TODO: support WORM mode

const (
	sqliteBufferSize        = 16 * 1024 * 1024
	sqliteTempFileThreshold = 16 * 1024 * 1024
	sqliteReadPoolSize      = 10 // TODO: what size?
)

// Multipart uploads are maintained as individual parts to avoid
// the overhead of recombining on completion. This also allows objects
// larger than SQLITE_MAX_LENGTH to be stored.
//
// The objects table uses "WITHOUT ROWID" because early experimentation found it to
// be faster for listing objects in a bucket. Since there are tradeoffs to this
// the results should be checked again once the implementation is complete.
// https://www.sqlite.org/withoutrowid.html
//
// Timestamps are stored as nanoseconds since unix epoch.
//
// Migrations are applied in order. To adjust the schema
// add another script to the slice. Do not remove or reorder
// scripts.
var sqliteMigrations = []string{
	`
	CREATE TABLE internal (
		key    TEXT PRIMARY KEY NOT NULL,
		value  NONE NOT NULL
	);
	CREATE TABLE buckets (
		bucket   TEXT    PRIMARY KEY NOT NULL,
		policy   BLOB,
		created  INTEGER NOT NULL
	);
	CREATE TABLE blobs (
		id       INTEGER NOT NULL,
		part_id  INTEGER NOT NULL DEFAULT 0,
		etag     TEXT,
		data     BLOB    NOT NULL,
		PRIMARY KEY (id, part_id)
	);
	CREATE TABLE objects (
		bucket        TEXT    REFERENCES buckets ON DELETE RESTRICT ON UPDATE CASCADE,
		object        TEXT    NOT NULL,
		metadata      TEXT    NOT NULL,
		size          INTEGER NOT NULL,
		modified      INTEGER NOT NULL,
		blob_id       INTEGER NOT NULL,
		etag          TEXT    NOT NULL,
		content_type  TEXT    NOT NULL,
		PRIMARY KEY (bucket, object)
	) WITHOUT ROWID;
	CREATE TABLE uploads (
		id         TEXT    PRIMARY KEY NOT NULL,
		bucket     TEXT    REFERENCES buckets ON DELETE CASCADE ON UPDATE CASCADE,
		object     TEXT    NOT NULL,
		metadata   TEXT    NOT NULL,
		initiated  INTEGER NOT NULL,
		blob_id    INTEGER NOT NULL
	);
	CREATE TRIGGER remove_object AFTER DELETE ON objects
	BEGIN
		DELETE FROM blobs WHERE id = OLD.blob_id AND NOT EXISTS (SELECT 1 FROM objects WHERE blob_id = OLD.blob_id);
	END;
	CREATE TRIGGER update_object AFTER UPDATE OF blob_id ON objects
	BEGIN
		DELETE FROM blobs WHERE id = OLD.blob_id AND NOT EXISTS (SELECT 1 FROM objects WHERE blob_id = OLD.blob_id);
	END;
	CREATE TRIGGER remove_upload AFTER DELETE ON uploads
	BEGIN
		DELETE FROM blobs WHERE id = OLD.blob_id AND NOT EXISTS (SELECT 1 FROM objects WHERE blob_id = OLD.blob_id);
	END;`,
}

type SQLite struct {
	// Object PUTs larger than sqliteTempFileThreshold are stored
	// in a temporary directory before being inserted into SQLite.
	// This prevents a slow client from blocking all write operations.
	tempDir string

	// Pool for readonly operations.
	readers *sqlitex.Pool

	// SQLite only allows a single writer at a time. To simplify
	// the code, have a dedicated writer conn protected my mutex.
	mu         sync.Mutex
	writer     *sqlite.Conn
	copyBuffer []byte // buffer used for writer io.CopyBuffer
}

func NewSQLiteLayer(uri *url.URL) (ObjectLayer, error) {
	if uri.Scheme == "sqlite" {
		uri.Scheme = "file"
	}
	file := uri.String()
	conn, err := sqlite.OpenConn(file, 0)
	if err != nil {
		return nil, err
	}
	sqliteInitConn(conn)

	// Only writer needs foreign_keys enabled.
	err = sqlitex.ExecTransient(conn, "PRAGMA foreign_keys = ON;", nil)
	if err != nil {
		return nil, err
	}

	// Create read pool
	const roFlags = sqlite.SQLITE_OPEN_READONLY | sqlite.SQLITE_OPEN_URI | sqlite.SQLITE_OPEN_NOMUTEX
	pool, err := sqlitex.Open(file, roFlags, sqliteReadPoolSize)
	if err != nil {
		return nil, err
	}

	// Create tempdir for PUTs
	tempDir, err := ioutil.TempDir("", ".minio_sqlite")
	if err != nil {
		return nil, err
	}

	s := &SQLite{
		tempDir:    tempDir,
		readers:    pool,
		writer:     conn,
		copyBuffer: make([]byte, sqliteBufferSize),
	}

	err = s.init()
	if err != nil {
		return nil, err
	}

	return s, nil
	return &DebugLayer{Wrapped: s, LogReturns: true, LogCallers: 1}, nil // TODO: remove
}

// sqliteInitConn applies settings that every connection should have.
func sqliteInitConn(conn *sqlite.Conn) {
	// Set very long busy timeout. Timeouts are enforced by conn.SetInterrupt(ctx.Done()).
	// This is done explicitly on the writer and implicitly by sqlitex.Pool.Get(ctx)
	// for readers.
	conn.SetBusyTimeout(5 * time.Minute)
}

func (s *SQLite) init() (err error) {
	// Start a savepoint to ensure migrations are applied atomically.
	defer sqlitex.Save(s.writer)(&err)

	// Check if this is a new database or not.
	stmt, _, err := s.writer.PrepareTransient("SELECT value FROM internal WHERE key = 'schema_version';")
	noVersion := err != nil && strings.Contains(err.Error(), "no such table")
	if err != nil && !noVersion {
		return err
	}

	// If internal table exists, retrieve schema version.
	migrationStart := 0
	if !noVersion {
		_, err = sqliteExecResults(stmt, func() error {
			migrationStart = stmt.ColumnInt(0)
			return nil
		})
		stmt.Finalize()
		if err != nil {
			return err
		}
	}

	// Execute migration necessary scripts.
	for _, migration := range sqliteMigrations[migrationStart:] {
		err := sqlitex.ExecScript(s.writer, migration)
		if err != nil {
			return err
		}
	}

	// Update schema version.
	stmt, _, err = s.writer.PrepareTransient(`
		INSERT INTO internal(key, value)
		VALUES ('schema_version', ?)
		ON CONFLICT (key) DO UPDATE
		SET value = EXCLUDED.value;`)
	stmt.BindInt64(1, int64(len(sqliteMigrations)))
	if err != nil {
		return err
	}
	err = sqliteExec(stmt)
	stmt.Finalize()
	if err != nil {
		return err
	}

	// Create minioMetaBucket if it doesn't exist.
	err = bucketExists(s.writer, minioMetaBucket)
	if err == nil {
		return nil
	}
	return s.MakeBucketWithLocation(context.Background(), minioMetaBucket, "")
}

func (s *SQLite) Shutdown(context.Context) error {
	err1 := s.readers.Close()
	err2 := s.writer.Close()
	err3 := os.RemoveAll(s.tempDir)

	switch {
	case err1 != nil:
		return err1
	case err2 != nil:
		return err2
	case err3 != nil:
		return err3
	}
	return nil
}

func (s *SQLite) StorageInfo(ctx context.Context) StorageInfo {
	size, _ := s.dbSize(ctx)

	var si StorageInfo
	si.Used = uint64(size)
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

var errSQLiteClosed = errors.New("sqlite: db closed")

// getConn retrieves a readonly conn from the pool and initializes it.
func (s *SQLite) getConn(ctx context.Context) (*sqlite.Conn, error) {
	conn := s.readers.Get(ctx)
	if conn == nil {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return nil, errSQLiteClosed
	}
	sqliteInitConn(conn)

	return conn, nil
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
	return sqliteExec(stmt)
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
	rows, err := sqliteExecResults(stmt, func() error {
		bi.Created = columnTime(stmt, 0)
		return nil
	})
	if err != nil {
		return bi, err
	}

	if rows == 0 {
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
	_, err = sqliteExecResults(stmt, func() error {
		b := BucketInfo{
			Name:    stmt.ColumnText(0),
			Created: columnTime(stmt, 1),
		}

		if isMinioMetaBucketName(b.Name) {
			return nil
		}

		buckets = append(buckets, b)
		return nil
	})

	return buckets, err
}

func (s *SQLite) DeleteBucket(ctx context.Context, bucket string) (err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.writer.SetInterrupt(ctx.Done())

	stmt := s.writer.Prep("DELETE FROM buckets WHERE bucket = ?;")
	stmt.BindText(1, bucket)

	err = sqliteExec(stmt)
	if sqlite.ErrCode(err) == sqlite.SQLITE_CONSTRAINT_TRIGGER {
		return BucketNotEmpty{Bucket: bucket}
	}
	return err
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

	var stmt *sqlite.Stmt
	if delimiter == "" {
		stmt = conn.Prep(`
			SELECT object as name, size, modified, etag FROM objects
			WHERE
				bucket = ? AND
				instr(object, ?) = 1 AND
				object > ?
			ORDER BY object LIMIT ?;`)
		stmt.BindText(1, bucket)
		stmt.BindText(2, prefix)
		stmt.BindText(3, marker)
		stmt.BindInt64(4, int64(maxKeys)+1)
	} else {
		stmt = conn.Prep(`
		SELECT DISTINCT
			substr(object, 1, length($prefix) + instr(substr(object, length($prefix)+1), $delimiter)) as name,
			0 as size,
			0 as modified,
			'' as etag,
			1 as is_prefix
		FROM objects
		WHERE
			bucket = $bucket AND
			instr(object, $prefix) = 1 AND
			object > $marker AND
			name <> $prefix
		UNION
		SELECT object as name, size, modified, etag, 0 as is_prefix
		FROM objects
		WHERE
			bucket = $bucket AND
			instr(object, $prefix) = 1 AND
			object > $marker AND
			instr(substr(object, length($prefix)+1), $delimiter) = 0
		ORDER BY object
		LIMIT $max_keys;`)
		stmt.SetText("$bucket", bucket)
		stmt.SetText("$prefix", prefix)
		stmt.SetText("$marker", marker)
		stmt.SetText("$delimiter", delimiter)
		stmt.SetInt64("$max_keys", int64(maxKeys)+1)
	}

	var count int
	var objects ListObjectsInfo
	_, err = sqliteExecResults(stmt, func() error {
		count++
		if count > maxKeys {
			objects.IsTruncated = true
			return nil
		}

		name := stmt.ColumnText(0)
		objects.NextMarker = name
		if delimiter != "" && stmt.ColumnInt64(4) == 1 { // is_prefix
			objects.Prefixes = append(objects.Prefixes, name)
			return nil
		}

		objects.Objects = append(objects.Objects, ObjectInfo{
			Bucket:  bucket,
			Name:    name,
			Size:    stmt.ColumnInt64(1),
			ModTime: columnTime(stmt, 2),
			ETag:    stmt.ColumnText(3),
		})

		return nil
	})
	if err != nil {
		return objects, err
	}

	if !objects.IsTruncated {
		objects.NextMarker = ""
	}

	return objects, nil
}
func (s *SQLite) ListObjectsV2(ctx context.Context, bucket, prefix, continuationToken, delimiter string, maxKeys int, fetchOwner bool, startAfter string) (ListObjectsV2Info, error) {
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
	err := checkGetObjArgs(ctx, bucket, object)
	if err != nil {
		return nil, err
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

	stmt := conn.Prep("SELECT object, blob_id, modified, size, metadata, etag FROM objects WHERE bucket = ? AND instr(object, ?) = 1 ORDER BY object LIMIT 1;")
	stmt.BindText(1, bucket)
	stmt.BindText(2, object)

	var (
		oi     ObjectInfo
		blobID int64
	)
	n, err := sqliteExecResults(stmt, func() error {
		oi = ObjectInfo{
			Name:    stmt.ColumnText(0),
			ModTime: columnTime(stmt, 2),
			Size:    stmt.ColumnInt64(3),
			ETag:    stmt.ColumnText(5),
		}
		err := json.NewDecoder(stmt.ColumnReader(4)).Decode(&oi.UserDefined)
		if err != nil {
			return err
		}
		blobID = stmt.ColumnInt64(1)
		return nil
	})
	if err != nil {
		return nil, err
	}

	if n == 0 {
		return nil, ObjectNotFound{
			Bucket: bucket,
			Object: object,
		}
	}

	// directory
	if strings.HasSuffix(object, slashSeparator) || strings.HasPrefix(strings.TrimPrefix(oi.Name, object), slashSeparator) {
		info := ObjectInfo{
			Bucket: bucket,
			Name:   object,
			IsDir:  true,
		}
		return NewGetObjectReaderFromReader(bytes.NewReader(nil), info, func() {}), nil
	}

	startOffset, length, err := rs.GetOffsetLength(oi.Size)
	if err != nil {
		return nil, err
	}

	if startOffset > oi.Size || startOffset+length > oi.Size {
		return nil, InvalidRange{
			OffsetBegin:  startOffset,
			OffsetEnd:    length,
			ResourceSize: oi.Size,
		}
	}

	var blobs []*sqlite.Blob
	defer func() {
		if connInUse {
			return
		}
		for _, blob := range blobs {
			blob.Close()
		}
	}()

	stmt = conn.Prep("SELECT rowid FROM blobs WHERE id = ? ORDER BY part_id;")
	stmt.BindInt64(1, blobID)
	_, err = sqliteExecResults(stmt, func() error {
		rowID := stmt.ColumnInt64(0)

		blob, err := conn.OpenBlob("", "blobs", "data", rowID, false)
		if err != nil {
			return err
		}

		blobs = append(blobs, blob)
		return nil
	})
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
		s.readers.Put(conn)
	}
	return NewGetObjectReaderFromReader(rdr, oi, closer), nil
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
		stmt := conn.Prep("SELECT 1 FROM objects WHERE bucket = ? AND instr(object, ?) = 1 LIMIT 1;")
		stmt.BindText(1, bucket)
		stmt.BindText(2, object)
		_, err = sqliteExecResults(stmt, func() error {
			if stmt.ColumnInt64(0) != 1 {
				return ObjectNotFound{Bucket: bucket, Object: object}
			}
			return nil
		})
		return err
	}

	stmt := conn.Prep("SELECT blob_id, size, etag FROM objects WHERE bucket = ? AND object = ?;")
	stmt.BindText(1, bucket)
	stmt.BindText(2, object)

	var (
		blobID     int64
		size       int64
		actualEtag string
	)
	n, err := sqliteExecResults(stmt, func() error {
		blobID = stmt.ColumnInt64(0)
		size = stmt.ColumnInt64(1)
		actualEtag = stmt.ColumnText(3)
		return nil
	})
	if err != nil {
		return err
	}
	if n == 0 {
		return ObjectNotFound{Bucket: bucket, Object: object}
	}

	if etag != "" && etag != defaultEtag && etag != actualEtag {
		return InvalidETag{}
	}

	if startOffset > size || startOffset+length > size {
		return InvalidRange{
			OffsetBegin:  startOffset,
			OffsetEnd:    length,
			ResourceSize: size,
		}
	}

	var blobs []*sqlite.Blob
	defer func() {
		for _, blob := range blobs {
			blob.Close()
		}
	}()

	stmt = conn.Prep("SELECT rowid FROM blobs WHERE id = ? ORDER BY part_id;")
	stmt.BindInt64(1, blobID)

	_, err = sqliteExecResults(stmt, func() error {
		rowID := stmt.ColumnInt64(0)
		blob, err := conn.OpenBlob("", "blobs", "data", rowID, false)
		if err != nil {
			return err
		}

		blobs = append(blobs, blob)
		return nil
	})
	if err != nil {
		return err
	}

	rdr, err := newBlobReader(bucket, object, blobs, startOffset, length)
	if err != nil {
		return err
	}

	_, err = io.Copy(writer, rdr)
	return err
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

	err = bucketExists(conn, bucket)
	if err != nil {
		return ObjectInfo{}, err
	}

	stmt := conn.Prep("SELECT object, size, modified, etag, content_type, metadata FROM objects WHERE bucket = ? AND instr(object, ?) = 1 ORDER BY object LIMIT 1;")
	stmt.BindText(1, bucket)
	stmt.BindText(2, object)

	var oi ObjectInfo
	n, err := sqliteExecResults(stmt, func() error {
		oi = ObjectInfo{
			Bucket:      bucket,
			Name:        stmt.ColumnText(0),
			Size:        stmt.ColumnInt64(1),
			ModTime:     columnTime(stmt, 2),
			ETag:        stmt.ColumnText(3),
			ContentType: stmt.ColumnText(4),
		}
		return json.NewDecoder(stmt.ColumnReader(5)).Decode(&oi.UserDefined)
	})
	if err != nil {
		return oi, err
	}
	if n == 0 {
		return oi, ObjectNotFound{Bucket: bucket, Object: object}
	}

	if strings.HasSuffix(object, "/") || strings.HasPrefix(strings.TrimPrefix(oi.Name, object), "/") {
		return ObjectInfo{
			Bucket:      bucket,
			Name:        object,
			IsDir:       true,
			ContentType: "application/octet-stream",
		}, nil
	}

	return oi, nil
}

func (s *SQLite) readToTemp(src *PutObjReader, size int64) (_ io.Reader, done func(), err error) {
	if size <= sqliteTempFileThreshold {
		b := make([]byte, size)
		_, err := io.ReadFull(src, b)
		if err != nil {
			if err == io.ErrUnexpectedEOF || err == io.EOF {
				err = IncompleteBody{}
			}
			return nil, nil, err
		}

		// Verify normally happens on EOF, but an EOF isn't
		// returned when reading the exact size.
		err = src.Verify()
		if err != nil {
			return nil, nil, err
		}
		return bytes.NewReader(b), func() {}, nil
	}

	f, err := ioutil.TempFile(s.tempDir, "")
	if err != nil {
		return nil, nil, err
	}

	name := f.Name()
	// Declared as doneFunc instead of assigning to done
	// so that the defer still references the function
	//even even when nil is returned for done.
	doneFunc := func() {
		f.Close()
		os.Remove(name)
	}
	defer func() {
		if err != nil {
			doneFunc()
		}
	}()

	n, err := io.Copy(f, src)
	if err != nil {
		return nil, nil, err
	}
	if n < size {
		return nil, nil, IncompleteBody{}
	}
	_, err = f.Seek(0, 0)
	if err != nil {
		return nil, nil, err
	}

	return f, doneFunc, nil
}

func (s *SQLite) PutObject(ctx context.Context, bucket, object string, data *PutObjReader, opts ObjectOptions) (_ ObjectInfo, err error) {
	size := data.Size()
	err = checkPutObjectArgs(ctx, bucket, object, s, size)
	if err != nil {
		return ObjectInfo{}, err
	}

	mdJSON, err := json.Marshal(opts.UserDefined)
	if err != nil {
		return ObjectInfo{}, err
	}

	src, done, err := s.readToTemp(data, size)
	if err != nil {
		return ObjectInfo{}, err
	}
	defer done()

	etag := data.MD5CurrentHexString()
	contentType := mimedb.TypeByExtension(path.Ext(object))

	s.mu.Lock()
	defer s.mu.Unlock()
	s.writer.SetInterrupt(ctx.Done())

	defer sqlitex.Save(s.writer)(&err)

	stmt := s.writer.Prep("INSERT INTO blobs (id, data) VALUES ((SELECT COALESCE(MAX(id), 0)+1 FROM blobs), ?);")
	stmt.BindZeroBlob(1, size)
	err = sqliteExec(stmt)
	if err != nil {
		return ObjectInfo{}, err
	}

	rowID := s.writer.LastInsertRowID()
	blob, err := s.writer.OpenBlob("", "blobs", "data", rowID, true)
	if err != nil {
		return ObjectInfo{}, sqliteToMinioError(err)
	}

	_, err = io.CopyBuffer(blob, src, s.copyBuffer)
	blob.Close()
	if err != nil {
		return ObjectInfo{}, err
	}

	stmt = s.writer.Prep(`
		INSERT INTO objects (bucket, object, size, metadata, modified, blob_id, etag, content_type)
		VALUES (?, ?, ?, ?, ?, (SELECT id FROM blobs WHERE rowid = ?), ?, ?)
		ON CONFLICT (bucket, object) DO UPDATE SET
			size=excluded.size,
			metadata=excluded.metadata,
			modified=excluded.modified,
			blob_id=excluded.blob_id,
			etag=excluded.etag;`)
	stmt.BindText(1, bucket)
	stmt.BindText(2, object)
	stmt.BindInt64(3, size)
	stmt.BindBytes(4, mdJSON)
	stmt.BindInt64(5, time.Now().UnixNano())
	stmt.BindInt64(6, rowID)
	stmt.BindText(7, etag)
	stmt.BindText(8, contentType)
	err = sqliteExec(stmt)
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
	bucketsSame := isStringEqual(srcBucket, destBucket)
	metadataOnly := bucketsSame && isStringEqual(srcObject, destObject) && srcInfo.metadataOnly

	mdJSON, err := json.Marshal(srcInfo.UserDefined)
	if err != nil {
		return ObjectInfo{}, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.writer.SetInterrupt(ctx.Done())

	err = bucketExists(s.writer, srcBucket)
	if err != nil {
		return ObjectInfo{}, err
	}

	if !bucketsSame {
		err = bucketExists(s.writer, destBucket)
		if err != nil {
			return ObjectInfo{}, err
		}
	}

	if metadataOnly {
		stmt := s.writer.Prep("UPDATE objects SET metadata = ? WHERE bucket = ? AND object ?")
		stmt.BindBytes(1, mdJSON)
		stmt.BindText(2, srcBucket)
		stmt.BindText(3, srcObject)
		err = sqliteExec(stmt)
		if err != nil {
			return ObjectInfo{}, err
		}

		// TODO: properly construct ObjectInfo to return
		return srcInfo, nil
	}

	stmt := s.writer.Prep(`
	INSERT INTO objects(bucket, object, metadata, size, blob_id, modified, etag, content_type)
	SELECT ?, ?, ?, size, blob_id, ?, etag, content_type FROM objects WHERE bucket = ? AND object = ?
	ON CONFLICT (bucket, object)
	DO UPDATE SET
		metadata = EXCLUDED.metadata,
		size = EXCLUDED.size,
		blob_id = EXCLUDED.blob_id,
		modified = EXCLUDED.modified,
		etag = EXCLUDED.etag,
		content_type = EXCLUDED.content_type;`)
	stmt.BindText(1, destBucket)
	stmt.BindText(2, destObject)
	stmt.BindBytes(3, mdJSON)
	stmt.BindInt64(4, time.Now().UnixNano())
	stmt.BindText(5, srcBucket)
	stmt.BindText(6, srcObject)
	err = sqliteExec(stmt)
	if err != nil {
		return ObjectInfo{}, err
	}

	if s.writer.Changes() == 0 {
		return ObjectInfo{}, ObjectNotFound{}
	}

	return srcInfo, nil
}

func (s *SQLite) DeleteObject(ctx context.Context, bucket, object string) (err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.writer.SetInterrupt(ctx.Done())

	err = bucketExists(s.writer, bucket)
	if err != nil {
		return err
	}

	stmt := s.writer.Prep("DELETE FROM objects WHERE bucket = ? AND object = ?;")
	stmt.BindText(1, bucket)
	stmt.BindText(2, object)
	err = sqliteExec(stmt)
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

	var stmt *sqlite.Stmt
	if delimiter == "" {
		stmt = conn.Prep(`
			SELECT id, object as name, initiated FROM uploads
			WHERE
				bucket = ? AND
				instr(object, ?) = 1 AND
				object > ? AND
				id > ?
			ORDER BY object LIMIT ?;`)
		stmt.BindText(1, bucket)
		stmt.BindText(2, prefix)
		stmt.BindText(3, keyMarker)
		stmt.BindText(4, uploadIDMarker)
		stmt.BindInt64(5, int64(maxUploads)+1)
	} else {
		stmt = conn.Prep(`
		SELECT DISTINCT
			id,
			substr(object, 1, length($prefix) + instr(substr(object, length($prefix)+1), $delimiter)) as name,
			0 as initiated,
			1 as is_prefix
		FROM uploads
		WHERE
			bucket = $bucket AND
			instr(object, $prefix) = 1 AND
			object > $key_marker AND
			id > $id_marker AND
			name <> $prefix
		UNION
		SELECT id, object as name, initiated, 0 as is_prefix
		FROM uploads
		WHERE
			bucket = $bucket AND
			instr(object, $prefix) = 1
			AND object > $key_marker
			AND id > $id_marker
			AND instr(substr(object, length($prefix)+1), $delimiter) = 0
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
	_, err = sqliteExecResults(stmt, func() error {
		count++
		if count > maxUploads {
			res.IsTruncated = true
			return nil
		}

		mi := MultipartInfo{
			UploadID:  stmt.ColumnText(0),
			Object:    stmt.ColumnText(1),
			Initiated: columnTime(stmt, 2),
		}
		res.NextKeyMarker = mi.Object
		res.NextUploadIDMarker = mi.UploadID
		if delimiter != "" && stmt.ColumnInt64(3) == 1 {
			res.CommonPrefixes = append(res.CommonPrefixes, mi.Object)
			return nil
		}

		res.Uploads = append(res.Uploads, mi)
		return nil
	})

	if !res.IsTruncated {
		res.NextKeyMarker = ""
		res.NextUploadIDMarker = ""
	}

	return res, err
}

func (s *SQLite) NewMultipartUpload(ctx context.Context, bucket, object string, opts ObjectOptions) (_ string, err error) {
	err = checkNewMultipartArgs(ctx, bucket, object, s)
	if err != nil {
		return "", err
	}

	id := MustGetUUID()
	mdJSON, err := json.Marshal(opts.UserDefined)
	if err != nil {
		return "", err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.writer.SetInterrupt(ctx.Done())

	defer sqlitex.Save(s.writer)(&err)

	// reserve a blob_id
	stmt := s.writer.Prep("INSERT INTO blobs (id, part_id, data) VALUES ((SELECT COALESCE(MAX(id), 0)+1 FROM blobs), -1, 0);")
	err = sqliteExec(stmt)
	if err != nil {
		return "", err
	}
	rowID := s.writer.LastInsertRowID()

	stmt = s.writer.Prep(`
		INSERT INTO uploads (id, bucket, object, metadata, initiated, blob_id)
		VALUES (?, ?, ?, ?, ?, (SELECT id FROM blobs WHERE rowid = ?))`)
	stmt.BindText(1, id)
	stmt.BindText(2, bucket)
	stmt.BindText(3, object)
	stmt.BindBytes(4, mdJSON)
	stmt.BindInt64(5, time.Now().UnixNano())
	stmt.BindInt64(6, rowID)
	err = sqliteExec(stmt)
	if err != nil {
		return "", err
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

	var blobID int64
	n, err := sqliteExecResults(stmt, func() error {
		blobID = stmt.ColumnInt64(0)
		return nil
	})
	if err != nil {
		return -1, err
	}
	if n == 0 {
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

	src, done, err := s.readToTemp(data, size)
	if err != nil {
		return PartInfo{}, err
	}
	defer done()

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
	err = sqliteExec(stmt)
	if err != nil {
		return PartInfo{}, err
	}

	// must query rowid since last insert will be incorrect on upsert
	stmt = s.writer.Prep("SELECT rowid FROM blobs WHERE id = ? AND part_id = ?")
	stmt.BindInt64(1, blobID)
	stmt.BindInt64(2, int64(partID))
	rowID, err := sqlitex.ResultInt64(stmt)
	if err != nil {
		return PartInfo{}, sqliteToMinioError(err)
	}

	blob, err := s.writer.OpenBlob("", "blobs", "data", rowID, true)
	if err != nil {
		return PartInfo{}, sqliteToMinioError(err)
	}
	defer blob.Close()

	_, err = io.CopyBuffer(blob, src, s.copyBuffer)
	if err != nil {
		return PartInfo{}, err
	}

	return PartInfo{PartNumber: partID, ETag: etag}, nil
}

func (s *SQLite) ListObjectParts(ctx context.Context, bucket, object, uploadID string, partNumberMarker int, maxParts int, opts ObjectOptions) (ListPartsInfo, error) {
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
	_, err = sqliteExecResults(stmt, func() error {
		count++
		if count > maxParts {
			res.IsTruncated = true
			return nil
		}

		pi := PartInfo{
			PartNumber: stmt.ColumnInt(0),
			Size:       stmt.ColumnInt64(1),
			ETag:       stmt.ColumnText(2),
		}

		res.NextPartNumberMarker = pi.PartNumber
		res.Parts = append(res.Parts, pi)
		return nil
	})
	if err != nil {
		return res, err
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

	stmt := s.writer.Prep("DELETE FROM uploads WHERE id = ?;")
	stmt.BindText(1, uploadID)
	err = sqliteExec(stmt)
	if err != nil {
		return sqliteToMinioError(err)
	}

	if s.writer.Changes() == 0 {
		return InvalidUploadID{}
	}

	return nil
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

	type partInfo struct {
		etag string
		size int64
	}
	dbPartIDs := make(map[int]partInfo)

	// get existing parts
	stmt := s.writer.Prep("SELECT part_id, etag, length(data) FROM blobs WHERE id = ? ORDER BY part_id;")
	stmt.BindInt64(1, blobID)

	_, err = sqliteExecResults(stmt, func() error {
		id := stmt.ColumnInt(0)
		dbPartIDs[id] = partInfo{
			etag: stmt.ColumnText(1),
			size: stmt.ColumnInt64(2),
		}
		return nil
	})
	if err != nil {
		return ObjectInfo{}, err
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
		err = sqliteExec(stmt)
		if err != nil {
			return ObjectInfo{}, err
		}
	}

	// create object
	stmt = s.writer.Prep(`
		INSERT INTO objects (bucket, object, modified, blob_id, etag, content_type, size, metadata) VALUES (
			?1, ?2, ?3, ?4, ?5, ?6,
			(SELECT SUM(LENGTH(data)) FROM blobs WHERE id = ?4),
			(SELECT metadata FROM uploads WHERE id = ?7)
		)
		ON CONFLICT (bucket, object) DO UPDATE SET
			size=excluded.size,
			metadata=excluded.metadata,
			modified=excluded.modified,
			blob_id=excluded.blob_id;`)
	stmt.BindText(1, bucket)
	stmt.BindText(2, object)
	stmt.BindInt64(3, time.Now().UnixNano())
	stmt.BindInt64(4, blobID)
	stmt.BindText(5, etag)
	stmt.BindText(6, contentType)
	stmt.BindText(7, uploadID)
	err = sqliteExec(stmt)
	if err != nil {
		return ObjectInfo{}, err
	}

	// remove upload
	stmt = s.writer.Prep("DELETE FROM uploads WHERE id = ?;")
	stmt.BindText(1, uploadID)
	err = sqliteExec(stmt)
	if err != nil {
		return ObjectInfo{}, err
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
func (s *SQLite) HealBucket(ctx context.Context, bucket string, dryRun, remove bool) (madmin.HealResultItem, error) {
	return madmin.HealResultItem{}, NotImplemented{}
}
func (s *SQLite) HealObject(ctx context.Context, bucket, object string, dryRun bool, remove bool) (madmin.HealResultItem, error) {
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
	err = sqliteExec(stmt)
	if err != nil {
		return err
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

	stmt := conn.Prep("SELECT policy FROM buckets WHERE bucket = ? AND policy IS NOT NULL;")
	stmt.BindText(1, bucket)

	var p *policy.Policy
	n, err := sqliteExecResults(stmt, func() error {
		p, err = policy.ParseConfig(stmt.ColumnReader(0), bucket)
		return err
	})
	if err != nil {
		return nil, err
	}
	if n == 0 {
		return nil, BucketPolicyNotFound{Bucket: bucket}
	}

	return p, nil
}

func (s *SQLite) DeleteBucketPolicy(ctx context.Context, bucket string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.writer.SetInterrupt(ctx.Done())

	stmt := s.writer.Prep("UPDATE buckets SET policy = NULL WHERE bucket = ?;")
	stmt.BindText(1, bucket)
	return sqliteExec(stmt)
}

// Supported operations check
func (s *SQLite) IsNotificationSupported() bool { return true }
func (s *SQLite) IsListenBucketSupported() bool { return true }
func (s *SQLite) IsEncryptionSupported() bool   { return true }
func (s *SQLite) IsCompressionSupported() bool  { return true }

func bucketExists(conn *sqlite.Conn, bucket string) error {
	stmt := conn.Prep("SELECT 1 FROM buckets WHERE bucket = ?;")
	stmt.BindText(1, bucket)

	n, err := sqliteExecResults(stmt, nil)
	if err != nil {
		return err
	}
	if n == 0 {
		return BucketNotFound{Bucket: bucket}
	}

	return nil
}

func columnTime(stmt *sqlite.Stmt, col int) time.Time {
	ns := stmt.ColumnInt64(col)
	if ns == 0 {
		return time.Time{}
	}
	return time.Unix(0, ns)
}

func sqliteToMinioError(err error) error {
	if err, ok := err.(sqlite.Error); ok {
		switch err.Code {
		case sqlite.SQLITE_FULL:
			return StorageFull{}
		case sqlite.SQLITE_BUSY:
			return OperationTimedOut{}
		}
	}
	return err
}

func sqliteExec(stmt *sqlite.Stmt) error {
	hasRow, err := stmt.Step()
	if err != nil {
		return sqliteToMinioError(err)
	}
	if hasRow {
		return errors.New("sqlite: unexpected row in sqliteExec")
	}
	return nil
}

func sqliteExecResults(stmt *sqlite.Stmt, fn func() error) (int, error) {
	var rows int
	for {
		hasRow, err := stmt.Step()
		if err != nil {
			return rows, sqliteToMinioError(err)
		}
		if !hasRow {
			return rows, nil
		}
		rows++

		if fn == nil {
			continue
		}

		err = fn()
		if err != nil {
			return rows, sqliteToMinioError(err)
		}
	}
}

func newBlobReader(bucket, object string, blobs []*sqlite.Blob, startOffset, length int64) (io.Reader, error) {
	if len(blobs) == 0 {
		return nil, ObjectNotFound{Bucket: bucket, Object: object}
	}

	return &multiBlobReader{
		blobs:       blobs,
		startOffset: startOffset,
		length:      length,
	}, nil
}

type multiBlobReader struct {
	idx         int
	blobs       []*sqlite.Blob
	startOffset int64
	length      int64
}

func (br *multiBlobReader) Read(p []byte) (int, error) {
	for {
		if br.idx >= len(br.blobs) || br.length == 0 {
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
