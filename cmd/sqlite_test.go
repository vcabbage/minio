package cmd

import (
	"context"
	"net/url"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"

	"crawshaw.io/sqlite"
	"crawshaw.io/sqlite/sqlitex"
)

func TestSQLiteLayer(t *testing.T) {
	testFuncs := []objTestType{
		testBucketRecreateFails,
		testContentType,
		testDeleteObject,
		testGetDirectoryReturnsObjectNotFound,
		testGetObject,
		testGetObjectInfo,
		testListBuckets,
		testListBucketsOrder,
		testListMultipartUploads,
		testListObjects,
		testListObjectsTestsForNonExistantBucket,
		testListObjectParts,
		testMakeBucket,
		testMultipartObjectAbort,
		testMultipartObjectCreation,
		testNonExistantBucketOperations,
		testNonExistantObjectInBucket,
		testObjectAbortMultipartUpload,
		testObjectAPIIsUploadIDExists,
		testObjectAPIPutObject,
		testObjectAPIPutObjectPart,
		testObjectCompleteMultipartUpload,
		testObjectNewMultipartUpload,
		testObjectOverwriteWorks,
		testPaging,
		testParseStorageClass,
		testPutObject,
		testPutObjectInSubdir,
	}

	for _, tf := range testFuncs {
		runLayerTest(t, tf)
	}
}

func runLayerTest(t *testing.T, testFunc objTestType) {
	t.Run(nameForTestFunc(testFunc), func(t *testing.T) {
		db := newTestDB(t)
		defer db.teardown()

		testFunc(db.SQLite, "SQLite", t)
	})
}

func nameForTestFunc(testFunc objTestType) string {
	name := runtime.FuncForPC(reflect.ValueOf(testFunc).Pointer()).Name()
	if i := strings.LastIndexByte(name, '.'); i >= 0 {
		name = name[i+1:]
	}

	return strings.TrimPrefix(name, "test")
}

type testDB struct {
	*SQLite
	t *testing.T
}

func (db testDB) exec(query string, expected int, f func(*sqlite.Stmt)) {
	conn, err := db.getConn(context.Background())
	testCheckErr(db.t, err)
	defer db.readers.Put(conn)

	var count int
	err = sqlitex.Exec(conn, query, func(stmt *sqlite.Stmt) error {
		count++
		f(stmt)
		return nil
	})
	testCheckErr(db.t, err)

	switch {
	case expected == -1 && count > 0:
	case expected == -1 && count == 0:
		db.t.Errorf("expected some results but got none")
	case expected != count:
		db.t.Errorf("expected %d results, got %d", expected, count)
	}
}

func (db testDB) teardown() {
	checkDB(db.t, db.SQLite)
	err := db.Shutdown(context.Background())
	testCheckErr(db.t, err)
}

func timeDiff(a, b time.Time) time.Duration {
	diff := a.Sub(b)
	if diff < 0 {
		return -diff
	}
	return diff
}

func newTestDB(t *testing.T) testDB {
	u, err := url.Parse("file::memory:?cache=shared")
	// u, err := url.Parse("file:../listtest.db")
	testCheckErr(t, err)

	o, err := NewSQLiteLayer(u)
	testCheckErr(t, err)

	return testDB{SQLite: o.(*SQLite), t: t}
}

// checkDB looks for invariant violations. No combination of operations
// via the public API should be able to make these fail.
func checkDB(t *testing.T, db *SQLite) {
	conn, err := db.getConn(context.Background())
	testCheckErr(t, err)
	defer db.readers.Put(conn)

	// NOTE: Some tests are redundant with DB constraints
	//       but are checked anyway in case the schema is changed.

	// All objects reference a bucket
	q := `
		SELECT bucket, object FROM objects
		WHERE bucket NOT IN (SELECT bucket FROM buckets)`
	err = sqlitex.Exec(conn, q, func(stmt *sqlite.Stmt) error {
		bucket := stmt.ColumnText(0)
		object := stmt.ColumnText(1)
		t.Errorf("Found orphaned object: %s - %s", bucket, object)
		return nil
	})
	testCheckErr(t, err)

	// All objects reference a blob
	q = `
		SELECT bucket, object, blob_id FROM objects
		WHERE blob_id NOT IN (SELECT id FROM blobs)`
	err = sqlitex.Exec(conn, q, func(stmt *sqlite.Stmt) error {
		bucket := stmt.ColumnText(0)
		object := stmt.ColumnText(1)
		blobID := stmt.ColumnInt64(2)
		t.Errorf("Found object with dangling blob reference: %s - %s - %d", bucket, object, blobID)
		return nil
	})
	testCheckErr(t, err)

	// All uploads reference a blob
	q = `
		SELECT bucket, object, blob_id FROM uploads
		WHERE blob_id NOT IN (SELECT id FROM blobs)`
	err = sqlitex.Exec(conn, q, func(stmt *sqlite.Stmt) error {
		bucket := stmt.ColumnText(0)
		object := stmt.ColumnText(1)
		blobID := stmt.ColumnInt64(2)
		t.Errorf("Found upload with dangling blob reference: %s - %s - %d", bucket, object, blobID)
		return nil
	})
	testCheckErr(t, err)

	// All blobs are referenced
	q = `
		SELECT id FROM blobs
		WHERE
			id NOT IN (SELECT blob_id FROM objects) AND
			id NOT IN (SELECT blob_id FROM uploads)`
	err = sqlitex.Exec(conn, q, func(stmt *sqlite.Stmt) error {
		id := stmt.ColumnInt64(0)
		t.Errorf("Found blob with no references: %d", id)
		return nil
	})
	testCheckErr(t, err)
}

func testCheckErr(t *testing.T, err error) {
	if err != nil {
		t.Fatal(err)
	}
}
