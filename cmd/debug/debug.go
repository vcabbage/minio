package debug

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"runtime"
	"sync/atomic"
	"time"

	minio "github.com/minio/minio/cmd"
	"github.com/minio/minio/pkg/madmin"
	"github.com/minio/minio/pkg/policy"
)

type DebugLayer struct {
	id uint64

	Wrapped    minio.ObjectLayer
	LogReturns bool
	LogTiming  bool
	LogCallers int
}

func (l *DebugLayer) tracef(format string, vals ...interface{}) func(...interface{}) {
	var buf bytes.Buffer

	start := time.Now()
	id := fmt.Sprintf("%d: ", atomic.AddUint64(&l.id, 1))

	buf.WriteString(id)

	l.flogf(&buf, format, vals...)

	const debugCallers = 1
	for i := 1; i <= l.LogCallers; i++ {
		_, file, line, ok := runtime.Caller(i + debugCallers)
		if !ok {
			continue
		}
		fmt.Fprintf(&buf, "  %s:%d\n", file, line)
	}

	fmt.Printf("%s", buf.Bytes())

	if !l.LogReturns {
		return func(...interface{}) {}
	}

	return func(returnVals ...interface{}) {
		for i, val := range returnVals {
			val = reflect.ValueOf(val).Elem().Interface()

			returnVals[i] = fmt.Sprintf("%+v", val)
		}
		if l.LogTiming {
			returnVals = append(returnVals, fmt.Sprintf("[%s]", time.Since(start)))
		}
		fmt.Println(append([]interface{}{id + "  "}, returnVals...)...)
	}
}

func (l *DebugLayer) flogf(w io.Writer, format string, vals ...interface{}) {
	for i, val := range vals {
		switch val := val.(type) {
		case context.Context:
			s := "ctx"

			deadline, ok := val.Deadline()
			if ok {
				s += fmt.Sprintf("[deadline:%s]", time.Until(deadline))
			}

			select {
			case <-val.Done():
				s += "[done]"
			default:
			}

			vals[i] = s
		}
	}

	fmt.Fprintf(w, format, vals...)
}

func (l *DebugLayer) Shutdown(ctx context.Context) (err error) {
	defer l.tracef("Shutdown(%v)\n", ctx)(&err)
	return l.Wrapped.Shutdown(ctx)
}

func (l *DebugLayer) StorageInfo(ctx context.Context) (res minio.StorageInfo) {
	defer l.tracef("StorageInfo(%v)\n", ctx)(&res)
	return l.Wrapped.StorageInfo(ctx)
}

func (l *DebugLayer) MakeBucketWithLocation(ctx context.Context, bucket string, location string) (err error) {
	defer l.tracef("MakeBucketWithLocation(%v, %q, %q)\n", ctx, bucket, location)(&err)
	return l.Wrapped.MakeBucketWithLocation(ctx, bucket, location)
}

func (l *DebugLayer) GetBucketInfo(ctx context.Context, bucket string) (res minio.BucketInfo, err error) {
	defer l.tracef("GetBucketInfo(%v, %q)\n", ctx, bucket)(&res, &err)
	return l.Wrapped.GetBucketInfo(ctx, bucket)
}
func (l *DebugLayer) ListBuckets(ctx context.Context) (res []minio.BucketInfo, err error) {
	defer l.tracef("ListBuckets(%v)\n", ctx)(&res, &err)
	return l.Wrapped.ListBuckets(ctx)
}
func (l *DebugLayer) DeleteBucket(ctx context.Context, bucket string) (err error) {
	defer l.tracef("DeleteBucket(%v, %q)\n", ctx, bucket)
	return l.Wrapped.DeleteBucket(ctx, bucket)
}
func (l *DebugLayer) ListObjects(ctx context.Context, bucket, prefix, marker, delimiter string, maxKeys int) (res minio.ListObjectsInfo, err error) {
	defer l.tracef("ListObjects(%v, %q, %q, %q, %q, %d)\n", ctx, bucket, prefix, marker, delimiter, maxKeys)(&res, &err)
	return l.Wrapped.ListObjects(ctx, bucket, prefix, marker, delimiter, maxKeys)
}
func (l *DebugLayer) ListObjectsV2(ctx context.Context, bucket, prefix, continuationToken, delimiter string, maxKeys int, fetchOwner bool, startAfter string) (res minio.ListObjectsV2Info, err error) {
	defer l.tracef("ListObjectsV2(%v, %q, %q, %q, %q, %d, %t, %q)\n", ctx, bucket, prefix, continuationToken, delimiter, maxKeys, fetchOwner, startAfter)(&res, &err)
	return l.Wrapped.ListObjectsV2(ctx, bucket, prefix, continuationToken, delimiter, maxKeys, fetchOwner, startAfter)
}

// Object operations.
func (l *DebugLayer) GetObjectNInfo(ctx context.Context, bucket, object string, rs *minio.HTTPRangeSpec, h http.Header, lockType minio.LockType, opts minio.ObjectOptions) (res *minio.GetObjectReader, err error) {
	defer l.tracef("GetObjectNInfo(%v, %q, %q, %v, %v, %v, %v)\n", ctx, bucket, object, rs, h, lockType, opts)(&res, &err)
	return l.Wrapped.GetObjectNInfo(ctx, bucket, object, rs, h, lockType, opts)
}
func (l *DebugLayer) GetObject(ctx context.Context, bucket, object string, startOffset, length int64, writer io.Writer, etag string, opts minio.ObjectOptions) (err error) {
	defer l.tracef("GetObject(%v, %q, %q, %d, %d, writer, %q, %v)\n", ctx, bucket, object, startOffset, length, etag, opts)(&err)
	return l.Wrapped.GetObject(ctx, bucket, object, startOffset, length, writer, etag, opts)
}
func (l *DebugLayer) GetObjectInfo(ctx context.Context, bucket, object string, opts minio.ObjectOptions) (res minio.ObjectInfo, err error) {
	defer l.tracef("GetObjectInfo(%v, %q, %q, %v)\n", ctx, bucket, object, opts)(&res, &err)
	return l.Wrapped.GetObjectInfo(ctx, bucket, object, opts)
}
func (l *DebugLayer) PutObject(ctx context.Context, bucket, object string, data *minio.PutObjReader, metadata map[string]string, opts minio.ObjectOptions) (res minio.ObjectInfo, err error) {
	defer l.tracef("PutObject(%v, %q, %q, data, %v, %v)\n", ctx, bucket, object, metadata, opts)(&res, &err)
	return l.Wrapped.PutObject(ctx, bucket, object, data, metadata, opts)
}
func (l *DebugLayer) CopyObject(ctx context.Context, srcBucket, srcObject, destBucket, destObject string, srcInfo minio.ObjectInfo, srcOpts, dstOpts minio.ObjectOptions) (res minio.ObjectInfo, err error) {
	defer l.tracef("CopyObject(%v, %q, %q, %q, %q, %v, %v, %v)\n", ctx, srcBucket, srcObject, destBucket, destObject, srcInfo, srcOpts, dstOpts)(&res, &err)
	return l.Wrapped.CopyObject(ctx, srcBucket, srcObject, destBucket, destObject, srcInfo, srcOpts, dstOpts)
}
func (l *DebugLayer) DeleteObject(ctx context.Context, bucket, object string) (err error) {
	defer l.tracef("DeleteObject(%v, %q, %q)\n", ctx, bucket, object)(&err)
	return l.Wrapped.DeleteObject(ctx, bucket, object)
}

// Multipart operations.
func (l *DebugLayer) ListMultipartUploads(ctx context.Context, bucket, prefix, keyMarker, uploadIDMarker, delimiter string, maxUploads int) (res minio.ListMultipartsInfo, err error) {
	defer l.tracef("ListMultipartUploads(%v, %q, %q, %q, %q, %q, %d)\n", ctx, bucket, prefix, keyMarker, uploadIDMarker, delimiter, maxUploads)(&res, &err)
	return l.Wrapped.ListMultipartUploads(ctx, bucket, prefix, keyMarker, uploadIDMarker, delimiter, maxUploads)
}
func (l *DebugLayer) NewMultipartUpload(ctx context.Context, bucket, object string, metadata map[string]string, opts minio.ObjectOptions) (res string, err error) {
	defer l.tracef("NewMultipartUpload(%v, %q, %q, %v, %v)\n", ctx, bucket, object, metadata, opts)(&res, &err)
	return l.Wrapped.NewMultipartUpload(ctx, bucket, object, metadata, opts)
}
func (l *DebugLayer) CopyObjectPart(ctx context.Context, srcBucket, srcObject, destBucket, destObject string, uploadID string, partID int, startOffset, length int64, srcInfo minio.ObjectInfo, srcOpts, dstOpts minio.ObjectOptions) (res minio.PartInfo, err error) {
	defer l.tracef("CopyObjectPart(%v, %q, %q, %q, %q, %d, %d, %d, %v, %v, %v)", srcBucket, srcObject, destBucket, destObject, uploadID, partID, startOffset, length, srcInfo, srcOpts, dstOpts)(&res, &err)
	return l.Wrapped.CopyObjectPart(ctx, srcBucket, srcObject, destBucket, destObject, uploadID, partID, startOffset, length, srcInfo, srcOpts, dstOpts)
}
func (l *DebugLayer) PutObjectPart(ctx context.Context, bucket, object, uploadID string, partID int, data *minio.PutObjReader, opts minio.ObjectOptions) (res minio.PartInfo, err error) {
	defer l.tracef("PutObjectPart(%v, %q, %q, %q, %d, data, %v)\n", ctx, bucket, object, uploadID, partID, opts)(&res, &err)
	return l.Wrapped.PutObjectPart(ctx, bucket, object, uploadID, partID, data, opts)
}
func (l *DebugLayer) ListObjectParts(ctx context.Context, bucket, object, uploadID string, partNumberMarker, maxParts int) (res minio.ListPartsInfo, err error) {
	defer l.tracef("ListObjectParts(%v, %q, %q, %q, %d, %d)\n", ctx, bucket, object, uploadID, partNumberMarker, maxParts)(&res, &err)
	return l.Wrapped.ListObjectParts(ctx, bucket, object, uploadID, partNumberMarker, maxParts)
}
func (l *DebugLayer) AbortMultipartUpload(ctx context.Context, bucket, object, uploadID string) (err error) {
	defer l.tracef("AbortMultipartUpload(%v, %q, %q, %q)\n", ctx, bucket, object, uploadID)(&err)
	return l.Wrapped.AbortMultipartUpload(ctx, bucket, object, uploadID)
}
func (l *DebugLayer) CompleteMultipartUpload(ctx context.Context, bucket, object, uploadID string, uploadedParts []minio.CompletePart, opts minio.ObjectOptions) (res minio.ObjectInfo, err error) {
	defer l.tracef("CompleteMultipartUpload(%v, %q, %q, %q, %v, %v)\n", ctx, bucket, object, uploadID, uploadedParts, opts)(&res, &err)
	return l.Wrapped.CompleteMultipartUpload(ctx, bucket, object, uploadID, uploadedParts, opts)
}

// Healing operations.
func (l *DebugLayer) ReloadFormat(ctx context.Context, dryRun bool) (err error) {
	defer l.tracef("ReloadFormat(%v, %t)\n", ctx, dryRun)(&err)
	return l.Wrapped.ReloadFormat(ctx, dryRun)
}
func (l *DebugLayer) HealFormat(ctx context.Context, dryRun bool) (res madmin.HealResultItem, err error) {
	defer l.tracef("HealFormat(%v, %t)\n", ctx, dryRun)(&res, &err)
	return l.Wrapped.HealFormat(ctx, dryRun)
}
func (l *DebugLayer) HealBucket(ctx context.Context, bucket string, dryRun bool) (res []madmin.HealResultItem, err error) {
	defer l.tracef("HealBucket(%v, %q, %t)\n", ctx, bucket, dryRun)(&res, &err)
	return l.Wrapped.HealBucket(ctx, bucket, dryRun)
}
func (l *DebugLayer) HealObject(ctx context.Context, bucket, object string, dryRun bool) (res madmin.HealResultItem, err error) {
	defer l.tracef("HealObject(%v, %q, %q, %t)\n", ctx, bucket, object, dryRun)(&res, &err)
	return l.Wrapped.HealObject(ctx, bucket, object, dryRun)
}
func (l *DebugLayer) ListBucketsHeal(ctx context.Context) (res []minio.BucketInfo, err error) {
	defer l.tracef("ListBucketsHeal(%v)\n", ctx)(&res, &err)
	return l.Wrapped.ListBucketsHeal(ctx)
}
func (l *DebugLayer) ListObjectsHeal(ctx context.Context, bucket, prefix, marker, delimiter string, maxKeys int) (res minio.ListObjectsInfo, err error) {
	defer l.tracef("ListObjectsHeal(%v, %q, %q, %q, %q, %d)\n", ctx, bucket, prefix, marker, delimiter, maxKeys)(&res, &err)
	return l.Wrapped.ListObjectsHeal(ctx, bucket, prefix, marker, delimiter, maxKeys)
}

// Policy operations
func (l *DebugLayer) SetBucketPolicy(ctx context.Context, bucket string, policy *policy.Policy) (err error) {
	defer l.tracef("SetBucketPolicy(%v, %q, %v)\n", ctx, bucket, policy)(&err)
	return l.Wrapped.SetBucketPolicy(ctx, bucket, policy)
}

func (l *DebugLayer) GetBucketPolicy(ctx context.Context, bucket string) (res *policy.Policy, err error) {
	defer l.tracef("GetBucketPolicy(%v, %q)\n", ctx, bucket)(&res, &err)
	return l.Wrapped.GetBucketPolicy(ctx, bucket)
}

func (l *DebugLayer) DeleteBucketPolicy(ctx context.Context, bucket string) (err error) {
	defer l.tracef("DeleteBucketPolicy(%v, %q)\n", ctx, bucket)(&err)
	return l.Wrapped.DeleteBucketPolicy(ctx, bucket)
}

// Supported operations check
func (l *DebugLayer) IsNotificationSupported() (res bool) {
	defer l.tracef("IsNotificationSupported()\n")(&res)
	return l.Wrapped.IsNotificationSupported()
}
func (l *DebugLayer) IsEncryptionSupported() (res bool) {
	defer l.tracef("IsEncryptionSupported()\n")(&res)
	return l.Wrapped.IsEncryptionSupported()
}
func (l *DebugLayer) IsCompressionSupported() (res bool) {
	defer l.tracef("IsCompressionSupported()\n")(&res)
	return l.Wrapped.IsCompressionSupported()
}
