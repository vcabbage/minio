package debug

import (
	"context"
	"fmt"
	"io"
	"net/http"

	minio "github.com/minio/minio/cmd"
	"github.com/minio/minio/pkg/madmin"
	"github.com/minio/minio/pkg/policy"
)

type DebugLayer struct {
	Wrapped minio.ObjectLayer
}

func (l DebugLayer) Shutdown(ctx context.Context) error {
	fmt.Println("Shutdown(ctx)")
	return l.Wrapped.Shutdown(ctx)
}

func (l DebugLayer) StorageInfo(ctx context.Context) minio.StorageInfo {
	fmt.Println("StorageInfo(ctx)")
	return l.Wrapped.StorageInfo(ctx)
}

func (l DebugLayer) MakeBucketWithLocation(ctx context.Context, bucket string, location string) error {
	fmt.Printf("MakeBucketWithLocation(ctx, %q, %q)\n", bucket, location)
	return l.Wrapped.MakeBucketWithLocation(ctx, bucket, location)
}

func (l DebugLayer) GetBucketInfo(ctx context.Context, bucket string) (minio.BucketInfo, error) {
	fmt.Printf("GetBucketInfo(ctx, %q)\n", bucket)
	return l.Wrapped.GetBucketInfo(ctx, bucket)
}
func (l DebugLayer) ListBuckets(ctx context.Context) ([]minio.BucketInfo, error) {
	fmt.Printf("ListBuckets(ctx)\n")
	return l.Wrapped.ListBuckets(ctx)
}
func (l DebugLayer) DeleteBucket(ctx context.Context, bucket string) (err error) {
	fmt.Printf("DeleteBucket(ctx, %q)\n", bucket)
	return l.Wrapped.DeleteBucket(ctx, bucket)
}
func (l DebugLayer) ListObjects(ctx context.Context, bucket, prefix, marker, delimiter string, maxKeys int) (minio.ListObjectsInfo, error) {
	fmt.Printf("ListObjects(ctx, %q, %q, %q, %q, %d)\n", bucket, prefix, marker, delimiter, maxKeys)
	return l.Wrapped.ListObjects(ctx, bucket, prefix, marker, delimiter, maxKeys)
}
func (l DebugLayer) ListObjectsV2(ctx context.Context, bucket, prefix, continuationToken, delimiter string, maxKeys int, fetchOwner bool, startAfter string) (minio.ListObjectsV2Info, error) {
	fmt.Printf("ListObjectsV2(ctx, %q, %q, %q, %q, %d, %t, %q)\n", bucket, prefix, continuationToken, delimiter, maxKeys, fetchOwner, startAfter)
	return l.Wrapped.ListObjectsV2(ctx, bucket, prefix, continuationToken, delimiter, maxKeys, fetchOwner, startAfter)
}

// Object operations.
func (l DebugLayer) GetObjectNInfo(ctx context.Context, bucket, object string, rs *minio.HTTPRangeSpec, h http.Header, lockType minio.LockType, opts minio.ObjectOptions) (*minio.GetObjectReader, error) {
	fmt.Printf("GetObjectNInfo(ctx, %q, %q, %v, %v, %v, %v)\n", bucket, object, rs, h, lockType, opts)
	return l.Wrapped.GetObjectNInfo(ctx, bucket, object, rs, h, lockType, opts)
}
func (l DebugLayer) GetObject(ctx context.Context, bucket, object string, startOffset, length int64, writer io.Writer, etag string, opts minio.ObjectOptions) error {
	fmt.Printf("GetObject(ctx, %q, %q, %d, %d, writer, %q, %v)\n", bucket, object, startOffset, length, etag, opts)
	return l.Wrapped.GetObject(ctx, bucket, object, startOffset, length, writer, etag, opts)
}
func (l DebugLayer) GetObjectInfo(ctx context.Context, bucket, object string, opts minio.ObjectOptions) (minio.ObjectInfo, error) {
	fmt.Printf("GetObjectInfo(ctx, %q, %q, %v)\n", bucket, object, opts)
	return l.Wrapped.GetObjectInfo(ctx, bucket, object, opts)
}
func (l DebugLayer) PutObject(ctx context.Context, bucket, object string, data *minio.PutObjReader, metadata map[string]string, opts minio.ObjectOptions) (_ minio.ObjectInfo, err error) {
	fmt.Printf("PutObject(ctx, %q, %q, data, %v, %v)\n", bucket, object, metadata, opts)
	return l.Wrapped.PutObject(ctx, bucket, object, data, metadata, opts)
}
func (l DebugLayer) CopyObject(ctx context.Context, srcBucket, srcObject, destBucket, destObject string, srcInfo minio.ObjectInfo, srcOpts, dstOpts minio.ObjectOptions) (minio.ObjectInfo, error) {
	fmt.Printf("CopyObject(ctx, %q, %q, %q, %q, %v, %v, %v)\n", srcBucket, srcObject, destObject, srcInfo, srcOpts, dstOpts)
	return l.Wrapped.CopyObject(ctx, srcBucket, srcObject, destBucket, destObject, srcInfo, srcOpts, dstOpts)
}
func (l DebugLayer) DeleteObject(ctx context.Context, bucket, object string) error {
	fmt.Printf("DeleteObject(ctx, %q, %q)\n", bucket, object)
	return l.Wrapped.DeleteObject(ctx, bucket, object)
}

// Multipart operations.
func (l DebugLayer) ListMultipartUploads(ctx context.Context, bucket, prefix, keyMarker, uploadIDMarker, delimiter string, maxUploads int) (minio.ListMultipartsInfo, error) {
	fmt.Printf("ListMultipartUploads(ctx, %q, %q, %q, %q, %q, %d)\n", bucket, prefix, keyMarker, uploadIDMarker, delimiter, maxUploads)
	return l.Wrapped.ListMultipartUploads(ctx, bucket, prefix, keyMarker, uploadIDMarker, delimiter, maxUploads)
}
func (l DebugLayer) NewMultipartUpload(ctx context.Context, bucket, object string, metadata map[string]string, opts minio.ObjectOptions) (string, error) {
	fmt.Printf("NewMultipartUpload(ctx, %q, %q, %v, %v)\n", bucket, object, metadata, opts)
	return l.Wrapped.NewMultipartUpload(ctx, bucket, object, metadata, opts)
}
func (l DebugLayer) CopyObjectPart(ctx context.Context, srcBucket, srcObject, destBucket, destObject string, uploadID string, partID int, startOffset, length int64, srcInfo minio.ObjectInfo, srcOpts, dstOpts minio.ObjectOptions) (minio.PartInfo, error) {
	fmt.Println("CopyObjectPart(ctx, %q, %q, %q, %q, %d, %d, %d, %v, %v, %v)", srcBucket, srcObject, destBucket, destObject, uploadID, partID, startOffset, length, srcInfo, srcOpts, dstOpts)
	return l.Wrapped.CopyObjectPart(ctx, srcBucket, srcObject, destBucket, destObject, uploadID, partID, startOffset, length, srcInfo, srcOpts, dstOpts)
}
func (l DebugLayer) PutObjectPart(ctx context.Context, bucket, object, uploadID string, partID int, data *minio.PutObjReader, opts minio.ObjectOptions) (minio.PartInfo, error) {
	fmt.Printf("PutObjectPart(ctx, %q, %q, %q, %d, data, %v)\n", bucket, object, uploadID, partID, opts)
	return l.Wrapped.PutObjectPart(ctx, bucket, object, uploadID, partID, data, opts)
}
func (l DebugLayer) ListObjectParts(ctx context.Context, bucket, object, uploadID string, partNumberMarker, maxParts int) (minio.ListPartsInfo, error) {
	fmt.Printf("ListObjectParts(ctx, %q, %q, %q, %d, %d)\n", bucket, object, uploadID, partNumberMarker, maxParts)
	return l.Wrapped.ListObjectParts(ctx, bucket, object, uploadID, partNumberMarker, maxParts)
}
func (l DebugLayer) AbortMultipartUpload(ctx context.Context, bucket, object, uploadID string) error {
	fmt.Printf("AbortMultipartUpload(ctx, %q, %q, %q)\n", bucket, object, uploadID)
	return l.Wrapped.AbortMultipartUpload(ctx, bucket, object, uploadID)
}
func (l DebugLayer) CompleteMultipartUpload(ctx context.Context, bucket, object, uploadID string, uploadedParts []minio.CompletePart, opts minio.ObjectOptions) (_ minio.ObjectInfo, err error) {
	fmt.Printf("CompleteMultipartUpload(ctx, %q, %q, %q, %v, %v)\n", bucket, object, uploadID, uploadedParts, opts)
	return l.CompleteMultipartUpload(ctx, bucket, object, uploadID, uploadedParts, opts)
}

// Healing operations.
func (l DebugLayer) ReloadFormat(ctx context.Context, dryRun bool) error {
	fmt.Printf("ReloadFormat(c\ntx, %t)\n")
	return l.Wrapped.ReloadFormat(ctx, dryRun)
}
func (l DebugLayer) HealFormat(ctx context.Context, dryRun bool) (madmin.HealResultItem, error) {
	fmt.Printf("HealFormat(ctx, %t)\n", dryRun)
	return l.Wrapped.HealFormat(ctx, dryRun)
}
func (l DebugLayer) HealBucket(ctx context.Context, bucket string, dryRun bool) ([]madmin.HealResultItem, error) {
	fmt.Printf("HealBucket(ctx, %q, %t)\n", bucket, dryRun)
	return l.Wrapped.HealBucket(ctx, bucket, dryRun)
}
func (l DebugLayer) HealObject(ctx context.Context, bucket, object string, dryRun bool) (madmin.HealResultItem, error) {
	fmt.Printf("HealObject(ctx, %q, %q, %t)\n", bucket, object, dryRun)
	return l.Wrapped.HealObject(ctx, bucket, object, dryRun)
}
func (l DebugLayer) ListBucketsHeal(ctx context.Context) ([]minio.BucketInfo, error) {
	fmt.Printf("ListBucketsHeal(ctx)\n")
	return l.Wrapped.ListBucketsHeal(ctx)
}
func (l DebugLayer) ListObjectsHeal(ctx context.Context, bucket, prefix, marker, delimiter string, maxKeys int) (minio.ListObjectsInfo, error) {
	fmt.Printf("ListObjectsHeal(ctx, %q, %q, %q, %q, %d)\n", bucket, prefix, marker, delimiter, maxKeys)
	return l.Wrapped.ListObjectsHeal(ctx, bucket, prefix, marker, delimiter, maxKeys)
}

// Policy operations
func (l DebugLayer) SetBucketPolicy(ctx context.Context, bucket string, policy *policy.Policy) error {
	fmt.Printf("SetBucketPolicy(ctx, %q, %v)\n", bucket, policy)
	return l.Wrapped.SetBucketPolicy(ctx, bucket, policy)
}

func (l DebugLayer) GetBucketPolicy(ctx context.Context, bucket string) (*policy.Policy, error) {
	fmt.Printf("GetBucketPolicy(ctx, %q)\n", bucket)
	return l.Wrapped.GetBucketPolicy(ctx, bucket)
}

func (l DebugLayer) DeleteBucketPolicy(ctx context.Context, bucket string) error {
	fmt.Printf("DeleteBucketPolicy(ctx, %q)\n", bucket)
	return l.Wrapped.DeleteBucketPolicy(ctx, bucket)
}

// Supported operations check
func (l DebugLayer) IsNotificationSupported() bool {
	fmt.Println("IsNotificationSupported()\n")
	return l.Wrapped.IsNotificationSupported()
}
func (l DebugLayer) IsEncryptionSupported() bool {
	fmt.Println("IsEncryptionSupported()\n")
	return l.Wrapped.IsEncryptionSupported()
}
func (l DebugLayer) IsCompressionSupported() bool {
	fmt.Println("IsCompressionSupported()\n")
	return l.Wrapped.IsCompressionSupported()
}
