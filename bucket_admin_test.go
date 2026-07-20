package objstore

import (
	"errors"
	"testing"
)

func TestBucketAdminInterface_Coverage(t *testing.T) {
	// 编译期校验：cosStore / s3Store 都实现 BucketAdmin
	var _ BucketAdmin = (*cosStore)(nil)
	var _ BucketAdmin = (*s3Store)(nil)
	// 编译期校验：cosStore / s3Store 都实现 BucketAdminOpt
	var _ BucketAdminOpt = (*cosStore)(nil)
	var _ BucketAdminOpt = (*s3Store)(nil)
}

func TestBucketSentinelErrors(t *testing.T) {
	if !errors.Is(ErrBucketNotFound, ErrBucketNotFound) {
		t.Fatal("ErrBucketNotFound sentinel mismatch")
	}
	if !errors.Is(ErrBucketAlreadyOwnedByYou, ErrBucketAlreadyOwnedByYou) {
		t.Fatal("ErrBucketAlreadyOwnedByYou sentinel mismatch")
	}
	if errors.Is(ErrBucketNotFound, ErrBucketAlreadyOwnedByYou) {
		t.Fatal("two sentinels should differ")
	}
}
