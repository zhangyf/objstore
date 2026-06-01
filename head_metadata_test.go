package objstore

import "testing"

// TestObjectInfoMetadataFields 仅做静态检查：确保扩展的字段存在、类型正确。
func TestObjectInfoMetadataFields(t *testing.T) {
	o := ObjectInfo{
		Key:                  "k",
		Size:                 123,
		ETag:                 "etag",
		StorageClass:         "STANDARD",
		ContentType:          "text/plain",
		ServerSideEncryption: "AES256",
		SSEKMSKeyID:          "alias/myKey",
		VersionID:            "v1",
		Metadata:             map[string]string{"author": "zhangyf"},
	}
	if o.ContentType != "text/plain" {
		t.Fatal("ContentType not set")
	}
	if o.ServerSideEncryption != "AES256" {
		t.Fatal("SSE not set")
	}
	if o.SSEKMSKeyID != "alias/myKey" {
		t.Fatal("KMS key not set")
	}
	if o.VersionID != "v1" {
		t.Fatal("VersionID not set")
	}
	if o.Metadata["author"] != "zhangyf" {
		t.Fatal("Metadata not set")
	}
}
