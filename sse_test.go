package objstore

import (
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func TestPutOptions_HasAny_SSE(t *testing.T) {
	if (&PutOptions{}).HasAny() {
		t.Fatal("empty PutOptions should not HasAny")
	}
	if !(&PutOptions{SSE: "AES256"}).HasAny() {
		t.Fatal("SSE=AES256 should HasAny")
	}
	if !(&PutOptions{SSEKMSKeyID: "k"}).HasAny() {
		t.Fatal("SSEKMSKeyID alone should HasAny")
	}
}

func TestApplyS3PutOptions_SSE(t *testing.T) {
	cases := []struct {
		name      string
		opts      *PutOptions
		wantSSE   s3types.ServerSideEncryption
		wantKeyID string
	}{
		{"none", &PutOptions{}, "", ""},
		{"sse-s3 (AES256)", &PutOptions{SSE: "AES256"}, s3types.ServerSideEncryptionAes256, ""},
		{"sse-kms with key", &PutOptions{SSE: "aws:kms", SSEKMSKeyID: "alias/myKey"}, s3types.ServerSideEncryptionAwsKms, "alias/myKey"},
		{"sse-kms no key (use account default)", &PutOptions{SSE: "aws:kms"}, s3types.ServerSideEncryptionAwsKms, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			input := &s3.PutObjectInput{
				Bucket: aws.String("b"),
				Key:    aws.String("k"),
			}
			applyS3PutOptions(input, tc.opts)
			if input.ServerSideEncryption != tc.wantSSE {
				t.Fatalf("ServerSideEncryption=%q want %q", input.ServerSideEncryption, tc.wantSSE)
			}
			gotKey := ""
			if input.SSEKMSKeyId != nil {
				gotKey = *input.SSEKMSKeyId
			}
			if gotKey != tc.wantKeyID {
				t.Fatalf("SSEKMSKeyId=%q want %q", gotKey, tc.wantKeyID)
			}
		})
	}
}

func TestApplyS3CreateMultipart_SSE(t *testing.T) {
	input := &s3.CreateMultipartUploadInput{
		Bucket: aws.String("b"),
		Key:    aws.String("k"),
	}
	applyS3CreateMultipart(input, &PutOptions{SSE: "aws:kms", SSEKMSKeyID: "abc"})
	if input.ServerSideEncryption != s3types.ServerSideEncryptionAwsKms {
		t.Fatalf("multipart SSE=%q", input.ServerSideEncryption)
	}
	if input.SSEKMSKeyId == nil || *input.SSEKMSKeyId != "abc" {
		t.Fatalf("multipart SSEKMSKeyId=%v", input.SSEKMSKeyId)
	}
}

func TestBuildCosPutHeader_SSE(t *testing.T) {
	cases := []struct {
		name     string
		opts     *PutOptions
		wantSSE  string
		wantKey  string
		wantTags string
	}{
		{"none", &PutOptions{}, "", "", ""},
		{"sse-cos AES256", &PutOptions{SSE: "AES256"}, "AES256", "", ""},
		{"sse-cos kms with key", &PutOptions{SSE: "cos/kms", SSEKMSKeyID: "uuid-1234"}, "cos/kms", "uuid-1234", ""},
		{"sse-cos kms no key", &PutOptions{SSE: "cos/kms"}, "cos/kms", "", ""},
		{"tag + sse coexist", &PutOptions{SSE: "AES256", Tags: map[string]string{"env": "prod"}}, "AES256", "", "env=prod"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := buildCosPutHeader(tc.opts, -1)
			if h == nil {
				if tc.wantSSE == "" && tc.wantTags == "" {
					return
				}
				t.Fatalf("expected header, got nil")
			}
			if tc.wantSSE == "" && tc.wantTags == "" {
				if h.XOptionHeader != nil && len(*h.XOptionHeader) > 0 {
					t.Fatalf("want empty XOptionHeader, got %v", *h.XOptionHeader)
				}
				return
			}
			if h.XOptionHeader == nil {
				t.Fatalf("expected XOptionHeader to be set")
			}
			gotSSE := h.XOptionHeader.Get("x-cos-server-side-encryption")
			if gotSSE != tc.wantSSE {
				t.Fatalf("x-cos-server-side-encryption=%q want %q", gotSSE, tc.wantSSE)
			}
			gotKey := h.XOptionHeader.Get("x-cos-server-side-encryption-cos-kms-key-id")
			if gotKey != tc.wantKey {
				t.Fatalf("kms-key-id=%q want %q", gotKey, tc.wantKey)
			}
			gotTags := h.XOptionHeader.Get("x-cos-tagging")
			if !strings.Contains(gotTags, tc.wantTags) {
				t.Fatalf("x-cos-tagging=%q want substr %q", gotTags, tc.wantTags)
			}
		})
	}
}
