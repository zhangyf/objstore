package objstore

import (
	"crypto/md5"
	"encoding/base64"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	cos "github.com/tencentyun/cos-go-sdk-v5"
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

// ============================================================
// SSE-C 单测
// ============================================================

// repeatKey 生成长度为 n 的测试密钥（字节内容为 i%256）。
func repeatKey(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i % 256)
	}
	return b
}

func TestNew_SSECustomerKey_LengthValidation(t *testing.T) {
	// 长度非 0 且非 32 → New 报错
	bad := []int{1, 16, 31, 33, 44, 64}
	for _, n := range bad {
		_, err := New(Config{Provider: ProviderCOS, Bucket: "b", Region: "ap-beijing",
			SecretID: "id", SecretKey: "key", SSECustomerKey: repeatKey(n)})
		if err == nil {
			t.Fatalf("len=%d 应当报错，但 New 成功", n)
		}
		if !strings.Contains(err.Error(), "32") {
			t.Fatalf("len=%d 错误信息应提到 32 字节，得到: %v", n, err)
		}
	}
	// 恰好 32 字节 → 成功
	st, err := New(Config{Provider: ProviderCOS, Bucket: "b", Region: "ap-beijing",
		SecretID: "id", SecretKey: "key", SSECustomerKey: repeatKey(32)})
	if err != nil {
		t.Fatalf("len=32 应当成功，得到错误: %v", err)
	}
	cs, ok := st.(*cosStore)
	if !ok || cs.ssec == nil {
		t.Fatalf("32 字节密钥应在 cosStore.ssec 上启用 SSE-C")
	}
	// 不传 → 不启用
	st2, err := New(Config{Provider: ProviderCOS, Bucket: "b", Region: "ap-beijing",
		SecretID: "id", SecretKey: "key"})
	if err != nil {
		t.Fatalf("不传 SSECustomerKey 不应报错: %v", err)
	}
	if st2.(*cosStore).ssec != nil {
		t.Fatalf("未传密钥时 ssec 应为 nil")
	}
}

func TestNewSSECustomerKey_HeaderValues(t *testing.T) {
	raw := repeatKey(32)
	k := newSSECustomerKey(raw)
	if k == nil {
		t.Fatal("newSSECustomerKey(32B) 返回 nil")
	}
	if k.algo() != "AES256" {
		t.Fatalf("algo=%q want AES256", k.algo())
	}
	wantKeyB64 := base64.StdEncoding.EncodeToString(raw)
	if k.keyB64 != wantKeyB64 {
		t.Fatalf("keyB64=%q want %q", k.keyB64, wantKeyB64)
	}
	sum := md5.Sum(raw)
	wantMD5 := base64.StdEncoding.EncodeToString(sum[:])
	if k.md5B64 != wantMD5 {
		t.Fatalf("md5B64=%q want %q", k.md5B64, wantMD5)
	}
	// 长度非 32 → nil
	if newSSECustomerKey(repeatKey(31)) != nil {
		t.Fatal("31 字节应返回 nil")
	}
	if newSSECustomerKey(nil) != nil {
		t.Fatal("nil 应返回 nil")
	}
}

func TestCOS_SSEC_InjectsGetHeaders(t *testing.T) {
	raw := repeatKey(32)
	c := &cosStore{ssec: newSSECustomerKey(raw)}

	// Get
	opt := c.applySSECGet(nil)
	if opt == nil {
		t.Fatal("applySSECGet 应返回非 nil")
	}
	if opt.XCosSSECustomerAglo != "AES256" {
		t.Fatalf("Get algo=%q", opt.XCosSSECustomerAglo)
	}
	if opt.XCosSSECustomerKey != c.ssec.keyB64 {
		t.Fatalf("Get key=%q want %q", opt.XCosSSECustomerKey, c.ssec.keyB64)
	}
	if opt.XCosSSECustomerKeyMD5 != c.ssec.md5B64 {
		t.Fatalf("Get keyMD5=%q want %q", opt.XCosSSECustomerKeyMD5, c.ssec.md5B64)
	}

	// Head
	hopt := c.applySSECHead(nil)
	if hopt.XCosSSECustomerAglo != "AES256" || hopt.XCosSSECustomerKey != c.ssec.keyB64 || hopt.XCosSSECustomerKeyMD5 != c.ssec.md5B64 {
		t.Fatalf("Head header 注入不正确: %+v", hopt)
	}

	// Put header
	ph := c.applySSECPutHeader(nil)
	if ph.XCosSSECustomerAglo != "AES256" || ph.XCosSSECustomerKey != c.ssec.keyB64 || ph.XCosSSECustomerKeyMD5 != c.ssec.md5B64 {
		t.Fatalf("Put header 注入不正确: %+v", ph)
	}

	// UploadPart
	up := c.applySSECUploadPart(nil)
	if up.XCosSSECustomerAglo != "AES256" || up.XCosSSECustomerKey != c.ssec.keyB64 || up.XCosSSECustomerKeyMD5 != c.ssec.md5B64 {
		t.Fatalf("UploadPart header 注入不正确: %+v", up)
	}

	// Init multipart（通过嵌入的 ObjectPutHeaderOptions）
	io := c.initOptWithSSEC(nil)
	if io == nil || io.ObjectPutHeaderOptions == nil {
		t.Fatal("initOptWithSSEC 应注入 ObjectPutHeaderOptions")
	}
	if io.ObjectPutHeaderOptions.XCosSSECustomerKey != c.ssec.keyB64 {
		t.Fatalf("Init header key=%q", io.ObjectPutHeaderOptions.XCosSSECustomerKey)
	}
}

func TestCOS_SSEC_Disabled_NoInject(t *testing.T) {
	c := &cosStore{} // ssec == nil
	if c.applySSECGet(nil) != nil {
		t.Fatal("未启用 SSE-C 时 applySSECGet(nil) 应返回 nil")
	}
	if c.applySSECHead(nil) != nil {
		t.Fatal("未启用 SSE-C 时 applySSECHead(nil) 应返回 nil")
	}
	if c.putOptWithSSEC(nil) != nil {
		t.Fatal("未启用 SSE-C 时 putOptWithSSEC(nil) 应返回 nil")
	}
	// 已有 opt 不应被改动
	got := c.applySSECGet(&cos.ObjectGetOptions{Range: "bytes=0-1"})
	if got.XCosSSECustomerAglo != "" {
		t.Fatal("未启用 SSE-C 时不应写入 algo")
	}
}

func TestCOS_SSEC_CopyHeaders(t *testing.T) {
	dst := &cosStore{ssec: newSSECustomerKey(repeatKey(32))}
	src := &cosStore{ssec: newSSECustomerKey(repeatKey(32))}
	co := dst.copyOptWithSSEC(src)
	if co == nil || co.ObjectCopyHeaderOptions == nil {
		t.Fatal("copyOptWithSSEC 应返回非 nil")
	}
	h := co.ObjectCopyHeaderOptions
	if h.XCosSSECustomerKey != dst.ssec.keyB64 {
		t.Fatalf("dst key 未注入: %q", h.XCosSSECustomerKey)
	}
	if h.XCosCopySourceSSECustomerKey != src.ssec.keyB64 {
		t.Fatalf("copy-source key 未注入: %q", h.XCosCopySourceSSECustomerKey)
	}
	// 都未启用 → nil
	if (&cosStore{}).copyOptWithSSEC(&cosStore{}) != nil {
		t.Fatal("两侧均未启用 SSE-C 时应返回 nil")
	}

	// CopyPart 仅注入 copy-source（dst 由 Init 决定）
	cp := dst.copyPartOptWithSSEC(src, &cos.ObjectCopyPartOptions{XCosCopySource: "u"})
	if cp.XCosCopySourceSSECustomerKey != src.ssec.keyB64 {
		t.Fatalf("CopyPart copy-source key 未注入: %q", cp.XCosCopySourceSSECustomerKey)
	}
}

func TestS3_SSEC_InjectsHeaders(t *testing.T) {
	raw := repeatKey(32)
	s := &s3Store{ssec: newSSECustomerKey(raw)}
	wantKey := s.ssec.keyB64
	wantMD5 := s.ssec.md5B64

	check := func(name, algo, key, md5v string) {
		if algo != "AES256" {
			t.Fatalf("%s algo=%q want AES256", name, algo)
		}
		if key != wantKey {
			t.Fatalf("%s key=%q want %q", name, key, wantKey)
		}
		if md5v != wantMD5 {
			t.Fatalf("%s md5=%q want %q", name, md5v, wantMD5)
		}
	}

	gi := &s3.GetObjectInput{Bucket: aws.String("b"), Key: aws.String("k")}
	s.applySSECGetInput(gi)
	check("Get", aws.ToString(gi.SSECustomerAlgorithm), aws.ToString(gi.SSECustomerKey), aws.ToString(gi.SSECustomerKeyMD5))

	hi := &s3.HeadObjectInput{Bucket: aws.String("b"), Key: aws.String("k")}
	s.applySSECHeadInput(hi)
	check("Head", aws.ToString(hi.SSECustomerAlgorithm), aws.ToString(hi.SSECustomerKey), aws.ToString(hi.SSECustomerKeyMD5))

	pi := &s3.PutObjectInput{Bucket: aws.String("b"), Key: aws.String("k")}
	s.applySSECPutInput(pi)
	check("Put", aws.ToString(pi.SSECustomerAlgorithm), aws.ToString(pi.SSECustomerKey), aws.ToString(pi.SSECustomerKeyMD5))

	ci := &s3.CreateMultipartUploadInput{Bucket: aws.String("b"), Key: aws.String("k")}
	s.applySSECCreateMultipart(ci)
	check("CreateMultipart", aws.ToString(ci.SSECustomerAlgorithm), aws.ToString(ci.SSECustomerKey), aws.ToString(ci.SSECustomerKeyMD5))

	ui := &s3.UploadPartInput{Bucket: aws.String("b"), Key: aws.String("k")}
	s.applySSECUploadPart(ui)
	check("UploadPart", aws.ToString(ui.SSECustomerAlgorithm), aws.ToString(ui.SSECustomerKey), aws.ToString(ui.SSECustomerKeyMD5))

	// Copy: dst + copy-source
	src := &s3Store{ssec: newSSECustomerKey(repeatKey(32))}
	coi := &s3.CopyObjectInput{Bucket: aws.String("b"), Key: aws.String("k")}
	s.applySSECCopyObject(coi, src)
	check("CopyObject-dst", aws.ToString(coi.SSECustomerAlgorithm), aws.ToString(coi.SSECustomerKey), aws.ToString(coi.SSECustomerKeyMD5))
	if aws.ToString(coi.CopySourceSSECustomerKey) != src.ssec.keyB64 {
		t.Fatalf("CopyObject copy-source key=%q", aws.ToString(coi.CopySourceSSECustomerKey))
	}

	upc := &s3.UploadPartCopyInput{Bucket: aws.String("b"), Key: aws.String("k")}
	s.applySSECUploadPartCopy(upc, src)
	check("UploadPartCopy-dst", aws.ToString(upc.SSECustomerAlgorithm), aws.ToString(upc.SSECustomerKey), aws.ToString(upc.SSECustomerKeyMD5))
	if aws.ToString(upc.CopySourceSSECustomerKey) != src.ssec.keyB64 {
		t.Fatalf("UploadPartCopy copy-source key=%q", aws.ToString(upc.CopySourceSSECustomerKey))
	}
}

func TestS3_SSEC_Disabled_NoInject(t *testing.T) {
	s := &s3Store{} // ssec == nil
	gi := &s3.GetObjectInput{Bucket: aws.String("b"), Key: aws.String("k")}
	s.applySSECGetInput(gi)
	if gi.SSECustomerAlgorithm != nil {
		t.Fatal("未启用 SSE-C 时不应写入 S3 Get SSECustomerAlgorithm")
	}
	pi := &s3.PutObjectInput{Bucket: aws.String("b"), Key: aws.String("k")}
	s.applySSECPutInput(pi)
	if pi.SSECustomerKey != nil {
		t.Fatal("未启用 SSE-C 时不应写入 S3 Put SSECustomerKey")
	}
}
