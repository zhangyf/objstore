package objstore

import (
	"os"
	"strings"
	"testing"
)

// 同时给 SecretID 不给 SecretKey → 必须报错
func TestS3_PartialStaticCreds_Rejected(t *testing.T) {
	_, err := newS3Store(Config{
		Provider:  ProviderS3,
		Bucket:    "x",
		Region:    "us-east-1",
		SecretID:  "AK",
		SecretKey: "",
	})
	if err == nil {
		t.Fatal("expected error when only SecretID provided, got nil")
	}
	if !strings.Contains(err.Error(), "SecretID 与 SecretKey 必须同时提供") {
		t.Fatalf("unexpected error: %v", err)
	}
}

// SecretID/Key 全空 → 走 default chain，env 缺失时不应在构造时炸（lazy 解析）
func TestS3_DefaultChain_NoStaticCreds(t *testing.T) {
	// 清掉可能干扰的 env
	cleanup := stashEnv(t,
		"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY",
		"AWS_PROFILE", "AWS_SHARED_CREDENTIALS_FILE", "AWS_CONFIG_FILE",
	)
	defer cleanup()
	// 把 shared credentials 指向一个不存在的文件，避免读到本机真的 ~/.aws
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", "/dev/null/objstore-test-noexist")
	t.Setenv("AWS_CONFIG_FILE", "/dev/null/objstore-test-noexist")

	_, err := newS3Store(Config{
		Provider: ProviderS3,
		Bucket:   "x",
		Region:   "us-east-1",
	})
	// awssdk 在 LoadDefaultConfig 阶段不会强制要求凭证存在，凭证解析是 lazy 的
	if err != nil {
		t.Fatalf("LoadDefaultConfig should not fail without static creds: %v", err)
	}
}

// 指定 Profile 且 profile 不存在：LoadDefaultConfig 会报错 (awssdk 在初始化阶段校验 shared profile)
func TestS3_Profile_NotFound(t *testing.T) {
	cleanup := stashEnv(t,
		"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY",
	)
	defer cleanup()
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", "/dev/null/objstore-test-noexist")
	t.Setenv("AWS_CONFIG_FILE", "/dev/null/objstore-test-noexist")

	_, err := newS3Store(Config{
		Provider: ProviderS3,
		Bucket:   "x",
		Region:   "us-east-1",
		Profile:  "no-such-profile",
	})
	if err == nil {
		t.Fatal("expected error for missing profile, got nil")
	}
	if !strings.Contains(err.Error(), "no-such-profile") {
		t.Fatalf("unexpected error: %v", err)
	}
}

// 静态凭证两个都给 → 构造应当成功
func TestS3_StaticCreds_OK(t *testing.T) {
	_, err := newS3Store(Config{
		Provider:  ProviderS3,
		Bucket:    "x",
		Region:    "us-east-1",
		SecretID:  "AK",
		SecretKey: "SK",
	})
	if err != nil {
		t.Fatalf("static creds should succeed: %v", err)
	}
}

// stashEnv 暂存并清空指定 env 变量，返回恢复函数。
func stashEnv(t *testing.T, keys ...string) func() {
	t.Helper()
	saved := map[string]string{}
	for _, k := range keys {
		if v, ok := os.LookupEnv(k); ok {
			saved[k] = v
		}
		os.Unsetenv(k)
	}
	return func() {
		for k := range saved {
			os.Setenv(k, saved[k])
		}
	}
}
