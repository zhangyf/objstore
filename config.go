package objstore

import "fmt"

// ProviderType 存储提供商类型
type ProviderType string

const (
	ProviderCOS ProviderType = "cos"
	ProviderS3  ProviderType = "s3"
)

// Config 统一存储配置。
//
// 凭证解析顺序（仅 S3）：
//   1. SecretID/SecretKey 同时非空 → 静态凭证（保持向后兼容）。
//   2. 否则走 awssdk default credential chain：
//      env (AWS_ACCESS_KEY_ID/SECRET) → shared credentials file (Profile)
//      → ECS/EKS container creds → EC2 IMDS → STS AssumeRole。
//      若指定 Profile，则只走该 profile（含 source_profile/AssumeRole 链）。
//
// COS 仅支持静态凭证（SecretID/SecretKey）。
type Config struct {
	Provider  ProviderType
	Bucket    string
	Region    string
	SecretID  string // COS SecretId / S3 Access Key ID（可空，S3 缺省走 default chain）
	SecretKey string // COS SecretKey / S3 Secret Access Key
	Endpoint  string // 自定义 endpoint（S3 兼容模式）

	// Profile 仅 S3 生效：从 ~/.aws/credentials / ~/.aws/config 加载指定 profile。
	// 仅当 SecretID/SecretKey 都为空时生效。空字符串 = 不指定（走 default chain）。
	Profile string

	// SSECustomerKey 启用 SSE-C（Server-Side Encryption with Customer-provided keys）。
	//
	// 取值：32 字节（256-bit）AES 原始密钥；nil 或长度 0 表示不启用 SSE-C。
	// 一旦非空，长度必须恰好 32 字节，否则 New 返回错误。
	//
	// SSE-C 与普通 SSE（PutOptions.SSE / SSEKMSKeyID）互斥：启用 SSE-C 时
	// 上传不再设 x-cos-server-side-encryption / ServerSideEncryption。
	//
	// SSE-C 密钥绑在 Store 实例上，同一个加密桶的所有读写（上传/下载/Head/
	// 分块每个 part/拷贝）共用同一把密钥，由 Store 在各路径自动注入三个 header：
	//   COS:  x-cos-server-side-encryption-customer-algorithm / -key / -key-MD5
	//   S3:   x-amz-server-side-encryption-customer-algorithm / -key / -key-MD5
	//
	// 注意：SSE-C 要求 HTTPS（COS/S3 均强制）。
	SSECustomerKey []byte
}

// New 根据 Config 创建对应的 Store 实现
func New(cfg Config) (Store, error) {
	if len(cfg.SSECustomerKey) != 0 && len(cfg.SSECustomerKey) != 32 {
		return nil, fmt.Errorf("objstore: SSECustomerKey 必须恰好 32 字节（当前 %d 字节）", len(cfg.SSECustomerKey))
	}
	switch cfg.Provider {
	case ProviderCOS:
		return newCOSStore(cfg)
	case ProviderS3:
		return newS3Store(cfg)
	default:
		return nil, fmt.Errorf("objstore: unknown provider %q, want %q or %q", cfg.Provider, ProviderCOS, ProviderS3)
	}
}
