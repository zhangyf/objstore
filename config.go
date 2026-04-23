package objstore

import "fmt"

// ProviderType 存储提供商类型
type ProviderType string

const (
	ProviderCOS ProviderType = "cos"
	ProviderS3  ProviderType = "s3"
)

// Config 统一存储配置
type Config struct {
	Provider  ProviderType
	Bucket    string
	Region    string
	SecretID  string // COS SecretId / S3 Access Key ID
	SecretKey string // COS SecretKey / S3 Secret Access Key
	Endpoint  string // 自定义 endpoint（S3 兼容模式）
}

// New 根据 Config 创建对应的 Store 实现
func New(cfg Config) (Store, error) {
	switch cfg.Provider {
	case ProviderCOS:
		return newCOSStore(cfg)
	case ProviderS3:
		return newS3Store(cfg)
	default:
		return nil, fmt.Errorf("objstore: unknown provider %q, want %q or %q", cfg.Provider, ProviderCOS, ProviderS3)
	}
}
