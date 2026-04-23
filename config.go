package objstore

import "fmt"

// Config 统一存储配置
type Config struct {
	Provider  string // "cos" 或 "s3"
	Bucket    string
	Region    string
	SecretID  string // COS SecretId / S3 Access Key ID
	SecretKey string // COS SecretKey / S3 Secret Access Key
	Endpoint  string // 自定义 endpoint（S3 兼容模式）
}

// New 根据 Config 创建对应的 Store 实现
func New(cfg Config) (Store, error) {
	switch cfg.Provider {
	case "cos":
		return newCOSStore(cfg)
	case "s3":
		return newS3Store(cfg)
	default:
		return nil, fmt.Errorf("objstore: unknown provider %q, want cos or s3", cfg.Provider)
	}
}
