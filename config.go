package objstore

import (
	"context"
	"fmt"
)

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

// IsCOSStore 判断 Store 是否为 COS 实现，是则返回内部 cosStore 供跨包调用
// 仅供 objcli 等上层工具做 COS→COS 服务端复制时使用
func IsCOSStore(s Store) (*COSStore, bool) {
	c, ok := s.(*cosStore)
	if !ok {
		return nil, false
	}
	return (*COSStore)(c), true
}

// COSStore 是对外暴露的 COS store 句柄，仅用于 CopyPartFrom
type COSStore cosStore

// CopyPartFrom 服务端分块复制（不过本机带宽），仅 COS→COS 可用
func (dst *COSStore) CopyPartFrom(ctx context.Context, dstKey string, src *COSStore,
	srcKey string, totalSize, chunkSize int64, concurrency int, onChunkDone func(int64)) error {
	return (*cosStore)(dst).CopyPartFrom(ctx, dstKey, (*cosStore)(src), srcKey, totalSize, chunkSize, concurrency, onChunkDone)
}
