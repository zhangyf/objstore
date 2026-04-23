package objstore

import (
	"context"
	"fmt"
)

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

// CopyPartFrom COS→COS 服务端分块复制大文件（不过本机带宽）。
// src/dst 必须都是 COS Store，否则返回 error。
func CopyPartFrom(ctx context.Context, dst, src Store,
	dstKey, srcKey string, totalSize, chunkSize int64, concurrency int,
	onChunkDone func(int64)) error {

	dstCOS, ok1 := dst.(*cosStore)
	srcCOS, ok2 := src.(*cosStore)
	if !ok1 || !ok2 {
		return fmt.Errorf("objstore.CopyPartFrom: both src and dst must be COS stores")
	}
	return dstCOS.CopyPartFrom(ctx, dstKey, srcCOS, srcKey, totalSize, chunkSize, concurrency, onChunkDone)
}
