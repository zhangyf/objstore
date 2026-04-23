package objstore

import (
	"context"
	"io"
)

// ObjectInfo 对象元信息
type ObjectInfo struct {
	Key  string
	Size int64
}

// Store 统一对象存储接口，COS / S3 各自实现
type Store interface {
	// --- 元信息 ---

	// HeadObject 获取对象大小
	HeadObject(ctx context.Context, key string) (int64, error)

	// ListObjects 列出指定前缀下所有对象 Key（不含 size）
	ListObjects(ctx context.Context, prefix string) ([]string, error)

	// ListObjectsWithSize 列出指定前缀下所有对象，含 size
	ListObjectsWithSize(ctx context.Context, prefix string) ([]ObjectInfo, error)

	// --- 下载 ---

	// GetObject 流式读取整个对象，调用方负责 Close
	GetObject(ctx context.Context, key string) (io.ReadCloser, error)

	// GetRange 按字节范围下载
	GetRange(ctx context.Context, key string, start, end int64) ([]byte, error)

	// GetAll 一次性读取整个对象到内存
	GetAll(ctx context.Context, key string) ([]byte, error)

	// --- 上传 ---

	// PutObject 单次上传（小文件）
	PutObject(ctx context.Context, key string, data []byte) error

	// PutObjectStream 流式上传，size=-1 时使用 chunked encoding（COS/S3 行为不同）
	PutObjectStream(ctx context.Context, key string, r io.Reader, size int64) error

	// --- 分块上传 ---

	// MultipartUpload 分块上传（大文件）
	// fetchPart 回调按 offset/size 获取分块数据
	MultipartUpload(ctx context.Context, key string, totalSize, chunkSize int64, concurrency int,
		fetchPart func(partNumber int, offset, size int64) ([]byte, error)) error

	// --- 其他 ---

	// DeleteObject 删除对象
	DeleteObject(ctx context.Context, key string) error

	// CopyObject 同桶内复制（服务端）
	CopyObject(ctx context.Context, srcKey, dstKey string) error

	// BucketName 返回桶名，用于日志
	BucketName() string

	// Provider 返回存储类型
	Provider() ProviderType
}

// ServerCopier 服务端对象复制接口，COS 和 S3 均实现。
// 调用方通过类型断言判断是否可用，可用时走服务端复制（不过本机带宽）。
type ServerCopier interface {
	// CopyPartFrom 将 src 的 srcKey 以服务端分块复制到自身的 dstKey。
	// onChunkDone 每完成一个分块后回调已传输字节数，可用于进度统计。
	CopyPartFrom(ctx context.Context, dstKey string, src ServerCopier,
		srcKey string, totalSize, chunkSize int64, concurrency int,
		onChunkDone func(int64)) error
}
