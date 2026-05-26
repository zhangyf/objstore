package objstore

import (
	"context"
	"io"
	"time"
)

// ObjectInfo 对象元信息
type ObjectInfo struct {
	Key          string
	Size         int64
	ETag         string
	LastModified time.Time
	StorageClass string
}

// Store 统一对象存储接口，COS / S3 各自实现
type Store interface {
	// --- 元信息 ---

	// HeadObject 获取对象元信息
	HeadObject(ctx context.Context, key string) (*ObjectInfo, error)

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

	// --- 预签名 URL ---

	// PresignGetObject 生成 GET 对象的预签名 URL，调用方可直接通过该 URL 下载对象。
	// expires 为 URL 有效期。
	PresignGetObject(ctx context.Context, key string, expires time.Duration) (string, error)

	// PresignPutObject 生成 PUT 对象的预签名 URL，调用方可直接通过该 URL 上传对象（HTTP PUT）。
	// expires 为 URL 有效期。
	PresignPutObject(ctx context.Context, key string, expires time.Duration) (string, error)

	// --- 其他 ---

	// DeleteObject 删除对象
	DeleteObject(ctx context.Context, key string) error

	// BucketName 返回桶名，用于日志
	BucketName() string

	// Provider 返回存储类型
	Provider() ProviderType
}

// ServerCopier 服务端对象复制接口，COS 和 S3 均实现。
// 调用方通过类型断言判断是否可用，可用时走服务端复制（不过本机带宽）。
type ServerCopier interface {
	// CopyObject 小文件单次服务端复制，适合不超过分块阈値的对象。
	CopyObject(ctx context.Context, dstKey string, src ServerCopier, srcKey string) error

	// CopyPartFrom 大文件服务端分块复制。
	// onChunkDone 每完成一个分块后回调已传输字节数，可用于进度统计。
	CopyPartFrom(ctx context.Context, dstKey string, src ServerCopier,
		srcKey string, totalSize, chunkSize int64, concurrency int,
		onChunkDone func(int64)) error
}
