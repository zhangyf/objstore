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

// ListOptions 列表查询选项
type ListOptions struct {
	Prefix    string // 前缀路径
	Delimiter string // 分隔符，默认 "/"；传 "" 则递归列出所有对象（不分组）
}

// PutOptions 上传/初始化 multipart 时可选的对象属性。
// COS / S3 各自映射到对应 SDK。为 nil 时表示不设置。
type PutOptions struct {
	// HTTP 头
	ContentType  string
	CacheControl string

	// 用户自定义元数据（不带前缀，底层 SDK 会加 x-amz-meta-/x-cos-meta-）
	Metadata map[string]string

	// 存储类型。填入云友好的字面量：
	//   COS:  STANDARD | STANDARD_IA | INTELLIGENT_TIERING | ARCHIVE | DEEP_ARCHIVE | MAZ_STANDARD | MAZ_STANDARD_IA
	//   S3:   STANDARD | STANDARD_IA | ONEZONE_IA | INTELLIGENT_TIERING | GLACIER | DEEP_ARCHIVE | REDUCED_REDUNDANCY
	StorageClass string

	// Canned ACL。注意：S3 与 COS 支持的 canned 值不同，调用方负责按 provider 校验。
	//   S3 (7): private | public-read | public-read-write | authenticated-read |
	//           aws-exec-read | bucket-owner-read | bucket-owner-full-control
	//   COS (4): default | private | public-read | public-read-write
	ACL string

	// 对象 Tag (URL-safe key=value)。S3 走 x-amz-tagging，COS 走 x-cos-tagging。
	Tags map[string]string
}

// HasAny 返回是否含任意一项设置，默认 nil 表示什么都不传。
func (o *PutOptions) HasAny() bool {
	if o == nil {
		return false
	}
	return o.ContentType != "" || o.CacheControl != "" || o.StorageClass != "" ||
		o.ACL != "" || len(o.Metadata) > 0 || len(o.Tags) > 0
}

// Store 统一对象存储接口，COS / S3 各自实现
type Store interface {
	// --- 元信息 ---

	// HeadObject 获取对象元信息
	HeadObject(ctx context.Context, key string) (*ObjectInfo, error)

	// ListObjects 列出指定前缀下的对象，含 size 等完整信息。
	// 默认使用 "/" 作为分隔符（只列出当前层）。
	// 如需递归列出所有对象，请设置 opts.Delimiter = ""。
	ListObjects(ctx context.Context, opts ListOptions) ([]ObjectInfo, error)

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

// UploadedPart 已上传的分块信息
type UploadedPart struct {
	PartNumber int
	ETag       string
	Size       int64
}

// MultipartResumer 可恢复的分块上传接口。
// COS / S3 实现；调用方通过类型断言检测。
//
// 典型使用流程：
//   1. InitMultipart 拿到 uploadID（或复用上次保存的 uploadID）
//   2. ListParts 看哪些分块已上传
//   3. 遵循跳过策略，调 UploadPartN 上传未完成的分块
//   4. CompleteMultipart 提交 所有分块
//   5. 如需主动丢弃 → AbortMultipart
type MultipartResumer interface {
	InitMultipart(ctx context.Context, key string) (uploadID string, err error)
	ListParts(ctx context.Context, key, uploadID string) ([]UploadedPart, error)
	UploadPartN(ctx context.Context, key, uploadID string, partNumber int, data []byte) (etag string, err error)
	CompleteMultipart(ctx context.Context, key, uploadID string, parts []UploadedPart) error
	AbortMultipart(ctx context.Context, key, uploadID string) error
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

// OptionalUploader 可选的带属性上传接口。
// 调用方通过类型断言检测；COS / S3 均实现。
// opts 为 nil 时等价于调用不带参数的上传。
type OptionalUploader interface {
	PutObjectOpt(ctx context.Context, key string, data []byte, opts *PutOptions) error
	PutObjectStreamOpt(ctx context.Context, key string, r io.Reader, size int64, opts *PutOptions) error
	MultipartUploadOpt(ctx context.Context, key string, totalSize, chunkSize int64, concurrency int,
		fetchPart func(partNumber int, offset, size int64) ([]byte, error), opts *PutOptions) error
	InitMultipartOpt(ctx context.Context, key string, opts *PutOptions) (uploadID string, err error)
}
