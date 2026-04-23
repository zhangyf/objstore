package objstore

import "context"

// COSCopier 是 COS 专属的服务端分块复制能力接口。
// 仅 COS Store 实现此接口；上层工具（如 objcli）通过类型断言判断是否可用。
type COSCopier interface {
	// CopyPartFrom 使用服务端 UploadPart-Copy 将 src 的 srcKey 复制到自身的 dstKey。
	// 不经过本机带宽，适合大文件 COS→COS 迁移。
	// onChunkDone 每完成一个分块后回调已传输字节数，可用于进度统计。
	CopyPartFrom(ctx context.Context, dstKey string, src COSCopier,
		srcKey string, totalSize, chunkSize int64, concurrency int,
		onChunkDone func(int64)) error
}
