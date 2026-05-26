# objstore

统一的对象存储客户端库，为腾讯云 COS 和 AWS S3（及 S3 兼容存储）提供一致的 Go 接口。

> Unified object storage client for COS and S3.

## 特性

- ✅ **统一接口**：`Store` 接口屏蔽 COS / S3 差异，一套代码同时支持两种后端
- ✅ **流式 I/O**：上传下载均支持流式读写，大文件不落盘、不占内存
- ✅ **并发分块上传**：内置 worker 池，支持自定义并发度和分块大小
- ✅ **服务端复制**：提供 `ServerCopier` 接口，跨桶/同桶复制走对象存储侧（不过本机带宽）
- ✅ **内网优化**：COS 默认使用 `cos-internal.<region>.tencentcos.cn` 域名，走腾讯云内网链路
- ✅ **S3 兼容**：通过 `Endpoint` 字段可对接任意 S3 兼容存储（MinIO 等），自动启用 path-style
- ✅ **预签名 URL**：内置 GET 预签名，临时分发对象无需暴露密钥
- ✅ **可选调试日志**：通过 `OBJSTORE_DEBUG` / `COS_DEBUG` 环境变量或 `SetDebug()` 开启详细操作日志

## 安装

```bash
go get github.com/zhangyf/objstore
```

依赖：
- Go 1.24.11+
- `github.com/tencentyun/cos-go-sdk-v5`
- `github.com/aws/aws-sdk-go-v2`

## 快速开始

```go
package main

import (
    "context"
    "log"

    "github.com/zhangyf/objstore"
)

func main() {
    // 创建 COS Store
    store, err := objstore.New(objstore.Config{
        Provider:  objstore.ProviderCOS,
        Bucket:    "my-bucket-1250000000",
        Region:    "ap-guangzhou",
        SecretID:  "your-secret-id",
        SecretKey: "your-secret-key",
    })
    if err != nil {
        log.Fatal(err)
    }

    ctx := context.Background()

    // 上传
    if err := store.PutObject(ctx, "hello.txt", []byte("hello world")); err != nil {
        log.Fatal(err)
    }

    // 下载
    data, err := store.GetAll(ctx, "hello.txt")
    if err != nil {
        log.Fatal(err)
    }
    log.Printf("got: %s", data)
}
```

切换到 S3 只需改 `Provider` 即可：

```go
store, _ := objstore.New(objstore.Config{
    Provider:  objstore.ProviderS3,
    Bucket:    "my-s3-bucket",
    Region:    "us-east-1",
    SecretID:  "AKIA...",      // S3 Access Key ID
    SecretKey: "...",           // S3 Secret Access Key
    Endpoint:  "",              // 留空走 AWS；填自定义 endpoint 即对接 S3 兼容存储
})
```

## 已实现的能力

### 1. 配置与构造

| 能力 | 说明 |
|---|---|
| `objstore.New(Config)` | 工厂函数，按 `Provider` 字段返回对应实现 |
| `Config.Provider` | 支持 `ProviderCOS` / `ProviderS3` |
| `Config.Endpoint` | S3 模式下可指定自定义 endpoint，自动启用 path-style，对接 MinIO 等 S3 兼容存储 |
| 内网域名 | COS 默认使用 `<bucket>.cos-internal.<region>.tencentcos.cn` |
| 连接池 | COS 客户端预设 `MaxIdleConns=200`、`MaxIdleConnsPerHost=100`、`IdleConnTimeout=90s` |

### 2. 元信息操作

| 方法 | COS | S3 | 说明 |
|---|:---:|:---:|---|
| `HeadObject(ctx, key) (int64, error)` | ✅ | ✅ | 获取对象大小 |
| `ListObjects(ctx, prefix) ([]string, error)` | ✅ | ✅ | 列出前缀下所有 Key（自动翻页） |
| `ListObjectsWithSize(ctx, prefix) ([]ObjectInfo, error)` | ✅ | ✅ | 列出前缀下所有对象（含 Size，自动翻页） |
| `BucketName() string` | ✅ | ✅ | 返回桶名（用于日志） |
| `Provider() ProviderType` | ✅ | ✅ | 返回存储类型 |

> ListObjects 内部自动循环翻页（COS 用 `Marker`，S3 用 `ContinuationToken`），单页 1000 条。

### 3. 下载

| 方法 | COS | S3 | 说明 |
|---|:---:|:---:|---|
| `GetObject(ctx, key) (io.ReadCloser, error)` | ✅ | ✅ | 流式读取，调用方负责 Close |
| `GetRange(ctx, key, start, end) ([]byte, error)` | ✅ | ✅ | 按字节范围下载 |
| `GetAll(ctx, key) ([]byte, error)` | ✅ | ✅ | 一次性读取整个对象到内存 |

### 4. 上传

| 方法 | COS | S3 | 说明 |
|---|:---:|:---:|---|
| `PutObject(ctx, key, data) error` | ✅ | ✅ | 单次上传（小文件） |
| `PutObjectStream(ctx, key, r, size) error` | ✅ | ✅ | 流式上传；`size=-1` 时使用 chunked encoding |
| `MultipartUpload(ctx, key, totalSize, chunkSize, concurrency, fetchPart) error` | ✅ | ✅ | 并发分块上传，回调式拉取分块数据 |

`MultipartUpload` 关键点：
- 分块数 = `⌈totalSize / chunkSize⌉`
- 内置 worker 池，按 `concurrency` 并发上传
- `fetchPart(partNumber, offset, size)` 由调用方提供，可对接任意数据源（本地文件 / 网络流 / 内存等）
- 任意分块失败会自动 `AbortMultipartUpload`，避免残留碎片
- 最终按 `PartNumber` 排序后调用 `CompleteMultipartUpload`

### 5. 服务端复制（`ServerCopier` 接口）

服务端复制走对象存储侧链路，**不占用本机带宽**，适合跨桶或同桶迁移。

| 方法 | COS | S3 | 说明 |
|---|:---:|:---:|---|
| `CopyObject(ctx, dstKey, src, srcKey) error` | ✅ | ✅ | 单次服务端复制（小文件） |
| `CopyPartFrom(ctx, dstKey, src, srcKey, totalSize, chunkSize, concurrency, onChunkDone) error` | ✅ | ✅ | 并发分块服务端复制（大文件），支持进度回调 |

实现细节：
- COS 走 `PUT Object - Copy` / `Upload Part - Copy`
- S3 走 `CopyObject` / `UploadPartCopy`
- 通过类型断言保证 src 与 dst 同为一种 Provider（COS↔COS 或 S3↔S3）
- `onChunkDone(bytes int64)` 每完成一个分块回调，可用于进度统计

使用方式：
```go
src, _ := objstore.New(srcCfg)
dst, _ := objstore.New(dstCfg)

if copier, ok := dst.(objstore.ServerCopier); ok {
    srcCopier := src.(objstore.ServerCopier)
    err := copier.CopyPartFrom(ctx, "dst/key", srcCopier, "src/key",
        totalSize, 8*1024*1024, 8, func(n int64) {
            log.Printf("copied %d bytes", n)
        })
}
```

### 6. 预签名 URL

| 方法 | COS | S3 | 说明 |
|---|:---:|:---:|---|
| `PresignGetObject(ctx, key, expires) (string, error)` | ✅ | ✅ | 生成 GET 对象的预签名 URL，调用方可直接通过该 URL 下载对象 |

实现细节：
- COS 使用 `cos-go-sdk-v5` 的 `Object.GetPresignedURL`，签名走 `SecretID/SecretKey`
- S3 使用 `aws-sdk-go-v2` 的 `s3.NewPresignClient` + `PresignGetObject`
- 可用于临时分发、跨服务下载、避免直接暴露密钥

```go
url, err := store.PresignGetObject(ctx, "path/to/object", 15*time.Minute)
if err != nil {
    log.Fatal(err)
}
log.Printf("download url: %s", url)
```

### 7. 删除

| 方法 | COS | S3 | 说明 |
|---|:---:|:---:|---|
| `DeleteObject(ctx, key) error` | ✅ | ✅ | 删除单个对象 |

### 8. 调试日志

通过环境变量或 API 开启详细操作日志（请求 URL、bucket、region、key 等）：

```bash
export OBJSTORE_DEBUG=true   # 或 COS_DEBUG=true
```

或代码中：
```go
objstore.SetDebug(true)
```

开启后会打印形如：
```
[objstore] DEBUG GetObject: URL=https://bkt.cos-internal.ap-guangzhou.tencentcos.cn/key, bucket=bkt, region=ap-guangzhou, key=key
[objstore] DEBUG [COS multipart] part 3/12 done
```

## 接口总览

```go
type Store interface {
    // 元信息
    HeadObject(ctx context.Context, key string) (int64, error)
    ListObjects(ctx context.Context, prefix string) ([]string, error)
    ListObjectsWithSize(ctx context.Context, prefix string) ([]ObjectInfo, error)

    // 下载
    GetObject(ctx context.Context, key string) (io.ReadCloser, error)
    GetRange(ctx context.Context, key string, start, end int64) ([]byte, error)
    GetAll(ctx context.Context, key string) ([]byte, error)

    // 上传
    PutObject(ctx context.Context, key string, data []byte) error
    PutObjectStream(ctx context.Context, key string, r io.Reader, size int64) error
    MultipartUpload(ctx context.Context, key string, totalSize, chunkSize int64,
        concurrency int, fetchPart func(int, int64, int64) ([]byte, error)) error

    // 预签名 URL
    PresignGetObject(ctx context.Context, key string, expires time.Duration) (string, error)

    // 删除
    DeleteObject(ctx context.Context, key string) error

    // 元数据
    BucketName() string
    Provider() ProviderType
}

type ServerCopier interface {
    CopyObject(ctx context.Context, dstKey string, src ServerCopier, srcKey string) error
    CopyPartFrom(ctx context.Context, dstKey string, src ServerCopier, srcKey string,
        totalSize, chunkSize int64, concurrency int, onChunkDone func(int64)) error
}
```

## 项目结构

```
objstore/
├── store.go    # 统一接口定义（Store / ServerCopier / ObjectInfo）
├── config.go   # Config 与工厂函数 New()
├── cos.go      # COS 实现（cos-go-sdk-v5）
├── s3.go       # S3 实现（aws-sdk-go-v2）
├── go.mod
└── go.sum
```

## License

私有项目，暂未开源许可。
