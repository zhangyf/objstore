# objstore

统一的对象存储客户端库，为腾讯云 COS 和 AWS S3（及 S3 兼容存储）提供一致的 Go 接口。

> Unified object storage client for COS and S3.

## 特性

- ✅ **统一接口**：`Store` 接口屏蔽 COS / S3 差异，一套代码同时支持两种后端
- ✅ **流式 I/O**：上传下载均支持流式读写，大文件不落盘、不占内存
- ✅ **并发分块上传**：内置 worker 池，支持自定义并发度和分块大小
- ✅ **服务端复制**：提供 `ServerCopier` 接口，跨桶/同桶复制走对象存储侧（不过本机带宽）
- ✅ **断点续传**：`MultipartResumer` 接口，合作 InitMultipart / ListParts / UploadPartN / CompleteMultipart / AbortMultipart 完成可恢复分块上传
- ✅ **孤儿扫描与 abort**：`MultipartLister.ListIncompleteUploads` 拿云端未完成 multipart uploads 全量列表（自动分页）。[v0.11.0]
- ✅ **对象属性透传**：`OptionalUploader` + `PutOptions` 透传 ContentType / CacheControl / Metadata / StorageClass / ACL / Tags。[v0.10.0]
- ✅ **内网优化**：COS 默认使用 `cos-internal.<region>.tencentcos.cn` 域名，走腾讯云内网链路
- ✅ **S3 兼容**：通过 `Endpoint` 字段可对接任意 S3 兼容存储（MinIO 等），自动启用 path-style
- ✅ **预签名 URL**：内置 GET / PUT 预签名，临时分发上传/下载无需暴露密钥
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
| `ListObjects(ctx, ListOptions) ([]ObjectInfo, error)` | ✅ | ✅ | 列出对象（含 Size），通过 `ListOptions` 控制分层/递归 |
| `BucketName() string` | ✅ | ✅ | 返回桶名（用于日志） |
| `Provider() ProviderType` | ✅ | ✅ | 返回存储类型 |

> `ListObjects` 使用 `ListOptions` 参数控制列表行为：
> - `Delimiter: "/"` → 只列出当前层（同 `ls`），自动翻页
> - `Delimiter: ""` → 递归列出前缀下所有文件（同 `du`），自动遍历所有子目录
> 
> COS 与 S3 均支持相同的语义。

> 内部自动循环翻页（COS 用 `Marker`，S3 用 `ContinuationToken`），单页 1000 条。

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
| `PresignGetObject(ctx, key, expires) (string, error)` | ✅ | ✅ | 生成 GET 预签名 URL，调用方可直接下载对象 |
| `PresignPutObject(ctx, key, expires) (string, error)` | ✅ | ✅ | 生成 PUT 预签名 URL，调用方可直接通过 HTTP PUT 上传对象 |

实现细节：
- COS 使用 `cos-go-sdk-v5` 的 `Object.GetPresignedURL`（method = GET / PUT），签名走 `SecretID/SecretKey`
- S3 使用 `aws-sdk-go-v2` 的 `s3.NewPresignClient` + `PresignGetObject` / `PresignPutObject`
- 适用于临时分发、跨服务上传/下载、避免直接暴露密钥

```go
// 下载预签名
getURL, err := store.PresignGetObject(ctx, "path/to/object", 15*time.Minute)

// 上传预签名
putURL, err := store.PresignPutObject(ctx, "path/to/object", 15*time.Minute)
// 客户端： curl -X PUT --upload-file local.bin "$putURL"
```

### 7. 删除

| 方法 | COS | S3 | 说明 |
|---|:---:|:---:|---|
| `DeleteObject(ctx, key) error` | ✅ | ✅ | 删除单个对象 |

### 8. 对象属性上传（OptionalUploader，v0.10.0）

调用方用类型断言检测，然后用带 `Opt` 后缀的方法透传 `PutOptions`。COS / S3 均实现。

```go
opts := &objstore.PutOptions{
    ContentType:  "text/html; charset=utf-8",
    CacheControl: "max-age=3600",
    Metadata:     map[string]string{"owner": "lingbo"},
    StorageClass: "STANDARD_IA",
    ACL:          "public-read",
    Tags:         map[string]string{"env": "prod", "team": "storage"},
}

if up, ok := store.(objstore.OptionalUploader); ok {
    _ = up.PutObjectOpt(ctx, "web/index.html", htmlBytes, opts)
}
```

覆盖上传的 4 条路径：`PutObjectOpt` / `PutObjectStreamOpt` / `InitMultipartOpt` / `MultipartUploadOpt`。跨厂商拷贝（S3↔COS）也会从源端读取 metadata 并透传。

> StorageClass / ACL 枚举本库不做本地校验（由上层决定），误传会在云端报 400。

### 9. 云端未完成 multipart uploads 扫描（MultipartLister，v0.11.0）

拿所指桶/前缀下云端保留的未完成 multipart uploads 全量列表（本函数自动处理分页）。与 `MultipartResumer.AbortMultipart` 配合可清理云端孤儿、释放计费。

```go
if lister, ok := store.(objstore.MultipartLister); ok {
    uploads, err := lister.ListIncompleteUploads(ctx, "data/")
    if err != nil {
        return err
    }
    fmt.Printf("未完成上传 %d 个\n", len(uploads))

    if resumer, ok := store.(objstore.MultipartResumer); ok {
        for _, u := range uploads {
            if time.Since(u.Initiated) > 24*time.Hour {  // 只清理超过 24h 的孤儿
                _ = resumer.AbortMultipart(ctx, u.Key, u.UploadID)
            }
        }
    }
}
```

### 10. 调试日志

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
type ListOptions struct {
    Prefix    string // 前缀路径
    Delimiter string // 分隔符，默认 "/"（当前层）；传 "" 则递归列出所有对象
}

type Store interface {
    // 元信息
    HeadObject(ctx context.Context, key string) (int64, error)
    ListObjects(ctx context.Context, opts ListOptions) ([]ObjectInfo, error)

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
    PresignPutObject(ctx context.Context, key string, expires time.Duration) (string, error)

    // 删除
    DeleteObject(ctx context.Context, key string) error

    // 元数据
    BucketName() string
    Provider() ProviderType
}

// 可选扩展接口：调用方用类型断言检测
func IsServerCopier(s Store) bool { _, ok := s.(ServerCopier); return ok }

type ServerCopier interface {
    CopyObject(ctx context.Context, dstKey string, src ServerCopier, srcKey string) error
    CopyPartFrom(ctx context.Context, dstKey string, src ServerCopier, srcKey string,
        totalSize, chunkSize int64, concurrency int, onChunkDone func(int64)) error
}

// MultipartResumer 可恢复的分块上传接口。
type MultipartResumer interface {
    InitMultipart(ctx context.Context, key string) (uploadID string, err error)
    ListParts(ctx context.Context, key, uploadID string) ([]UploadedPart, error)
    UploadPartN(ctx context.Context, key, uploadID string, partNumber int, data []byte) (etag string, err error)
    CompleteMultipart(ctx context.Context, key, uploadID string, parts []UploadedPart) error
    AbortMultipart(ctx context.Context, key, uploadID string) error
}

// MultipartLister 列出云端未完成 multipart uploads（v0.11.0）。
type IncompleteUpload struct {
    Key       string
    UploadID  string
    Initiated time.Time
}

type MultipartLister interface {
    ListIncompleteUploads(ctx context.Context, prefix string) ([]IncompleteUpload, error)
}

// OptionalUploader 带对象属性的上传（v0.10.0）。在 4 条上传路径上透传 PutOptions。
type PutOptions struct {
    ContentType   string            // 为空表示云端自动推断
    CacheControl  string
    Metadata      map[string]string // 进 x-amz-meta-* / x-cos-meta-*
    StorageClass  string            // STANDARD / STANDARD_IA / …按 provider 取值
    ACL           string            // canned ACL，按 provider 取值
    Tags          map[string]string // 进 x-amz-tagging / x-cos-tagging
}

func (o *PutOptions) HasAny() bool { /* 字段不为空则 true */ }

type OptionalUploader interface {
    PutObjectOpt(ctx context.Context, key string, data []byte, opts *PutOptions) error
    PutObjectStreamOpt(ctx context.Context, key string, r io.Reader, size int64, opts *PutOptions) error
    InitMultipartOpt(ctx context.Context, key string, opts *PutOptions) (uploadID string, err error)
    MultipartUploadOpt(ctx context.Context, key string, totalSize, chunkSize int64,
        concurrency int, fetchPart func(int, int64, int64) ([]byte, error), opts *PutOptions) error
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
