# Changelog

All notable changes to this project will be documented in this file.

格式基于 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/)，
版本遵循 [Semantic Versioning](https://semver.org/lang/zh-CN/)。

## [1.0.0] - 2026-07-20

### 版本策略声明

自 v1.0.0 起，`Store` 接口锁定，不再做破坏性变更。此后：
- **`Store` 接口**及其所有方法签名稳定，API 兼容。
- 新增能力通过**可选接口（Optional Interface）**扩展，调用方类型断言获取。
- 结构体字段新增遵循零值向后兼容原则（零值 = 旧行为）。

### Added

- `BucketAdminOpt` 可选接口：`CreateBucketOpt(ctx, opts)` 支持创建桶时传入 ACL/Tags/OFS/MAZ 等选项。
- `CreateBucketOptions` 结构体，含 `HasAny()` 辅助方法。
- `ListOptions.ListConcurrency` 字段：控制列表操作的子前缀并行度（0 = 串行，保持向后兼容）。
- COS `listAllObjects` 支持并行遍历子前缀（仅顶层并行，递归层串行）。

### Changed

- `CreateBucket`（COS / S3）：内部委托至 `CreateBucketOpt(ctx, nil)`，行为不变。

---

## [0.17.0] - 2026-07-18

### Added

- SSE-C（客户提供密钥）服务端加密支持。

---

## [0.16.0] - 2026-07-15

### Added

- COS endpoint 可配置，默认使用公网域名 `myqcloud.com`。

### ⚠️ Breaking

- COS 默认 endpoint 从 `cos-internal.<region>.tencentcos.cn`（内网）改为 `cos.<region>.myqcloud.com`（公网）。如需走内网链路，调用方需在 `Config.Endpoint` 显式指定。

---

## [0.15.1] - 2026-07-12

### Fixed

- COS `CreateBucket`：409 响应时增加 `HeadBucket` 探测，正确区分"桶已为你所有"（返回 `ErrBucketAlreadyOwnedByYou`）与"桶名被他人占用"（返回原始错误）。

---

## [0.15.0] - 2026-07-10

### Added

- `BucketAdmin` 可选接口：`CreateBucket` / `DeleteBucket` / `HeadBucket`（COS / S3 均实现）。
- `ErrBucketNotFound` / `ErrBucketAlreadyOwnedByYou` 哨兵错误。

---

## [0.14.0] - 2026-07-05

### Added

- `HeadObject` 返回值增加 `ContentType` / `SSE` / `Metadata` / `VersionID` 字段。

### ⚠️ Breaking

- `HeadObject` 返回类型从 `(int64, error)` 改为 `(*ObjectInfo, error)`。调用方需更新解构方式。

---

## [0.13.0] - 2026-07-01

### Added

- `PutOptions` 支持服务端加密（SSE）字段。

---

## [0.12.0] - 2026-06-28

### Added

- S3 支持 default credential chain、Profile、IAM role、STS。

---

## [0.11.0] - 2026-06-25

### Added

- `MultipartLister` 可选接口：`ListIncompleteUploads` 列出云端未完成 multipart uploads。

---

## [0.10.0] - 2026-06-22

### Added

- `PutOptions` 增加 ACL / Tags 字段。

---

## [0.9.3] - 2026-06-20

### Added

- `OptionalUploader` 接口 + `PutOptions`：支持透传 ContentType / CacheControl / Metadata / StorageClass。
- 4 条上传路径：`PutObjectOpt` / `PutObjectStreamOpt` / `InitMultipartOpt` / `MultipartUploadOpt`。

---

## [0.9.2] - 2026-06-18

### Fixed

- COS `ListParts` 去除返回 ETag 的引号，与 `UploadPartN` 保持一致。

---

## [0.9.1] - 2026-06-17

### Fixed

- `MultipartResumer` ETag 处理：状态去引号，提交时加引号。

---

## [0.9.0] - 2026-06-15

### Added

- `MultipartResumer` 可选接口：支持断点续传（InitMultipart / ListParts / UploadPartN / CompleteMultipart / AbortMultipart）。
- `HeadObject` 返回 `ObjectInfo` 结构体，增加 ETag / LastModified / StorageClass 字段。
- `ListObjects` 统一用 `ListOptions` 控制 delimiter 和递归遍历。

### ⚠️ Breaking

- `ListObjects` 签名从 `(ctx, prefix)` 改为 `(ctx, ListOptions)`。
- `HeadObject` 返回类型从 `(int64, error)` 改为 `(ObjectInfo, error)`。

---

## [0.8.0] - 2026-06-10

### Added

- `PresignGetObject` / `PresignPutObject`：GET / PUT 预签名 URL。
- 调试日志：通过 `OBJSTORE_DEBUG` 环境变量或 `SetDebug()` 开启。
- COS 默认使用内网域名 `cos-internal.<region>.tencentcos.cn`。
- COS HTTP Transport 连接池优化：`MaxIdleConnsPerHost=100`。

---

## [0.7.0] - 2026-06-05

### Added

- `ServerCopier.CopyObject`：小文件服务端复制。

### ⚠️ Breaking

- `CopyObject` 从 `Store` 接口移除。使用方式改为类型断言 `ServerCopier` 后调用。

---

## [0.6.0] - 2026-06-02

### ⚠️ Breaking

- `COSCopier` 重命名为 `ServerCopier`。
- S3 的 `CopyPartFrom` 改为走 `UploadPartCopy`。

---

## [0.5.0] - 2026-05-30

### Changed

- `COSCopier` 接口合并至 `cos.go`，移除 `cos_copier.go`。

---

## [0.4.0] - 2026-05-28

### Changed

- `COSCopier` 接口定义优化；`config.go` 精简为 `Config + New()` 工厂模式。

---

## [0.3.0] - 2026-05-25

### Added

- `ProviderType` 枚举，`IsCOSStore` 辅助函数。

### ⚠️ Breaking

- 移除顶层 `CopyPartFrom` 包级函数。`Provider()` 从返回 string 改为 `ProviderType`。

---

## [0.2.0] - 2026-05-22

### Added

- `CopyPartFrom` 包级函数。
- 导出 `Provider()` 方法。

---

## [0.1.0] - 2026-05-20

### Added

- 初始版本：统一 COS / S3 对象存储接口。
- `Store` 接口基础方法（HeadObject / GetObject / PutObject / DeleteObject / ListObjects）。
- COS 与 S3 实现。
