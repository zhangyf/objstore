package objstore

import (
	"bytes"
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	cos "github.com/tencentyun/cos-go-sdk-v5"
)

type cosStore struct {
	inner     *cos.Client
	bucket    string
	region    string
	host      string // COS 域名后缀，如 cos.ap-tokyo.myqcloud.com（不含 bucket 前缀）
	secretID  string
	secretKey string
	ssec      *sseCustomerKey // SSE-C 客户密钥，nil 表示未启用
}

// cosHost 计算 COS 域名后缀（不含 bucket）。
// endpoint 为空时默认走公网 cos.<region>.myqcloud.com；
// 显式传入时可带 scheme（会被剔除）、尾部斜杠。
// 示例："cos-internal.ap-tokyo.tencentcos.cn" 或 "cos.ap-beijing.myqcloud.com"。
func cosHost(region, endpoint string) string {
	ep := strings.TrimSpace(endpoint)
	if ep == "" {
		return fmt.Sprintf("cos.%s.myqcloud.com", region)
	}
	ep = strings.TrimPrefix(ep, "https://")
	ep = strings.TrimPrefix(ep, "http://")
	ep = strings.TrimRight(ep, "/")
	return ep
}

// logOperation 记录操作日志，根据 Debug 标志决定是否输出
func (c *cosStore) logOperation(op, key string, extra ...string) {
	if !COSDebug {
		return // 调试模式关闭时不记录详细操作
	}

	// 生成完整 URL
	var fullURL string
	if key == "" && (op == "ListObjects" || op == "ListObjectsWithSize") {
		// 对于列表操作，不显示 key，但可以显示 base URL
		fullURL = fmt.Sprintf("https://%s.%s/", c.bucket, c.host)
	} else if key == "" {
		// 没有 key 的情况
		fullURL = fmt.Sprintf("https://%s.%s/", c.bucket, c.host)
	} else {
		// 有 key 的情况，构建完整对象 URL
		fullURL = fmt.Sprintf("https://%s.%s/%s", c.bucket, c.host, key)
	}

	msg := fmt.Sprintf("[objstore] DEBUG %s: URL=%s, bucket=%s, region=%s, key=%s", op, fullURL, c.bucket, c.region, key)
	if len(extra) > 0 {
		msg += ", " + strings.Join(extra, ", ")
	}
	if strings.HasPrefix(msg, "[objstore] DEBUG") && !COSDebug {
		return
	}
	log.Print(msg)
}

// COSDebug 控制是否开启详细的操作日志（URL、参数等）
var COSDebug = false

// SetDebug 设置调试模式
func SetDebug(debug bool) {
	COSDebug = debug
}

func init() {
	// 从环境变量初始化调试模式
	if os.Getenv("OBJSTORE_DEBUG") == "true" || os.Getenv("COS_DEBUG") == "true" {
		COSDebug = true
		log.Printf("[objstore] DEBUG 模式已启用")
	}
}

func newCOSStore(cfg Config) (Store, error) {
	host := cosHost(cfg.Region, cfg.Endpoint)
	u, err := url.Parse(fmt.Sprintf("https://%s.%s", cfg.Bucket, host))
	if err != nil {
		return nil, err
	}
	if COSDebug {
		log.Printf("[objstore] DEBUG New COS Client: endpoint=%s", u.String())
	}
	inner := cos.NewClient(&cos.BaseURL{BucketURL: u}, &http.Client{
		Transport: &cos.AuthorizationTransport{
			SecretID:  cfg.SecretID,
			SecretKey: cfg.SecretKey,
			Transport: &http.Transport{
				MaxIdleConns:        200,
				MaxIdleConnsPerHost: 100,
				IdleConnTimeout:     90 * time.Second,
			},
		},
	})
	var ssec *sseCustomerKey
	if len(cfg.SSECustomerKey) == 32 {
		ssec = newSSECustomerKey(cfg.SSECustomerKey)
	}
	return &cosStore{inner: inner, bucket: cfg.Bucket, region: cfg.Region, host: host, secretID: cfg.SecretID, secretKey: cfg.SecretKey, ssec: ssec}, nil
}

func (c *cosStore) Provider() ProviderType { return ProviderCOS }
func (c *cosStore) BucketName() string     { return c.bucket }

// ---- SSE-C header 注入辅助 ----

// applySSECGet 给 ObjectGetOptions 注入 SSE-C 三个 header（启用时）。
// 返回可能新建的 opt（入参为 nil 且启用 SSE-C 时）。
func (c *cosStore) applySSECGet(opt *cos.ObjectGetOptions) *cos.ObjectGetOptions {
	if c.ssec == nil {
		return opt
	}
	if opt == nil {
		opt = &cos.ObjectGetOptions{}
	}
	opt.XCosSSECustomerAglo = c.ssec.algo()
	opt.XCosSSECustomerKey = c.ssec.keyB64
	opt.XCosSSECustomerKeyMD5 = c.ssec.md5B64
	return opt
}

// applySSECHead 给 ObjectHeadOptions 注入 SSE-C header（启用时）。
func (c *cosStore) applySSECHead(opt *cos.ObjectHeadOptions) *cos.ObjectHeadOptions {
	if c.ssec == nil {
		return opt
	}
	if opt == nil {
		opt = &cos.ObjectHeadOptions{}
	}
	opt.XCosSSECustomerAglo = c.ssec.algo()
	opt.XCosSSECustomerKey = c.ssec.keyB64
	opt.XCosSSECustomerKeyMD5 = c.ssec.md5B64
	return opt
}

// applySSECPutHeader 给 ObjectPutHeaderOptions 注入 SSE-C header（启用时）。
// 用于单次上传 / multipart Init。
func (c *cosStore) applySSECPutHeader(h *cos.ObjectPutHeaderOptions) *cos.ObjectPutHeaderOptions {
	if c.ssec == nil {
		return h
	}
	if h == nil {
		h = &cos.ObjectPutHeaderOptions{}
	}
	h.XCosSSECustomerAglo = c.ssec.algo()
	h.XCosSSECustomerKey = c.ssec.keyB64
	h.XCosSSECustomerKeyMD5 = c.ssec.md5B64
	return h
}

// applySSECUploadPart 给 ObjectUploadPartOptions 注入 SSE-C header（启用时）。
func (c *cosStore) applySSECUploadPart(opt *cos.ObjectUploadPartOptions) *cos.ObjectUploadPartOptions {
	if c.ssec == nil {
		return opt
	}
	if opt == nil {
		opt = &cos.ObjectUploadPartOptions{}
	}
	opt.XCosSSECustomerAglo = c.ssec.algo()
	opt.XCosSSECustomerKey = c.ssec.keyB64
	opt.XCosSSECustomerKeyMD5 = c.ssec.md5B64
	return opt
}

// putOptWithSSEC 为上传构造 ObjectPutOptions，启用 SSE-C 时注入 header；
// 未启用时原样返回传入的 opt。
func (c *cosStore) putOptWithSSEC(opt *cos.ObjectPutOptions) *cos.ObjectPutOptions {
	if c.ssec == nil {
		return opt
	}
	if opt == nil {
		opt = &cos.ObjectPutOptions{}
	}
	opt.ObjectPutHeaderOptions = c.applySSECPutHeader(opt.ObjectPutHeaderOptions)
	return opt
}

// initOptWithSSEC 为 multipart Init 构造 InitiateMultipartUploadOptions，启用 SSE-C 时注入 header。
func (c *cosStore) initOptWithSSEC(opt *cos.InitiateMultipartUploadOptions) *cos.InitiateMultipartUploadOptions {
	if c.ssec == nil {
		return opt
	}
	if opt == nil {
		opt = &cos.InitiateMultipartUploadOptions{}
	}
	opt.ObjectPutHeaderOptions = c.applySSECPutHeader(opt.ObjectPutHeaderOptions)
	return opt
}

// copyOptWithSSEC 为服务端 Copy 构造 ObjectCopyOptions：
//   - 本 store（目标）启用 SSE-C 时，注入目标 SSE-C header；
//   - srcStore（源）启用 SSE-C 时，注入 copy-source SSE-C header。
// 两者均未启用时返回 nil。
func (c *cosStore) copyOptWithSSEC(srcStore *cosStore) *cos.ObjectCopyOptions {
	if c.ssec == nil && srcStore.ssec == nil {
		return nil
	}
	h := &cos.ObjectCopyHeaderOptions{}
	if c.ssec != nil {
		h.XCosSSECustomerAglo = c.ssec.algo()
		h.XCosSSECustomerKey = c.ssec.keyB64
		h.XCosSSECustomerKeyMD5 = c.ssec.md5B64
	}
	if srcStore.ssec != nil {
		h.XCosCopySourceSSECustomerAglo = srcStore.ssec.algo()
		h.XCosCopySourceSSECustomerKey = srcStore.ssec.keyB64
		h.XCosCopySourceSSECustomerKeyMD5 = srcStore.ssec.md5B64
	}
	return &cos.ObjectCopyOptions{ObjectCopyHeaderOptions: h}
}

// copyPartOptWithSSEC 为服务端 CopyPart 注入 SSE-C copy-source header（src 启用时）。
// 目标的 SSE-C 由 Init 决定（参见 initOptWithSSEC），这里只需处理源端。
func (c *cosStore) copyPartOptWithSSEC(srcStore *cosStore, opt *cos.ObjectCopyPartOptions) *cos.ObjectCopyPartOptions {
	if srcStore.ssec == nil {
		return opt
	}
	if opt == nil {
		opt = &cos.ObjectCopyPartOptions{}
	}
	opt.XCosCopySourceSSECustomerAglo = srcStore.ssec.algo()
	opt.XCosCopySourceSSECustomerKey = srcStore.ssec.keyB64
	opt.XCosCopySourceSSECustomerKeyMD5 = srcStore.ssec.md5B64
	return opt
}

// ---- BucketAdmin ----

// CreateBucket 创建当前桶（无选项），委托到 CreateBucketOpt(nil)。
func (c *cosStore) CreateBucket(ctx context.Context) error {
	return c.CreateBucketOpt(ctx, nil)
}

// CreateBucketOpt 创建当前桶，支持 OFS / MAZ / ACL / Tags 等选项。
// COS 的 CreateBucketConfiguration 中 BucketArchConfig / BucketAZConfig 分别映射 OFS / MAZ。
func (c *cosStore) CreateBucketOpt(ctx context.Context, opts *CreateBucketOptions) error {
	c.logOperation("CreateBucketOpt", "")
	var putOpt *cos.BucketPutOptions
	if opts.HasAny() {
		putOpt = &cos.BucketPutOptions{
			CreateBucketConfiguration: &cos.CreateBucketConfiguration{},
		}
		if opts.OFS {
			putOpt.CreateBucketConfiguration.BucketArchConfig = "OFS"
		}
		if opts.MAZ {
			putOpt.CreateBucketConfiguration.BucketAZConfig = "MAZ"
		}
		if opts.ACL != "" {
			putOpt.XCosACL = opts.ACL
		}
		if len(opts.Tags) > 0 {
			putOpt.XCosTagging = encodeTagging(opts.Tags)
		}
	}
	resp, err := c.inner.Bucket.Put(ctx, putOpt)
	if err == nil {
		return nil
	}
	// 409 在 COS 表示桶名已占用（可能是你刚创建过，也可能是别人占名），
	// 需额外 Head 一下来区分。
	if resp != nil && resp.StatusCode == http.StatusConflict {
		if headErr := c.HeadBucket(ctx); headErr == nil {
			return ErrBucketAlreadyOwnedByYou
		}
		// Head 失败 → 这个名字你不拥有，使用原始 error。
	}
	return err
}

// DeleteBucket 删除当前桶。要求桶为空。
func (c *cosStore) DeleteBucket(ctx context.Context) error {
	c.logOperation("DeleteBucket", "")
	_, err := c.inner.Bucket.Delete(ctx)
	return err
}

// HeadBucket 检查当前桶是否存在且可访问。
func (c *cosStore) HeadBucket(ctx context.Context) error {
	c.logOperation("HeadBucket", "")
	resp, err := c.inner.Bucket.Head(ctx)
	if err != nil {
		if resp != nil && resp.StatusCode == http.StatusNotFound {
			return ErrBucketNotFound
		}
		return err
	}
	return nil
}

// ---- 元信息 ----

func (c *cosStore) HeadObject(ctx context.Context, key string) (*ObjectInfo, error) {
	c.logOperation("HeadObject", key)
	resp, err := c.inner.Object.Head(ctx, key, c.applySSECHead(nil))
	if err != nil {
		return nil, err
	}
	info := &ObjectInfo{Key: key, Size: resp.ContentLength}
	if resp.Header != nil {
		if lmStr := resp.Header.Get("Last-Modified"); lmStr != "" {
			lm, _ := time.Parse(time.RFC1123, lmStr)
			info.LastModified = lm
		}
		info.ETag = strings.Trim(resp.Header.Get("ETag"), "\"")
		info.StorageClass = resp.Header.Get("x-cos-storage-class")
		info.ContentType = resp.Header.Get("Content-Type")
		info.ServerSideEncryption = resp.Header.Get("x-cos-server-side-encryption")
		info.SSEKMSKeyID = resp.Header.Get("x-cos-server-side-encryption-cos-kms-key-id")
		info.VersionID = resp.Header.Get("x-cos-version-id")
		// 提取 x-cos-meta-* 自定义元数据
		const metaPrefix = "X-Cos-Meta-"
		for name, vals := range resp.Header {
			if strings.HasPrefix(name, metaPrefix) && len(vals) > 0 {
				if info.Metadata == nil {
					info.Metadata = make(map[string]string)
				}
				info.Metadata[strings.ToLower(strings.TrimPrefix(name, metaPrefix))] = vals[0]
			}
		}
	}
	return info, nil
}

func (c *cosStore) ListObjects(ctx context.Context, opts ListOptions) ([]ObjectInfo, error) {
	c.logOperation("ListObjects", "", fmt.Sprintf("prefix=%s delimiter=%q", opts.Prefix, opts.Delimiter))

	if opts.Delimiter == "" {
		return c.listAllObjects(ctx, opts.Prefix, opts.ListConcurrency)
	}

	return c.listWithDelimiter(ctx, opts.Prefix, opts.Delimiter)
}

// listWithDelimiter 按 delimiter 分层列出对象
func (c *cosStore) listWithDelimiter(ctx context.Context, prefix string, delimiter string) ([]ObjectInfo, error) {
	var result []ObjectInfo
	marker := ""
	baseURL := fmt.Sprintf("https://%s.%s", c.bucket, c.host)

	for {
		u, err := url.Parse(baseURL + "/")
		if err != nil {
			return nil, fmt.Errorf("ListObjects: URL 构造失败: %w", err)
		}
		q := u.Query()
		q.Set("max-keys", "1000")
		q.Set("prefix", prefix)
		q.Set("delimiter", delimiter)
		if marker != "" {
			q.Set("marker", marker)
		}
		u.RawQuery = q.Encode()

		body, err := c.listHTTPGet(ctx, u.String())
		if err != nil {
			return nil, err
		}

		type bucketListResult struct {
			Contents []struct {
				Key          string `xml:"Key"`
				Size         int64  `xml:"Size"`
				ETag         string `xml:"ETag"`
				LastModified string `xml:"LastModified"`
				StorageClass string `xml:"StorageClass"`
			} `xml:"Contents"`
			CommonPrefixes []string `xml:"CommonPrefixes>Prefix"`
			IsTruncated    bool     `xml:"IsTruncated"`
			NextMarker     string   `xml:"NextMarker"`
		}

		var r bucketListResult
		if err := xml.Unmarshal(body, &r); err != nil {
			return nil, fmt.Errorf("ListObjects: 解析 XML 失败: %w", err)
		}

		for _, obj := range r.Contents {
			result = append(result, c.parseObjectInfo(obj.Key, obj.Size, obj.ETag, obj.LastModified, obj.StorageClass))
		}

		if !r.IsTruncated {
			break
		}
		marker = r.NextMarker
	}

	return result, nil
}

// listAllObjects 递归列出前缀下所有对象（遍历 CommonPrefixes）。
// concurrency <= 1 时串行；>1 时对顶层子前缀并行遍历，深层回退串行。
func (c *cosStore) listAllObjects(ctx context.Context, prefix string, concurrency int) ([]ObjectInfo, error) {
	var result []ObjectInfo
	var prefixes []string
	var err error

	// 先列出当前层（含对象和子目录前缀）
	prefixes, result, err = c.listWithCommonPrefixes(ctx, prefix)
	if err != nil {
		return nil, err
	}
	if len(prefixes) == 0 {
		return result, nil
	}

	// 串行路径
	if concurrency <= 1 {
		for _, subPrefix := range prefixes {
			subResult, err := c.listAllObjects(ctx, subPrefix, 0)
			if err != nil {
				return nil, err
			}
			result = append(result, subResult...)
		}
		return result, nil
	}

	// 并行路径：仅这一层并行，递归子层恢复串行（concurrency=0）
	sem := make(chan struct{}, concurrency)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var firstErr error

	for _, sp := range prefixes {
		wg.Add(1)
		sem <- struct{}{}
		go func(p string) {
			defer wg.Done()
			defer func() { <-sem }()
			sub, err := c.listAllObjects(ctx, p, 0)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				if firstErr == nil {
					firstErr = err
				}
				return
			}
			result = append(result, sub...)
		}(sp)
	}
	wg.Wait()
	if firstErr != nil {
		return nil, firstErr
	}
	return result, nil
}

// listWithCommonPrefixes 列出一层，返回该层的对象列表和子目录前缀列表
func (c *cosStore) listWithCommonPrefixes(ctx context.Context, prefix string) ([]string, []ObjectInfo, error) {
	var result []ObjectInfo
	var prefixes []string
	marker := ""
	baseURL := fmt.Sprintf("https://%s.%s", c.bucket, c.host)

	for {
		u, err := url.Parse(baseURL + "/")
		if err != nil {
			return nil, nil, fmt.Errorf("ListObjects: URL 构造失败: %w", err)
		}
		q := u.Query()
		q.Set("max-keys", "1000")
		q.Set("prefix", prefix)
		q.Set("delimiter", "/")
		if marker != "" {
			q.Set("marker", marker)
		}
		u.RawQuery = q.Encode()

		body, err := c.listHTTPGet(ctx, u.String())
		if err != nil {
			return nil, nil, err
		}

		type bucketListResult struct {
			Contents []struct {
				Key          string `xml:"Key"`
				Size         int64  `xml:"Size"`
				ETag         string `xml:"ETag"`
				LastModified string `xml:"LastModified"`
				StorageClass string `xml:"StorageClass"`
			} `xml:"Contents"`
			CommonPrefixes []string `xml:"CommonPrefixes>Prefix"`
			IsTruncated    bool     `xml:"IsTruncated"`
			NextMarker     string   `xml:"NextMarker"`
		}

		var r bucketListResult
		if err := xml.Unmarshal(body, &r); err != nil {
			return nil, nil, fmt.Errorf("ListObjects: 解析 XML 失败: %w", err)
		}

		for _, obj := range r.Contents {
			result = append(result, c.parseObjectInfo(obj.Key, obj.Size, obj.ETag, obj.LastModified, obj.StorageClass))
		}
		prefixes = append(prefixes, r.CommonPrefixes...)

		if !r.IsTruncated {
			break
		}
		marker = r.NextMarker
	}

	return prefixes, result, nil
}

// listHTTPGet 执行一次带签名的 HTTP GET 请求
func (c *cosStore) listHTTPGet(ctx context.Context, urlStr string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", urlStr, nil)
	if err != nil {
		return nil, fmt.Errorf("ListObjects: 创建请求失败: %w", err)
	}
	httpClient := &http.Client{
		Transport: &cos.AuthorizationTransport{
			SecretID:  c.secretID,
			SecretKey: c.secretKey,
		},
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("ListObjects: 请求失败: %w", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("ListObjects: 读取响应失败: %w", err)
	}
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("ListObjects: HTTP %d: prefix=%s", resp.StatusCode, urlStr)
	}
	return body, nil
}

func (c *cosStore) parseObjectInfo(key string, size int64, etag, lastModified, storageClass string) ObjectInfo {
	info := ObjectInfo{
		Key:  key,
		Size: size,
		ETag: strings.Trim(etag, "\""),
	}
	if lastModified != "" {
		// COS ListObjects 返回 ISO8601 (RFC3339)，HeadObject 返回 RFC1123
		for _, layout := range []string{time.RFC3339Nano, time.RFC3339, time.RFC1123} {
			if lm, err := time.Parse(layout, lastModified); err == nil {
				info.LastModified = lm
				break
			}
		}
	}
	info.StorageClass = storageClass
	return info
}

// ---- 下载 ----

func (c *cosStore) GetObject(ctx context.Context, key string) (io.ReadCloser, error) {
	c.logOperation("GetObject", key)
	resp, err := c.inner.Object.Get(ctx, key, c.applySSECGet(nil))
	if err != nil {
		return nil, err
	}
	return resp.Body, nil
}

func (c *cosStore) GetRange(ctx context.Context, key string, start, end int64) ([]byte, error) {
	c.logOperation("GetRange", key, fmt.Sprintf("range=%d-%d", start, end))
	resp, err := c.inner.Object.Get(ctx, key, c.applySSECGet(&cos.ObjectGetOptions{
		Range: fmt.Sprintf("bytes=%d-%d", start, end),
	}))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	return io.ReadAll(resp.Body)
}

func (c *cosStore) GetAll(ctx context.Context, key string) ([]byte, error) {
	c.logOperation("GetAll", key)
	resp, err := c.inner.Object.Get(ctx, key, c.applySSECGet(nil))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	return io.ReadAll(resp.Body)
}

// ---- 上传 ----

func (c *cosStore) PutObject(ctx context.Context, key string, data []byte) error {
	c.logOperation("PutObject", key, fmt.Sprintf("size=%d", len(data)))
	_, err := c.inner.Object.Put(ctx, key, bytes.NewReader(data), c.putOptWithSSEC(nil))
	return err
}

func (c *cosStore) PutObjectStream(ctx context.Context, key string, r io.Reader, size int64) error {
	c.logOperation("PutObjectStream", key, fmt.Sprintf("size=%d", size))
	opt := &cos.ObjectPutOptions{}
	if size >= 0 {
		opt.ObjectPutHeaderOptions = &cos.ObjectPutHeaderOptions{
			ContentLength: size,
		}
	}
	opt = c.putOptWithSSEC(opt)
	_, err := c.inner.Object.Put(ctx, key, r, opt)
	return err
}

// ---- 分块上传 ----

func (c *cosStore) MultipartUpload(ctx context.Context, key string, totalSize, chunkSize int64, concurrency int,
	fetchPart func(partNumber int, offset, size int64) ([]byte, error)) error {

	totalParts := int((totalSize + chunkSize - 1) / chunkSize)
	initResp, _, err := c.inner.Object.InitiateMultipartUpload(ctx, key, c.initOptWithSSEC(nil))
	if err != nil {
		return fmt.Errorf("InitiateMultipartUpload: %w", err)
	}
	uploadID := initResp.UploadID
	abort := func() { c.inner.Object.AbortMultipartUpload(ctx, key, uploadID) }

	type partResult struct {
		partNumber int
		etag       string
		err        error
	}
	jobs := make(chan int, concurrency*2)
	results := make(chan partResult, totalParts)
	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for pn := range jobs {
				offset := int64(pn-1) * chunkSize
				sz := chunkSize
				if offset+sz > totalSize {
					sz = totalSize - offset
				}
				data, err := fetchPart(pn, offset, sz)
				if err != nil {
					results <- partResult{pn, "", fmt.Errorf("fetchPart %d: %w", pn, err)}
					continue
				}
				resp, err := c.inner.Object.UploadPart(ctx, key, uploadID, pn, bytes.NewReader(data), c.applySSECUploadPart(nil))
				if err != nil {
					results <- partResult{pn, "", fmt.Errorf("UploadPart %d: %w", pn, err)}
					continue
				}
				if COSDebug {
					log.Printf("[objstore] DEBUG [COS multipart] part %d/%d done", pn, totalParts)
				}
				results <- partResult{pn, resp.Header.Get("ETag"), nil}
			}
		}()
	}
	go func() {
		for i := 1; i <= totalParts; i++ {
			jobs <- i
		}
		close(jobs)
	}()
	go func() { wg.Wait(); close(results) }()

	var parts []cos.Object
	for r := range results {
		if r.err != nil {
			abort()
			return r.err
		}
		parts = append(parts, cos.Object{PartNumber: r.partNumber, ETag: r.etag})
	}
	sort.Slice(parts, func(i, j int) bool { return parts[i].PartNumber < parts[j].PartNumber })
	_, _, err = c.inner.Object.CompleteMultipartUpload(ctx, key, uploadID,
		&cos.CompleteMultipartUploadOptions{Parts: parts})
	if err != nil {
		abort()
		return fmt.Errorf("CompleteMultipartUpload: %w", err)
	}
	return nil
}

// ---- 预签名 URL ----

func (c *cosStore) PresignGetObject(ctx context.Context, key string, expires time.Duration) (string, error) {
	c.logOperation("PresignGetObject", key, fmt.Sprintf("expires=%s", expires))
	u, err := c.inner.Object.GetPresignedURL(ctx, http.MethodGet, key, c.secretID, c.secretKey, expires, nil)
	if err != nil {
		return "", fmt.Errorf("cos PresignGetObject: %w", err)
	}
	return u.String(), nil
}

func (c *cosStore) PresignPutObject(ctx context.Context, key string, expires time.Duration) (string, error) {
	c.logOperation("PresignPutObject", key, fmt.Sprintf("expires=%s", expires))
	u, err := c.inner.Object.GetPresignedURL(ctx, http.MethodPut, key, c.secretID, c.secretKey, expires, nil)
	if err != nil {
		return "", fmt.Errorf("cos PresignPutObject: %w", err)
	}
	return u.String(), nil
}

// ---- 其他 ----

func (c *cosStore) DeleteObject(ctx context.Context, key string) error {
	c.logOperation("DeleteObject", key)
	_, err := c.inner.Object.Delete(ctx, key)
	return err
}

func (c *cosStore) CopyObject(ctx context.Context, dstKey string, src ServerCopier, srcKey string) error {
	srcStore, ok := src.(*cosStore)
	if !ok {
		return fmt.Errorf("COS CopyObject: src must also be a COS store")
	}
	srcURL := fmt.Sprintf("%s.%s/%s", srcStore.bucket, srcStore.host, srcKey)
	c.logOperation("CopyObject", dstKey, fmt.Sprintf("src=%s", srcURL))
	_, _, err := c.inner.Object.Copy(ctx, dstKey, srcURL, c.copyOptWithSSEC(srcStore))
	return err
}

// CopyPartFrom 使用服务端 UploadPart-Copy 跨桶/同桶复制大文件（不过本机带宽）
// srcStore 必须也是 cosStore；onChunkDone 每完成一个分块后回调已传输字节数
func (c *cosStore) CopyPartFrom(ctx context.Context, dstKey string, src ServerCopier, srcKey string,
	totalSize, chunkSize int64, concurrency int, onChunkDone func(int64)) error {

	srcStore, ok := src.(*cosStore)
	if !ok {
		return fmt.Errorf("COS CopyPartFrom: src must also be a COS store")
	}

	totalParts := int((totalSize + chunkSize - 1) / chunkSize)
	srcURL := fmt.Sprintf("%s.%s/%s", srcStore.bucket, srcStore.host, srcKey)
	c.logOperation("CopyPartFrom", dstKey, fmt.Sprintf("src=%s, totalSize=%d, chunks=%d", srcURL, totalSize, totalParts))

	initResp, _, err := c.inner.Object.InitiateMultipartUpload(ctx, dstKey, c.initOptWithSSEC(nil))
	if err != nil {
		return fmt.Errorf("InitiateMultipartUpload: %w", err)
	}
	uploadID := initResp.UploadID
	abort := func() { c.inner.Object.AbortMultipartUpload(ctx, dstKey, uploadID) }

	type partResult struct {
		partNumber int
		etag       string
		err        error
	}
	jobs := make(chan int, concurrency*2)
	results := make(chan partResult, totalParts)
	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for pn := range jobs {
				start := int64(pn-1) * chunkSize
				end := start + chunkSize - 1
				if end >= totalSize {
					end = totalSize - 1
				}
				resp, _, err := c.inner.Object.CopyPart(ctx, dstKey, uploadID, pn, srcURL,
					c.copyPartOptWithSSEC(srcStore, &cos.ObjectCopyPartOptions{
						XCosCopySource:      srcURL,
						XCosCopySourceRange: fmt.Sprintf("bytes=%d-%d", start, end),
					}))
				if err != nil {
					results <- partResult{pn, "", fmt.Errorf("CopyPart %d: %w", pn, err)}
					continue
				}
				if onChunkDone != nil {
					onChunkDone(end - start + 1)
				}
				if COSDebug {
					log.Printf("[objstore] DEBUG [COS CopyPart] part %d/%d done", pn, totalParts)
				}
				results <- partResult{pn, resp.ETag, nil}
			}
		}()
	}
	go func() {
		for i := 1; i <= totalParts; i++ {
			jobs <- i
		}
		close(jobs)
	}()
	go func() { wg.Wait(); close(results) }()

	var parts []cos.Object
	for r := range results {
		if r.err != nil {
			abort()
			return r.err
		}
		parts = append(parts, cos.Object{PartNumber: r.partNumber, ETag: r.etag})
	}
	sort.Slice(parts, func(i, j int) bool { return parts[i].PartNumber < parts[j].PartNumber })
	_, _, err = c.inner.Object.CompleteMultipartUpload(ctx, dstKey, uploadID,
		&cos.CompleteMultipartUploadOptions{Parts: parts})
	if err != nil {
		abort()
		return fmt.Errorf("CompleteMultipartUpload: %w", err)
	}
	return nil
}

// ============================================================
// MultipartResumer
// ============================================================

// InitMultipart 初始化一个分块上传，返回 uploadID
func (c *cosStore) InitMultipart(ctx context.Context, key string) (string, error) {
	c.logOperation("InitMultipart", key, "")
	resp, _, err := c.inner.Object.InitiateMultipartUpload(ctx, key, c.initOptWithSSEC(nil))
	if err != nil {
		return "", fmt.Errorf("cos InitMultipart: %w", err)
	}
	return resp.UploadID, nil
}

// ListParts 列出已上传的分块
func (c *cosStore) ListParts(ctx context.Context, key, uploadID string) ([]UploadedPart, error) {
	c.logOperation("ListParts", key, fmt.Sprintf("uploadID=%s", uploadID))

	var out []UploadedPart
	marker := ""
	for {
		opt := &cos.ObjectListPartsOptions{
			MaxParts:         "1000",
			PartNumberMarker: marker,
		}
		res, _, err := c.inner.Object.ListParts(ctx, key, uploadID, opt)
		if err != nil {
			return nil, fmt.Errorf("cos ListParts: %w", err)
		}
		for _, p := range res.Parts {
			sz, _ := strconv.ParseInt(fmt.Sprintf("%v", p.Size), 10, 64)
			// 去除可能的双引号，保持状态中 ETag 格式一致
			etag := strings.Trim(p.ETag, "\"")
			out = append(out, UploadedPart{
				PartNumber: p.PartNumber,
				ETag:       etag,
				Size:       sz,
			})
		}
		if !res.IsTruncated {
			break
		}
		marker = res.NextPartNumberMarker
	}
	return out, nil
}

// UploadPartN 上传单个分块，返回 ETag
func (c *cosStore) UploadPartN(ctx context.Context, key, uploadID string, partNumber int, data []byte) (string, error) {
	resp, err := c.inner.Object.UploadPart(ctx, key, uploadID, partNumber, bytes.NewReader(data), c.applySSECUploadPart(nil))
	if err != nil {
		return "", fmt.Errorf("cos UploadPart %d: %w", partNumber, err)
	}
	// COS HTTP Header 返回的 ETag 带双引号，ListParts 返回的不带
	// 统一不带引号版本，提交时再加
	etag := strings.Trim(resp.Header.Get("ETag"), "\"")
	return etag, nil
}

// CompleteMultipart 提交所有分块
func (c *cosStore) CompleteMultipart(ctx context.Context, key, uploadID string, parts []UploadedPart) error {
	c.logOperation("CompleteMultipart", key, fmt.Sprintf("uploadID=%s parts=%d", uploadID, len(parts)))

	cosParts := make([]cos.Object, 0, len(parts))
	for _, p := range parts {
		etag := p.ETag
		if !strings.HasPrefix(etag, "\"") {
			etag = "\"" + etag + "\""
		}
		cosParts = append(cosParts, cos.Object{PartNumber: p.PartNumber, ETag: etag})
	}
	sort.Slice(cosParts, func(i, j int) bool { return cosParts[i].PartNumber < cosParts[j].PartNumber })

	_, _, err := c.inner.Object.CompleteMultipartUpload(ctx, key, uploadID,
		&cos.CompleteMultipartUploadOptions{Parts: cosParts})
	if err != nil {
		return fmt.Errorf("cos CompleteMultipart: %w", err)
	}
	return nil
}

// AbortMultipart 取消分块上传
func (c *cosStore) AbortMultipart(ctx context.Context, key, uploadID string) error {
	c.logOperation("AbortMultipart", key, fmt.Sprintf("uploadID=%s", uploadID))
	_, err := c.inner.Object.AbortMultipartUpload(ctx, key, uploadID)
	if err != nil {
		return fmt.Errorf("cos AbortMultipart: %w", err)
	}
	return nil
}

// ListIncompleteUploads 列出云端未完成的 multipart uploads（自动分页）。
func (c *cosStore) ListIncompleteUploads(ctx context.Context, prefix string) ([]IncompleteUpload, error) {
	c.logOperation("ListIncompleteUploads", prefix, "")
	var out []IncompleteUpload
	var keyMarker, uploadIDMarker string
	for {
		opt := &cos.ListMultipartUploadsOptions{
			Prefix:         prefix,
			MaxUploads:     1000,
			KeyMarker:      keyMarker,
			UploadIDMarker: uploadIDMarker,
		}
		res, _, err := c.inner.Bucket.ListMultipartUploads(ctx, opt)
		if err != nil {
			return nil, fmt.Errorf("cos ListMultipartUploads: %w", err)
		}
		for _, u := range res.Uploads {
			t, _ := time.Parse(time.RFC3339, u.Initiated)
			out = append(out, IncompleteUpload{
				Key:       u.Key,
				UploadID:  u.UploadID,
				Initiated: t,
			})
		}
		if !res.IsTruncated {
			break
		}
		keyMarker = res.NextKeyMarker
		uploadIDMarker = res.NextUploadIDMarker
	}
	return out, nil
}

// ============================================================
// OptionalUploader 实现：带元数据的上传
// ============================================================

// buildCosPutHeader 根据 PutOptions 构造 COS put header
func buildCosPutHeader(opts *PutOptions, contentLength int64) *cos.ObjectPutHeaderOptions {
	if opts == nil && contentLength < 0 {
		return nil
	}
	h := &cos.ObjectPutHeaderOptions{}
	if contentLength >= 0 {
		h.ContentLength = contentLength
	}
	if opts != nil {
		if opts.ContentType != "" {
			h.ContentType = opts.ContentType
		}
		if opts.CacheControl != "" {
			h.CacheControl = opts.CacheControl
		}
		if opts.StorageClass != "" {
			h.XCosStorageClass = opts.StorageClass
		}
		if len(opts.Metadata) > 0 {
			meta := http.Header{}
			for k, v := range opts.Metadata {
				meta.Set("x-cos-meta-"+k, v)
			}
			h.XCosMetaXXX = &meta
		}
		// Tag 由 x-cos-tagging 透传，SDK 本身不暴露专用字段，走 XOptionHeader
		// SSE 同理，COS SDK 没有 SSE 专用字段，透传 x-cos-server-side-encryption。
		extra := http.Header{}
		if len(opts.Tags) > 0 {
			extra.Set("x-cos-tagging", encodeTagging(opts.Tags))
		}
		if opts.SSE != "" {
			extra.Set("x-cos-server-side-encryption", opts.SSE)
			if (opts.SSE == "cos/kms" || opts.SSE == "aws:kms") && opts.SSEKMSKeyID != "" {
				extra.Set("x-cos-server-side-encryption-cos-kms-key-id", opts.SSEKMSKeyID)
			}
		}
		if len(extra) > 0 {
			h.XOptionHeader = &extra
		}
	}
	return h
}

// buildCosACLHeader 根据 PutOptions.ACL 构造 COS ACLHeaderOptions。
func buildCosACLHeader(opts *PutOptions) *cos.ACLHeaderOptions {
	if opts == nil || opts.ACL == "" {
		return nil
	}
	return &cos.ACLHeaderOptions{XCosACL: opts.ACL}
}

// encodeTagging 将 tag map 编码为 URL query 格式（k1=v1&k2=v2）。
// S3 与 COS 的 Tagging header 都使用该格式，key/value 需预先 URL-encode。
func encodeTagging(tags map[string]string) string {
	values := url.Values{}
	for k, v := range tags {
		values.Set(k, v)
	}
	return values.Encode()
}

func (c *cosStore) PutObjectOpt(ctx context.Context, key string, data []byte, opts *PutOptions) error {
	c.logOperation("PutObjectOpt", key, fmt.Sprintf("size=%d, opts=%v", len(data), opts.HasAny()))
	var putOpt *cos.ObjectPutOptions
	if opts.HasAny() {
		putOpt = &cos.ObjectPutOptions{
			ACLHeaderOptions:       buildCosACLHeader(opts),
			ObjectPutHeaderOptions: buildCosPutHeader(opts, -1),
		}
	}
	putOpt = c.putOptWithSSEC(putOpt) // SSE-C 启用时注入（即使 opts 为空）
	_, err := c.inner.Object.Put(ctx, key, bytes.NewReader(data), putOpt)
	return err
}

func (c *cosStore) PutObjectStreamOpt(ctx context.Context, key string, r io.Reader, size int64, opts *PutOptions) error {
	c.logOperation("PutObjectStreamOpt", key, fmt.Sprintf("size=%d, opts=%v", size, opts.HasAny()))
	putOpt := &cos.ObjectPutOptions{
		ACLHeaderOptions:       buildCosACLHeader(opts),
		ObjectPutHeaderOptions: buildCosPutHeader(opts, size),
	}
	putOpt = c.putOptWithSSEC(putOpt)
	_, err := c.inner.Object.Put(ctx, key, r, putOpt)
	return err
}

func (c *cosStore) InitMultipartOpt(ctx context.Context, key string, opts *PutOptions) (string, error) {
	c.logOperation("InitMultipartOpt", key, fmt.Sprintf("opts=%v", opts.HasAny()))
	var initOpt *cos.InitiateMultipartUploadOptions
	if opts.HasAny() {
		initOpt = &cos.InitiateMultipartUploadOptions{
			ACLHeaderOptions:       buildCosACLHeader(opts),
			ObjectPutHeaderOptions: buildCosPutHeader(opts, -1),
		}
	}
	initOpt = c.initOptWithSSEC(initOpt) // SSE-C 启用时注入
	resp, _, err := c.inner.Object.InitiateMultipartUpload(ctx, key, initOpt)
	if err != nil {
		return "", fmt.Errorf("cos InitMultipartOpt: %w", err)
	}
	return resp.UploadID, nil
}

// MultipartUploadOpt 与 MultipartUpload 完全一致，只在 Init 时多设了 PutOptions。
// 其余分块上传/合并流程不变。
func (c *cosStore) MultipartUploadOpt(ctx context.Context, key string, totalSize, chunkSize int64, concurrency int,
	fetchPart func(partNumber int, offset, size int64) ([]byte, error), opts *PutOptions) error {

	totalParts := int((totalSize + chunkSize - 1) / chunkSize)
	var initOpt *cos.InitiateMultipartUploadOptions
	if opts.HasAny() {
		initOpt = &cos.InitiateMultipartUploadOptions{
			ACLHeaderOptions:       buildCosACLHeader(opts),
			ObjectPutHeaderOptions: buildCosPutHeader(opts, -1),
		}
	}
	initOpt = c.initOptWithSSEC(initOpt) // SSE-C 启用时注入
	initResp, _, err := c.inner.Object.InitiateMultipartUpload(ctx, key, initOpt)
	if err != nil {
		return fmt.Errorf("InitiateMultipartUpload: %w", err)
	}
	uploadID := initResp.UploadID
	abort := func() { c.inner.Object.AbortMultipartUpload(ctx, key, uploadID) }

	type partResult struct {
		partNumber int
		etag       string
		err        error
	}
	jobs := make(chan int, concurrency*2)
	results := make(chan partResult, totalParts)
	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for pn := range jobs {
				offset := int64(pn-1) * chunkSize
				sz := chunkSize
				if offset+sz > totalSize {
					sz = totalSize - offset
				}
				data, err := fetchPart(pn, offset, sz)
				if err != nil {
					results <- partResult{partNumber: pn, err: err}
					continue
				}
				resp, err := c.inner.Object.UploadPart(ctx, key, uploadID, pn, bytes.NewReader(data), c.applySSECUploadPart(nil))
				if err != nil {
					results <- partResult{partNumber: pn, err: err}
					continue
				}
				results <- partResult{partNumber: pn, etag: resp.Header.Get("ETag")}
			}
		}()
	}
	go func() {
		for pn := 1; pn <= totalParts; pn++ {
			jobs <- pn
		}
		close(jobs)
	}()
	go func() { wg.Wait(); close(results) }()

	parts := make([]cos.Object, 0, totalParts)
	for r := range results {
		if r.err != nil {
			abort()
			return fmt.Errorf("UploadPart %d: %w", r.partNumber, r.err)
		}
		parts = append(parts, cos.Object{PartNumber: r.partNumber, ETag: r.etag})
	}
	sort.Slice(parts, func(i, j int) bool { return parts[i].PartNumber < parts[j].PartNumber })
	_, _, err = c.inner.Object.CompleteMultipartUpload(ctx, key, uploadID, &cos.CompleteMultipartUploadOptions{Parts: parts})
	if err != nil {
		abort()
		return fmt.Errorf("CompleteMultipartUpload: %w", err)
	}
	return nil
}
