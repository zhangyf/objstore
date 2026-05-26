package objstore

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	cos "github.com/tencentyun/cos-go-sdk-v5"
)


type cosStore struct {
	inner     *cos.Client
	bucket    string
	region    string
	secretID  string
	secretKey string
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
		fullURL = fmt.Sprintf("https://%s.cos-internal.%s.tencentcos.cn/", c.bucket, c.region)
	} else if key == "" {
		// 没有 key 的情况
		fullURL = fmt.Sprintf("https://%s.cos-internal.%s.tencentcos.cn/", c.bucket, c.region)
	} else {
		// 有 key 的情况，构建完整对象 URL
		fullURL = fmt.Sprintf("https://%s.cos-internal.%s.tencentcos.cn/%s", c.bucket, c.region, key)
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
	u, err := url.Parse(fmt.Sprintf("https://%s.cos-internal.%s.tencentcos.cn", cfg.Bucket, cfg.Region))
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
	return &cosStore{inner: inner, bucket: cfg.Bucket, region: cfg.Region, secretID: cfg.SecretID, secretKey: cfg.SecretKey}, nil
}

func (c *cosStore) Provider() ProviderType { return ProviderCOS }
func (c *cosStore) BucketName() string  { return c.bucket }

// ---- 元信息 ----

func (c *cosStore) HeadObject(ctx context.Context, key string) (int64, error) {
	c.logOperation("HeadObject", key)
	resp, err := c.inner.Object.Head(ctx, key, nil)
	if err != nil {
		return 0, err
	}
	return resp.ContentLength, nil
}

func (c *cosStore) ListObjects(ctx context.Context, prefix string) ([]string, error) {
	c.logOperation("ListObjects", "", "prefix="+prefix)
	infos, err := c.ListObjectsWithSize(ctx, prefix)
	if err != nil {
		return nil, err
	}
	keys := make([]string, len(infos))
	for i, o := range infos {
		keys[i] = o.Key
	}
	return keys, nil
}

func (c *cosStore) ListObjectsWithSize(ctx context.Context, prefix string) ([]ObjectInfo, error) {
	c.logOperation("ListObjectsWithSize", "", "prefix="+prefix)
	var result []ObjectInfo
	marker := ""
	for {
		resp, _, err := c.inner.Bucket.Get(ctx, &cos.BucketGetOptions{
			Prefix:  prefix,
			Marker:  marker,
			MaxKeys: 1000,
		})
		if err != nil {
			return nil, fmt.Errorf("cos ListObjects: %w", err)
		}
		for _, obj := range resp.Contents {
			result = append(result, ObjectInfo{Key: obj.Key, Size: obj.Size})
		}
		if !resp.IsTruncated {
			break
		}
		marker = resp.NextMarker
	}
	return result, nil
}

// ---- 下载 ----

func (c *cosStore) GetObject(ctx context.Context, key string) (io.ReadCloser, error) {
	c.logOperation("GetObject", key)
	resp, err := c.inner.Object.Get(ctx, key, nil)
	if err != nil {
		return nil, err
	}
	return resp.Body, nil
}

func (c *cosStore) GetRange(ctx context.Context, key string, start, end int64) ([]byte, error) {
	c.logOperation("GetRange", key, fmt.Sprintf("range=%d-%d", start, end))
	resp, err := c.inner.Object.Get(ctx, key, &cos.ObjectGetOptions{
		Range: fmt.Sprintf("bytes=%d-%d", start, end),
	})
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	return io.ReadAll(resp.Body)
}

func (c *cosStore) GetAll(ctx context.Context, key string) ([]byte, error) {
	c.logOperation("GetAll", key)
	resp, err := c.inner.Object.Get(ctx, key, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	return io.ReadAll(resp.Body)
}

// ---- 上传 ----

func (c *cosStore) PutObject(ctx context.Context, key string, data []byte) error {
	c.logOperation("PutObject", key, fmt.Sprintf("size=%d", len(data)))
	_, err := c.inner.Object.Put(ctx, key, bytes.NewReader(data), nil)
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
	_, err := c.inner.Object.Put(ctx, key, r, opt)
	return err
}

// ---- 分块上传 ----

func (c *cosStore) MultipartUpload(ctx context.Context, key string, totalSize, chunkSize int64, concurrency int,
	fetchPart func(partNumber int, offset, size int64) ([]byte, error)) error {

	totalParts := int((totalSize + chunkSize - 1) / chunkSize)
	initResp, _, err := c.inner.Object.InitiateMultipartUpload(ctx, key, nil)
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
				resp, err := c.inner.Object.UploadPart(ctx, key, uploadID, pn, bytes.NewReader(data), nil)
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
	srcURL := fmt.Sprintf("%s.cos-internal.%s.tencentcos.cn/%s", srcStore.bucket, srcStore.region, srcKey)
	c.logOperation("CopyObject", dstKey, fmt.Sprintf("src=%s", srcURL))
	_, _, err := c.inner.Object.Copy(ctx, dstKey, srcURL, nil)
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
	srcURL := fmt.Sprintf("%s.cos-internal.%s.tencentcos.cn/%s", srcStore.bucket, srcStore.region, srcKey)
	c.logOperation("CopyPartFrom", dstKey, fmt.Sprintf("src=%s, totalSize=%d, chunks=%d", srcURL, totalSize, totalParts))

	initResp, _, err := c.inner.Object.InitiateMultipartUpload(ctx, dstKey, nil)
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
					&cos.ObjectCopyPartOptions{
						XCosCopySource:      srcURL,
						XCosCopySourceRange: fmt.Sprintf("bytes=%d-%d", start, end),
					})
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
