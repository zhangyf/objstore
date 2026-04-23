package objstore

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"sort"
	"sync"

	cos "github.com/tencentyun/cos-go-sdk-v5"
)


type cosStore struct {
	inner  *cos.Client
	bucket string
	region string
}

func newCOSStore(cfg Config) (Store, error) {
	u, err := url.Parse(fmt.Sprintf("https://%s.cos.%s.myqcloud.com", cfg.Bucket, cfg.Region))
	if err != nil {
		return nil, err
	}
	inner := cos.NewClient(&cos.BaseURL{BucketURL: u}, &http.Client{
		Transport: &cos.AuthorizationTransport{
			SecretID:  cfg.SecretID,
			SecretKey: cfg.SecretKey,
		},
	})
	return &cosStore{inner: inner, bucket: cfg.Bucket, region: cfg.Region}, nil
}

func (c *cosStore) Provider() ProviderType { return ProviderCOS }
func (c *cosStore) BucketName() string  { return c.bucket }

// ---- 元信息 ----

func (c *cosStore) HeadObject(ctx context.Context, key string) (int64, error) {
	resp, err := c.inner.Object.Head(ctx, key, nil)
	if err != nil {
		return 0, err
	}
	return resp.ContentLength, nil
}

func (c *cosStore) ListObjects(ctx context.Context, prefix string) ([]string, error) {
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
	resp, err := c.inner.Object.Get(ctx, key, nil)
	if err != nil {
		return nil, err
	}
	return resp.Body, nil
}

func (c *cosStore) GetRange(ctx context.Context, key string, start, end int64) ([]byte, error) {
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
	resp, err := c.inner.Object.Get(ctx, key, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	return io.ReadAll(resp.Body)
}

// ---- 上传 ----

func (c *cosStore) PutObject(ctx context.Context, key string, data []byte) error {
	_, err := c.inner.Object.Put(ctx, key, bytes.NewReader(data), nil)
	return err
}

func (c *cosStore) PutObjectStream(ctx context.Context, key string, r io.Reader, size int64) error {
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
				log.Printf("[COS multipart] part %d/%d done", pn, totalParts)
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

// ---- 其他 ----

func (c *cosStore) DeleteObject(ctx context.Context, key string) error {
	_, err := c.inner.Object.Delete(ctx, key)
	return err
}

func (c *cosStore) CopyObject(ctx context.Context, dstKey string, src ServerCopier, srcKey string) error {
	srcStore, ok := src.(*cosStore)
	if !ok {
		return fmt.Errorf("COS CopyObject: src must also be a COS store")
	}
	srcURL := fmt.Sprintf("%s.cos.%s.myqcloud.com/%s", srcStore.bucket, srcStore.region, srcKey)
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
	srcURL := fmt.Sprintf("%s.cos.%s.myqcloud.com/%s", srcStore.bucket, srcStore.region, srcKey)

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
				log.Printf("[COS CopyPart] part %d/%d done", pn, totalParts)
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
