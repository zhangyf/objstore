package objstore

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type s3Store struct {
	inner  *s3.Client
	bucket string
	region string
}

func newS3Store(cfg Config) (Store, error) {
	awsCfg, err := awsconfig.LoadDefaultConfig(context.Background(),
		awsconfig.WithRegion(cfg.Region),
		awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.SecretID, cfg.SecretKey, ""),
		),
		awsconfig.WithClientLogMode(0),
	)
	if err != nil {
		return nil, fmt.Errorf("s3 config: %w", err)
	}
	var opts []func(*s3.Options)
	if cfg.Endpoint != "" {
		ep := cfg.Endpoint
		opts = append(opts, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(ep)
			o.UsePathStyle = true
		})
	}
	return &s3Store{
		inner:  s3.NewFromConfig(awsCfg, opts...),
		bucket: cfg.Bucket,
		region: cfg.Region,
	}, nil
}

func (s *s3Store) Provider() ProviderType { return ProviderS3 }
func (s *s3Store) BucketName() string { return s.bucket }

// ---- 元信息 ----

func (s *s3Store) HeadObject(ctx context.Context, key string) (*ObjectInfo, error) {
	resp, err := s.inner.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, err
	}
	info := &ObjectInfo{Key: key}
	if resp.ContentLength != nil {
		info.Size = *resp.ContentLength
	}
	if resp.LastModified != nil {
		info.LastModified = *resp.LastModified
	}
	info.ETag = aws.ToString(resp.ETag)
	info.StorageClass = string(resp.StorageClass)
	return info, nil
}

func (s *s3Store) ListObjects(ctx context.Context, opts ListOptions) ([]ObjectInfo, error) {
	var result []ObjectInfo
	var token *string

	delimiter := aws.String("/")
	if opts.Delimiter == "" {
		delimiter = nil
	}

	for {
		resp, err := s.inner.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(s.bucket),
			Prefix:            aws.String(opts.Prefix),
			Delimiter:         delimiter,
			ContinuationToken: token,
		})
		if err != nil {
			return nil, fmt.Errorf("s3 ListObjectsV2: %w", err)
		}
		for _, obj := range resp.Contents {
			sz := int64(0)
			if obj.Size != nil {
				sz = *obj.Size
			}
			lm := time.Time{}
			if obj.LastModified != nil {
				lm = *obj.LastModified
			}
			result = append(result, ObjectInfo{
				Key:          aws.ToString(obj.Key),
				Size:         sz,
				LastModified: lm,
				ETag:         aws.ToString(obj.ETag),
				StorageClass: string(obj.StorageClass),
			})
		}
		if !aws.ToBool(resp.IsTruncated) {
			break
		}
		token = resp.NextContinuationToken
	}
	return result, nil
}

// ---- 下载 ----

func (s *s3Store) GetObject(ctx context.Context, key string) (io.ReadCloser, error) {
	resp, err := s.inner.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, err
	}
	return resp.Body, nil
}

func (s *s3Store) GetRange(ctx context.Context, key string, start, end int64) ([]byte, error) {
	resp, err := s.inner.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
		Range:  aws.String(fmt.Sprintf("bytes=%d-%d", start, end)),
	})
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	return io.ReadAll(resp.Body)
}

func (s *s3Store) GetAll(ctx context.Context, key string) ([]byte, error) {
	resp, err := s.inner.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	return io.ReadAll(resp.Body)
}

// ---- 上传 ----

func (s *s3Store) PutObject(ctx context.Context, key string, data []byte) error {
	_, err := s.inner.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(data),
	})
	return err
}

func (s *s3Store) PutObjectStream(ctx context.Context, key string, r io.Reader, size int64) error {
	input := &s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
		Body:   r,
	}
	if size >= 0 {
		input.ContentLength = aws.Int64(size)
	}
	_, err := s.inner.PutObject(ctx, input)
	return err
}

// ---- 分块上传 ----

func (s *s3Store) MultipartUpload(ctx context.Context, key string, totalSize, chunkSize int64, concurrency int,
	fetchPart func(partNumber int, offset, size int64) ([]byte, error)) error {

	totalParts := int((totalSize + chunkSize - 1) / chunkSize)
	createResp, err := s.inner.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return fmt.Errorf("CreateMultipartUpload: %w", err)
	}
	uploadID := aws.ToString(createResp.UploadId)
	abort := func() {
		s.inner.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
			Bucket: aws.String(s.bucket), Key: aws.String(key), UploadId: aws.String(uploadID),
		})
	}

	type partResult struct {
		pn   int
		etag string
		err  error
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
				resp, err := s.inner.UploadPart(ctx, &s3.UploadPartInput{
					Bucket:     aws.String(s.bucket),
					Key:        aws.String(key),
					UploadId:   aws.String(uploadID),
					PartNumber: aws.Int32(int32(pn)),
					Body:       bytes.NewReader(data),
				})
				if err != nil {
					results <- partResult{pn, "", fmt.Errorf("UploadPart %d: %w", pn, err)}
					continue
				}
				log.Printf("[S3 multipart] part %d/%d done", pn, totalParts)
				results <- partResult{pn, aws.ToString(resp.ETag), nil}
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

	type pr struct {
		pn   int
		etag string
	}
	var parts []pr
	for r := range results {
		if r.err != nil {
			abort()
			return r.err
		}
		parts = append(parts, pr{r.pn, r.etag})
	}
	sort.Slice(parts, func(i, j int) bool { return parts[i].pn < parts[j].pn })

	s3Parts := make([]s3types.CompletedPart, len(parts))
	for i, p := range parts {
		s3Parts[i] = s3types.CompletedPart{
			PartNumber: aws.Int32(int32(p.pn)),
			ETag:       aws.String(p.etag),
		}
	}
	_, err = s.inner.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:          aws.String(s.bucket),
		Key:             aws.String(key),
		UploadId:        aws.String(uploadID),
		MultipartUpload: &s3types.CompletedMultipartUpload{Parts: s3Parts},
	})
	if err != nil {
		abort()
		return fmt.Errorf("CompleteMultipartUpload: %w", err)
	}
	return nil
}

// ---- 预签名 URL ----

func (s *s3Store) PresignGetObject(ctx context.Context, key string, expires time.Duration) (string, error) {
	presign := s3.NewPresignClient(s.inner)
	req, err := presign.PresignGetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	}, func(po *s3.PresignOptions) {
		po.Expires = expires
	})
	if err != nil {
		return "", fmt.Errorf("s3 PresignGetObject: %w", err)
	}
	return req.URL, nil
}

func (s *s3Store) PresignPutObject(ctx context.Context, key string, expires time.Duration) (string, error) {
	presign := s3.NewPresignClient(s.inner)
	req, err := presign.PresignPutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	}, func(po *s3.PresignOptions) {
		po.Expires = expires
	})
	if err != nil {
		return "", fmt.Errorf("s3 PresignPutObject: %w", err)
	}
	return req.URL, nil
}

// ---- 其他 ----

func (s *s3Store) DeleteObject(ctx context.Context, key string) error {
	_, err := s.inner.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	return err
}

func (s *s3Store) CopyObject(ctx context.Context, dstKey string, src ServerCopier, srcKey string) error {
	srcStore, ok := src.(*s3Store)
	if !ok {
		return fmt.Errorf("S3 CopyObject: src must also be an S3 store")
	}
	_, err := s.inner.CopyObject(ctx, &s3.CopyObjectInput{
		Bucket:     aws.String(s.bucket),
		Key:        aws.String(dstKey),
		CopySource: aws.String(srcStore.bucket + "/" + srcKey),
	})
	return err
}

// CopyPartFrom 使用服务端 UploadPartCopy 跨桶/同桶复制大文件（不过本机带宽）。
func (s *s3Store) CopyPartFrom(ctx context.Context, dstKey string, src ServerCopier,
	srcKey string, totalSize, chunkSize int64, concurrency int, onChunkDone func(int64)) error {

	srcS3, ok := src.(*s3Store)
	if !ok {
		return fmt.Errorf("S3 CopyPartFrom: src must also be an S3 store")
	}

	totalParts := int((totalSize + chunkSize - 1) / chunkSize)
	copySource := fmt.Sprintf("%s/%s", srcS3.bucket, srcKey)

	createResp, err := s.inner.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(dstKey),
	})
	if err != nil {
		return fmt.Errorf("S3 CreateMultipartUpload: %w", err)
	}
	uploadID := aws.ToString(createResp.UploadId)
	abort := func() {
		s.inner.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
			Bucket: aws.String(s.bucket), Key: aws.String(dstKey), UploadId: aws.String(uploadID),
		})
	}

	type partResult struct {
		pn   int
		etag string
		err  error
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
				resp, err := s.inner.UploadPartCopy(ctx, &s3.UploadPartCopyInput{
					Bucket:          aws.String(s.bucket),
					Key:             aws.String(dstKey),
					UploadId:        aws.String(uploadID),
					PartNumber:      aws.Int32(int32(pn)),
					CopySource:      aws.String(copySource),
					CopySourceRange: aws.String(fmt.Sprintf("bytes=%d-%d", start, end)),
				})
				if err != nil {
					results <- partResult{pn, "", fmt.Errorf("UploadPartCopy %d: %w", pn, err)}
					continue
				}
				if onChunkDone != nil {
					onChunkDone(end - start + 1)
				}
				log.Printf("[S3 CopyPart] part %d/%d done", pn, totalParts)
				results <- partResult{pn, aws.ToString(resp.CopyPartResult.ETag), nil}
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

	var parts []s3types.CompletedPart
	for r := range results {
		if r.err != nil {
			abort()
			return r.err
		}
		parts = append(parts, s3types.CompletedPart{
			PartNumber: aws.Int32(int32(r.pn)),
			ETag:       aws.String(r.etag),
		})
	}
	sort.Slice(parts, func(i, j int) bool {
		return aws.ToInt32(parts[i].PartNumber) < aws.ToInt32(parts[j].PartNumber)
	})
	_, err = s.inner.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:          aws.String(s.bucket),
		Key:             aws.String(dstKey),
		UploadId:        aws.String(uploadID),
		MultipartUpload: &s3types.CompletedMultipartUpload{Parts: parts},
	})
	if err != nil {
		abort()
		return fmt.Errorf("S3 CompleteMultipartUpload: %w", err)
	}
	return nil
}

// ============================================================
// MultipartResumer
// ============================================================

// InitMultipart 初始化一个分块上传，返回 uploadID
func (s *s3Store) InitMultipart(ctx context.Context, key string) (string, error) {
	resp, err := s.inner.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return "", fmt.Errorf("s3 InitMultipart: %w", err)
	}
	return aws.ToString(resp.UploadId), nil
}

// ListParts 列出已上传的分块
func (s *s3Store) ListParts(ctx context.Context, key, uploadID string) ([]UploadedPart, error) {
	var out []UploadedPart
	var marker *string
	for {
		input := &s3.ListPartsInput{
			Bucket:           aws.String(s.bucket),
			Key:              aws.String(key),
			UploadId:         aws.String(uploadID),
			PartNumberMarker: marker,
			MaxParts:         aws.Int32(1000),
		}
		resp, err := s.inner.ListParts(ctx, input)
		if err != nil {
			return nil, fmt.Errorf("s3 ListParts: %w", err)
		}
		for _, p := range resp.Parts {
			pn := int(aws.ToInt32(p.PartNumber))
			sz := aws.ToInt64(p.Size)
			out = append(out, UploadedPart{
				PartNumber: pn,
				ETag:       aws.ToString(p.ETag),
				Size:       sz,
			})
		}
		if !aws.ToBool(resp.IsTruncated) {
			break
		}
		marker = resp.NextPartNumberMarker
	}
	return out, nil
}

// UploadPartN 上传单个分块，返回 ETag
func (s *s3Store) UploadPartN(ctx context.Context, key, uploadID string, partNumber int, data []byte) (string, error) {
	resp, err := s.inner.UploadPart(ctx, &s3.UploadPartInput{
		Bucket:        aws.String(s.bucket),
		Key:           aws.String(key),
		UploadId:      aws.String(uploadID),
		PartNumber:    aws.Int32(int32(partNumber)),
		Body:          bytes.NewReader(data),
		ContentLength: aws.Int64(int64(len(data))),
	})
	if err != nil {
		return "", fmt.Errorf("s3 UploadPart %d: %w", partNumber, err)
	}
	// 与 cos 一致：存状态不带引号
	etag := strings.Trim(aws.ToString(resp.ETag), "\"")
	return etag, nil
}

// CompleteMultipart 提交所有分块
func (s *s3Store) CompleteMultipart(ctx context.Context, key, uploadID string, parts []UploadedPart) error {
	completed := make([]s3types.CompletedPart, 0, len(parts))
	for _, p := range parts {
		// S3 提交时需要带引号的 ETag
		etag := p.ETag
		if !strings.HasPrefix(etag, "\"") {
			etag = "\"" + etag + "\""
		}
		completed = append(completed, s3types.CompletedPart{
			PartNumber: aws.Int32(int32(p.PartNumber)),
			ETag:       aws.String(etag),
		})
	}
	sort.Slice(completed, func(i, j int) bool {
		return aws.ToInt32(completed[i].PartNumber) < aws.ToInt32(completed[j].PartNumber)
	})
	_, err := s.inner.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:          aws.String(s.bucket),
		Key:             aws.String(key),
		UploadId:        aws.String(uploadID),
		MultipartUpload: &s3types.CompletedMultipartUpload{Parts: completed},
	})
	if err != nil {
		return fmt.Errorf("s3 CompleteMultipart: %w", err)
	}
	return nil
}

// AbortMultipart 取消分块上传
func (s *s3Store) AbortMultipart(ctx context.Context, key, uploadID string) error {
	_, err := s.inner.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
		Bucket:   aws.String(s.bucket),
		Key:      aws.String(key),
		UploadId: aws.String(uploadID),
	})
	if err != nil {
		return fmt.Errorf("s3 AbortMultipart: %w", err)
	}
	return nil
}

// ============================================================
// OptionalUploader 实现：带元数据的上传
// ============================================================

// applyS3PutOptions 把 PutOptions 写入 PutObjectInput
func applyS3PutOptions(input *s3.PutObjectInput, opts *PutOptions) {
	if !opts.HasAny() {
		return
	}
	if opts.ContentType != "" {
		input.ContentType = aws.String(opts.ContentType)
	}
	if opts.CacheControl != "" {
		input.CacheControl = aws.String(opts.CacheControl)
	}
	if opts.StorageClass != "" {
		input.StorageClass = s3types.StorageClass(opts.StorageClass)
	}
	if len(opts.Metadata) > 0 {
		m := make(map[string]string, len(opts.Metadata))
		for k, v := range opts.Metadata {
			m[k] = v
		}
		input.Metadata = m
	}
}

// applyS3CreateMultipart 把 PutOptions 写入 CreateMultipartUploadInput
func applyS3CreateMultipart(input *s3.CreateMultipartUploadInput, opts *PutOptions) {
	if !opts.HasAny() {
		return
	}
	if opts.ContentType != "" {
		input.ContentType = aws.String(opts.ContentType)
	}
	if opts.CacheControl != "" {
		input.CacheControl = aws.String(opts.CacheControl)
	}
	if opts.StorageClass != "" {
		input.StorageClass = s3types.StorageClass(opts.StorageClass)
	}
	if len(opts.Metadata) > 0 {
		m := make(map[string]string, len(opts.Metadata))
		for k, v := range opts.Metadata {
			m[k] = v
		}
		input.Metadata = m
	}
}

func (s *s3Store) PutObjectOpt(ctx context.Context, key string, data []byte, opts *PutOptions) error {
	input := &s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(data),
	}
	applyS3PutOptions(input, opts)
	_, err := s.inner.PutObject(ctx, input)
	return err
}

func (s *s3Store) PutObjectStreamOpt(ctx context.Context, key string, r io.Reader, size int64, opts *PutOptions) error {
	input := &s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
		Body:   r,
	}
	if size >= 0 {
		input.ContentLength = aws.Int64(size)
	}
	applyS3PutOptions(input, opts)
	_, err := s.inner.PutObject(ctx, input)
	return err
}

func (s *s3Store) InitMultipartOpt(ctx context.Context, key string, opts *PutOptions) (string, error) {
	input := &s3.CreateMultipartUploadInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	}
	applyS3CreateMultipart(input, opts)
	resp, err := s.inner.CreateMultipartUpload(ctx, input)
	if err != nil {
		return "", fmt.Errorf("s3 InitMultipartOpt: %w", err)
	}
	return aws.ToString(resp.UploadId), nil
}

// MultipartUploadOpt 与 MultipartUpload 完全一致，只在 Init 时多设了 PutOptions。
func (s *s3Store) MultipartUploadOpt(ctx context.Context, key string, totalSize, chunkSize int64, concurrency int,
	fetchPart func(partNumber int, offset, size int64) ([]byte, error), opts *PutOptions) error {

	if chunkSize < 5*1024*1024 && totalSize > chunkSize {
		return fmt.Errorf("S3 multipart 上传 chunk 必须 ≥ 5MB（当前 -chunk=%d）", chunkSize/1024/1024)
	}
	totalParts := int((totalSize + chunkSize - 1) / chunkSize)
	createInput := &s3.CreateMultipartUploadInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	}
	applyS3CreateMultipart(createInput, opts)
	createResp, err := s.inner.CreateMultipartUpload(ctx, createInput)
	if err != nil {
		return fmt.Errorf("CreateMultipartUpload: %w", err)
	}
	uploadID := aws.ToString(createResp.UploadId)
	abort := func() {
		s.inner.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
			Bucket:   aws.String(s.bucket),
			Key:      aws.String(key),
			UploadId: aws.String(uploadID),
		})
	}

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
				upResp, err := s.inner.UploadPart(ctx, &s3.UploadPartInput{
					Bucket:        aws.String(s.bucket),
					Key:           aws.String(key),
					UploadId:      aws.String(uploadID),
					PartNumber:    aws.Int32(int32(pn)),
					Body:          bytes.NewReader(data),
					ContentLength: aws.Int64(int64(len(data))),
				})
				if err != nil {
					results <- partResult{partNumber: pn, err: err}
					continue
				}
				results <- partResult{partNumber: pn, etag: aws.ToString(upResp.ETag)}
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

	completed := make([]s3types.CompletedPart, 0, totalParts)
	for r := range results {
		if r.err != nil {
			abort()
			return fmt.Errorf("UploadPart %d: %w", r.partNumber, r.err)
		}
		completed = append(completed, s3types.CompletedPart{
			PartNumber: aws.Int32(int32(r.partNumber)),
			ETag:       aws.String(r.etag),
		})
	}
	sort.Slice(completed, func(i, j int) bool { return aws.ToInt32(completed[i].PartNumber) < aws.ToInt32(completed[j].PartNumber) })
	_, err = s.inner.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:          aws.String(s.bucket),
		Key:             aws.String(key),
		UploadId:        aws.String(uploadID),
		MultipartUpload: &s3types.CompletedMultipartUpload{Parts: completed},
	})
	if err != nil {
		abort()
		return fmt.Errorf("CompleteMultipartUpload: %w", err)
	}
	return nil
}
