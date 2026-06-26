package objstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/url"
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
	ssec   *sseCustomerKey // SSE-C 客户密钥，nil 表示未启用
}

func newS3Store(cfg Config) (Store, error) {
	loadOpts := []func(*awsconfig.LoadOptions) error{
		awsconfig.WithClientLogMode(0),
	}
	if cfg.Region != "" {
		loadOpts = append(loadOpts, awsconfig.WithRegion(cfg.Region))
	}

	// 凭证解析：
	//   - SecretID/SecretKey 都给 → 静态凭证（保持向后兼容）。
	//   - 否则走 awssdk default credential chain：
	//       env → shared credentials/profile → container creds → IMDS → STS AssumeRole
	//   - 指定 Profile 时只走该 profile（含 source_profile/AssumeRole 链）。
	switch {
	case cfg.SecretID != "" && cfg.SecretKey != "":
		loadOpts = append(loadOpts, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.SecretID, cfg.SecretKey, ""),
		))
	case cfg.SecretID != "" || cfg.SecretKey != "":
		return nil, fmt.Errorf("s3 config: SecretID 与 SecretKey 必须同时提供（或同时为空走 default credential chain）")
	case cfg.Profile != "":
		loadOpts = append(loadOpts, awsconfig.WithSharedConfigProfile(cfg.Profile))
	}

	awsCfg, err := awsconfig.LoadDefaultConfig(context.Background(), loadOpts...)
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
	var ssec *sseCustomerKey
	if len(cfg.SSECustomerKey) == 32 {
		ssec = newSSECustomerKey(cfg.SSECustomerKey)
	}
	return &s3Store{
		inner:  s3.NewFromConfig(awsCfg, opts...),
		bucket: cfg.Bucket,
		region: cfg.Region,
		ssec:   ssec,
	}, nil
}

func (s *s3Store) Provider() ProviderType { return ProviderS3 }
func (s *s3Store) BucketName() string { return s.bucket }

// ---- SSE-C header 注入辅助 ----
//
// S3 SDK 的 SSECustomerKey 传 base64 字符串，SSECustomerKeyMD5 传 base64(md5)。

func (s *s3Store) applySSECGetInput(in *s3.GetObjectInput) {
	if s.ssec == nil {
		return
	}
	in.SSECustomerAlgorithm = aws.String(s.ssec.algo())
	in.SSECustomerKey = aws.String(s.ssec.keyB64)
	in.SSECustomerKeyMD5 = aws.String(s.ssec.md5B64)
}

func (s *s3Store) applySSECHeadInput(in *s3.HeadObjectInput) {
	if s.ssec == nil {
		return
	}
	in.SSECustomerAlgorithm = aws.String(s.ssec.algo())
	in.SSECustomerKey = aws.String(s.ssec.keyB64)
	in.SSECustomerKeyMD5 = aws.String(s.ssec.md5B64)
}

func (s *s3Store) applySSECPutInput(in *s3.PutObjectInput) {
	if s.ssec == nil {
		return
	}
	in.SSECustomerAlgorithm = aws.String(s.ssec.algo())
	in.SSECustomerKey = aws.String(s.ssec.keyB64)
	in.SSECustomerKeyMD5 = aws.String(s.ssec.md5B64)
}

func (s *s3Store) applySSECCreateMultipart(in *s3.CreateMultipartUploadInput) {
	if s.ssec == nil {
		return
	}
	in.SSECustomerAlgorithm = aws.String(s.ssec.algo())
	in.SSECustomerKey = aws.String(s.ssec.keyB64)
	in.SSECustomerKeyMD5 = aws.String(s.ssec.md5B64)
}

func (s *s3Store) applySSECUploadPart(in *s3.UploadPartInput) {
	if s.ssec == nil {
		return
	}
	in.SSECustomerAlgorithm = aws.String(s.ssec.algo())
	in.SSECustomerKey = aws.String(s.ssec.keyB64)
	in.SSECustomerKeyMD5 = aws.String(s.ssec.md5B64)
}

// applySSECCopyObject 给 CopyObjectInput 注入目标 SSE-C（本 store）与源 SSE-C（srcStore）header。
func (s *s3Store) applySSECCopyObject(in *s3.CopyObjectInput, srcStore *s3Store) {
	if s.ssec != nil {
		in.SSECustomerAlgorithm = aws.String(s.ssec.algo())
		in.SSECustomerKey = aws.String(s.ssec.keyB64)
		in.SSECustomerKeyMD5 = aws.String(s.ssec.md5B64)
	}
	if srcStore.ssec != nil {
		in.CopySourceSSECustomerAlgorithm = aws.String(srcStore.ssec.algo())
		in.CopySourceSSECustomerKey = aws.String(srcStore.ssec.keyB64)
		in.CopySourceSSECustomerKeyMD5 = aws.String(srcStore.ssec.md5B64)
	}
}

// applySSECUploadPartCopy 给 UploadPartCopyInput 注入目标与源 SSE-C header。
func (s *s3Store) applySSECUploadPartCopy(in *s3.UploadPartCopyInput, srcStore *s3Store) {
	if s.ssec != nil {
		in.SSECustomerAlgorithm = aws.String(s.ssec.algo())
		in.SSECustomerKey = aws.String(s.ssec.keyB64)
		in.SSECustomerKeyMD5 = aws.String(s.ssec.md5B64)
	}
	if srcStore.ssec != nil {
		in.CopySourceSSECustomerAlgorithm = aws.String(srcStore.ssec.algo())
		in.CopySourceSSECustomerKey = aws.String(srcStore.ssec.keyB64)
		in.CopySourceSSECustomerKeyMD5 = aws.String(srcStore.ssec.md5B64)
	}
}

// ---- BucketAdmin ----

// CreateBucket 创建当前桶。主要区域 us-east-1 不能指定 LocationConstraint。
func (s *s3Store) CreateBucket(ctx context.Context) error {
	input := &s3.CreateBucketInput{Bucket: aws.String(s.bucket)}
	if s.region != "" && s.region != "us-east-1" {
		input.CreateBucketConfiguration = &s3types.CreateBucketConfiguration{
			LocationConstraint: s3types.BucketLocationConstraint(s.region),
		}
	}
	_, err := s.inner.CreateBucket(ctx, input)
	if err == nil {
		return nil
	}
	var already *s3types.BucketAlreadyOwnedByYou
	if errors.As(err, &already) {
		return ErrBucketAlreadyOwnedByYou
	}
	return err
}

// DeleteBucket 删除当前桶。要求桶为空。
func (s *s3Store) DeleteBucket(ctx context.Context) error {
	_, err := s.inner.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: aws.String(s.bucket)})
	return err
}

// HeadBucket 检查当前桶是否存在且可访问。
func (s *s3Store) HeadBucket(ctx context.Context) error {
	_, err := s.inner.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(s.bucket)})
	if err != nil {
		var nf *s3types.NoSuchBucket
		if errors.As(err, &nf) {
			return ErrBucketNotFound
		}
		// 404 不一定走 NoSuchBucket（可能是 apierr 404 string）
		if strings.Contains(err.Error(), "404") || strings.Contains(err.Error(), "NotFound") {
			return ErrBucketNotFound
		}
		return err
	}
	return nil
}


// ---- 元信息 ----

func (s *s3Store) HeadObject(ctx context.Context, key string) (*ObjectInfo, error) {
	in := &s3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	}
	s.applySSECHeadInput(in)
	resp, err := s.inner.HeadObject(ctx, in)
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
	info.ETag = strings.Trim(aws.ToString(resp.ETag), "\"")
	info.StorageClass = string(resp.StorageClass)
	info.ContentType = aws.ToString(resp.ContentType)
	info.ServerSideEncryption = string(resp.ServerSideEncryption)
	info.SSEKMSKeyID = aws.ToString(resp.SSEKMSKeyId)
	info.VersionID = aws.ToString(resp.VersionId)
	if len(resp.Metadata) > 0 {
		info.Metadata = make(map[string]string, len(resp.Metadata))
		for k, v := range resp.Metadata {
			info.Metadata[strings.ToLower(k)] = v
		}
	}
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
	in := &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	}
	s.applySSECGetInput(in)
	resp, err := s.inner.GetObject(ctx, in)
	if err != nil {
		return nil, err
	}
	return resp.Body, nil
}

func (s *s3Store) GetRange(ctx context.Context, key string, start, end int64) ([]byte, error) {
	in := &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
		Range:  aws.String(fmt.Sprintf("bytes=%d-%d", start, end)),
	}
	s.applySSECGetInput(in)
	resp, err := s.inner.GetObject(ctx, in)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	return io.ReadAll(resp.Body)
}

func (s *s3Store) GetAll(ctx context.Context, key string) ([]byte, error) {
	in := &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	}
	s.applySSECGetInput(in)
	resp, err := s.inner.GetObject(ctx, in)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	return io.ReadAll(resp.Body)
}

// ---- 上传 ----

func (s *s3Store) PutObject(ctx context.Context, key string, data []byte) error {
	in := &s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(data),
	}
	s.applySSECPutInput(in)
	_, err := s.inner.PutObject(ctx, in)
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
	s.applySSECPutInput(input)
	_, err := s.inner.PutObject(ctx, input)
	return err
}

// ---- 分块上传 ----

func (s *s3Store) MultipartUpload(ctx context.Context, key string, totalSize, chunkSize int64, concurrency int,
	fetchPart func(partNumber int, offset, size int64) ([]byte, error)) error {

	totalParts := int((totalSize + chunkSize - 1) / chunkSize)
	createInput := &s3.CreateMultipartUploadInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	}
	s.applySSECCreateMultipart(createInput)
	createResp, err := s.inner.CreateMultipartUpload(ctx, createInput)
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
				upInput := &s3.UploadPartInput{
					Bucket:     aws.String(s.bucket),
					Key:        aws.String(key),
					UploadId:   aws.String(uploadID),
					PartNumber: aws.Int32(int32(pn)),
					Body:       bytes.NewReader(data),
				}
				s.applySSECUploadPart(upInput)
				resp, err := s.inner.UploadPart(ctx, upInput)
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
	in := &s3.CopyObjectInput{
		Bucket:     aws.String(s.bucket),
		Key:        aws.String(dstKey),
		CopySource: aws.String(srcStore.bucket + "/" + srcKey),
	}
	s.applySSECCopyObject(in, srcStore)
	_, err := s.inner.CopyObject(ctx, in)
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

	createInput := &s3.CreateMultipartUploadInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(dstKey),
	}
	s.applySSECCreateMultipart(createInput)
	createResp, err := s.inner.CreateMultipartUpload(ctx, createInput)
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
				copyIn := &s3.UploadPartCopyInput{
					Bucket:          aws.String(s.bucket),
					Key:             aws.String(dstKey),
					UploadId:        aws.String(uploadID),
					PartNumber:      aws.Int32(int32(pn)),
					CopySource:      aws.String(copySource),
					CopySourceRange: aws.String(fmt.Sprintf("bytes=%d-%d", start, end)),
				}
				s.applySSECUploadPartCopy(copyIn, srcS3)
				resp, err := s.inner.UploadPartCopy(ctx, copyIn)
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
	in := &s3.CreateMultipartUploadInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	}
	s.applySSECCreateMultipart(in)
	resp, err := s.inner.CreateMultipartUpload(ctx, in)
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
	in := &s3.UploadPartInput{
		Bucket:        aws.String(s.bucket),
		Key:           aws.String(key),
		UploadId:      aws.String(uploadID),
		PartNumber:    aws.Int32(int32(partNumber)),
		Body:          bytes.NewReader(data),
		ContentLength: aws.Int64(int64(len(data))),
	}
	s.applySSECUploadPart(in)
	resp, err := s.inner.UploadPart(ctx, in)
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

// ListIncompleteUploads 列出云端未完成的 multipart uploads（自动分页）。
func (s *s3Store) ListIncompleteUploads(ctx context.Context, prefix string) ([]IncompleteUpload, error) {
	var out []IncompleteUpload
	var keyMarker, uploadIDMarker *string
	for {
		in := &s3.ListMultipartUploadsInput{
			Bucket:         aws.String(s.bucket),
			KeyMarker:      keyMarker,
			UploadIdMarker: uploadIDMarker,
		}
		if prefix != "" {
			in.Prefix = aws.String(prefix)
		}
		res, err := s.inner.ListMultipartUploads(ctx, in)
		if err != nil {
			return nil, fmt.Errorf("s3 ListMultipartUploads: %w", err)
		}
		for _, u := range res.Uploads {
			item := IncompleteUpload{}
			if u.Key != nil {
				item.Key = *u.Key
			}
			if u.UploadId != nil {
				item.UploadID = *u.UploadId
			}
			if u.Initiated != nil {
				item.Initiated = *u.Initiated
			}
			out = append(out, item)
		}
		if res.IsTruncated == nil || !*res.IsTruncated {
			break
		}
		keyMarker = res.NextKeyMarker
		uploadIDMarker = res.NextUploadIdMarker
	}
	return out, nil
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
	if opts.ACL != "" {
		input.ACL = s3types.ObjectCannedACL(opts.ACL)
	}
	if len(opts.Tags) > 0 {
		input.Tagging = aws.String(encodeS3Tagging(opts.Tags))
	}
	if opts.SSE != "" {
		input.ServerSideEncryption = s3types.ServerSideEncryption(opts.SSE)
		if opts.SSEKMSKeyID != "" {
			input.SSEKMSKeyId = aws.String(opts.SSEKMSKeyID)
		}
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
	if opts.ACL != "" {
		input.ACL = s3types.ObjectCannedACL(opts.ACL)
	}
	if len(opts.Tags) > 0 {
		input.Tagging = aws.String(encodeS3Tagging(opts.Tags))
	}
	if opts.SSE != "" {
		input.ServerSideEncryption = s3types.ServerSideEncryption(opts.SSE)
		if opts.SSEKMSKeyID != "" {
			input.SSEKMSKeyId = aws.String(opts.SSEKMSKeyID)
		}
	}
	if len(opts.Metadata) > 0 {
		m := make(map[string]string, len(opts.Metadata))
		for k, v := range opts.Metadata {
			m[k] = v
		}
		input.Metadata = m
	}
}

// encodeS3Tagging 按 S3 文档 Tagging header 格式编码 (k1=v1&k2=v2，k/v URL-encoded)。
func encodeS3Tagging(tags map[string]string) string {
	values := url.Values{}
	for k, v := range tags {
		values.Set(k, v)
	}
	return values.Encode()
}

func (s *s3Store) PutObjectOpt(ctx context.Context, key string, data []byte, opts *PutOptions) error {
	input := &s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(data),
	}
	applyS3PutOptions(input, opts)
	s.applySSECPutInput(input) // SSE-C 启用时注入
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
	s.applySSECPutInput(input)
	_, err := s.inner.PutObject(ctx, input)
	return err
}

func (s *s3Store) InitMultipartOpt(ctx context.Context, key string, opts *PutOptions) (string, error) {
	input := &s3.CreateMultipartUploadInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	}
	applyS3CreateMultipart(input, opts)
	s.applySSECCreateMultipart(input)
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
	s.applySSECCreateMultipart(createInput)
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
				upInput := &s3.UploadPartInput{
					Bucket:        aws.String(s.bucket),
					Key:           aws.String(key),
					UploadId:      aws.String(uploadID),
					PartNumber:    aws.Int32(int32(pn)),
					Body:          bytes.NewReader(data),
					ContentLength: aws.Int64(int64(len(data))),
				}
				s.applySSECUploadPart(upInput)
				upResp, err := s.inner.UploadPart(ctx, upInput)
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
