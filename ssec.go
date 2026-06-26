package objstore

import (
	"crypto/md5"
	"encoding/base64"
)

// SSE-C 算法常量。COS / S3 均要求 AES256。
const sseCustomerAlgorithm = "AES256"

// sseCustomerKey 持有 SSE-C 客户密钥及其预计算的 header 值，
// 绑定在 cosStore / s3Store 实例上，所有读写路径共用同一把密钥。
//
// rawKey 必须恰好 32 字节（256-bit）。在 New 时已校验长度。
type sseCustomerKey struct {
	rawKey []byte // 32 字节原始密钥
	keyB64 string // base64(rawKey)
	md5B64 string // base64(md5(rawKey))
}

// newSSECustomerKey 由 32 字节原始密钥构造，预计算 base64(key) 与 base64(md5(key))。
// rawKey 长度非 32 时返回 nil（调用方应已在 New 校验长度，这里做防御性返回）。
func newSSECustomerKey(rawKey []byte) *sseCustomerKey {
	if len(rawKey) != 32 {
		return nil
	}
	sum := md5.Sum(rawKey)
	return &sseCustomerKey{
		rawKey: rawKey,
		keyB64: base64.StdEncoding.EncodeToString(rawKey),
		md5B64: base64.StdEncoding.EncodeToString(sum[:]),
	}
}

// algo 返回 SSE-C 算法字符串（恒为 AES256）。
func (k *sseCustomerKey) algo() string { return sseCustomerAlgorithm }
