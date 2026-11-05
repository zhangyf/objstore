package objstore

import (
	"context"
	"fmt"
	"net/url"
	"regexp"
	"strings"

	cos "github.com/tencentyun/cos-go-sdk-v5"
)

// bucketAppIDRe 匹配 COS 桶名尾部的 AppId，形如 "xxx-1251036673"。
var bucketAppIDRe = regexp.MustCompile(`-(\d+)$`)

// resolveMetaInsightAppID 解析 MetaInsight 需要的 AppId。
// 优先用 Config.MetaInsightAppID 显式指定；否则从 Bucket 名尾号自动解析。
// 返回 "" 表示无法解析（此时不构造 MetaInsightURL，DocSearcher 不可用）。
func resolveMetaInsightAppID(cfg Config) string {
	if id := strings.TrimSpace(cfg.MetaInsightAppID); id != "" {
		return id
	}
	if m := bucketAppIDRe.FindStringSubmatch(cfg.Bucket); m != nil {
		return m[1]
	}
	return ""
}

// metaInsightBaseURL 构造 CI 智能检索域名 <AppId>.ci.<region>.myqcloud.com。
// SDK 的 newRequest 会用 metaInsightHostPrefix 校验 host 必须匹配此格式。
func metaInsightBaseURL(appID, region string) *url.URL {
	if appID == "" || region == "" {
		return nil
	}
	u, err := url.Parse(fmt.Sprintf("https://%s.ci.%s.myqcloud.com", appID, region))
	if err != nil {
		return nil
	}
	return u
}

// 编译期断言：*cosStore 实现可选接口 DocSearcher。
var _ DocSearcher = (*cosStore)(nil)

// DocSearch 执行 COS 智能检索（MetaInsight）文档检索（hybridsearch + DocSearch 模板）。
//
// Mode / Templates 固定为 "text" / "DocSearch"（文档语义检索），调用方只需给查询文本。
// 凭证、签名、host 全部复用 Store 初始化时的 cos.Client，不额外引入依赖或配置。
func (c *cosStore) DocSearch(ctx context.Context, q DocSearchQuery) ([]DocHit, error) {
	if !c.metaInsightOK {
		return nil, ErrMetaInsightUnavailable
	}
	if strings.TrimSpace(q.DatasetName) == "" {
		return nil, fmt.Errorf("objstore: DocSearch 需要 DatasetName")
	}
	if strings.TrimSpace(q.SearchText) == "" {
		return nil, fmt.Errorf("objstore: DocSearch 需要 SearchText")
	}

	opt := &cos.HybridSearchOptions{
		DatasetName:    q.DatasetName,
		Mode:           "text",
		Templates:      "DocSearch",
		SearchText:     q.SearchText,
		Limit:          q.Limit,
		MatchThreshold: q.MatchThreshold,
		Filter:         q.Filter,
	}

	res, _, err := c.inner.MetaInsight.HybridSearch(ctx, opt)
	if err != nil {
		return nil, err
	}
	if res == nil {
		return nil, nil
	}

	hits := make([]DocHit, 0, len(res.DocResult))
	for _, d := range res.DocResult {
		if d == nil {
			continue
		}
		hits = append(hits, DocHit{
			URI:       d.URI,
			Text:      d.Text,
			Score:     d.Score,
			TextPage:  d.TextPage,
			ImageUrls: d.ImageUrls,
		})
	}
	return hits, nil
}
