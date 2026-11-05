package objstore

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"

	cos "github.com/tencentyun/cos-go-sdk-v5"
)

func TestResolveMetaInsightAppID(t *testing.T) {
	cases := []struct {
		name string
		cfg  Config
		want string
	}{
		{"显式指定优先", Config{Bucket: "foo-111", MetaInsightAppID: " 999 "}, "999"},
		{"从桶名尾号解析", Config{Bucket: "openclaw-backup-tx-1251036673"}, "1251036673"},
		{"多段连字符仍取尾号", Config{Bucket: "my-a-b-1300000000"}, "1300000000"},
		{"桶名无数字尾号", Config{Bucket: "mybucket"}, ""},
		{"空配置", Config{}, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := resolveMetaInsightAppID(tc.cfg); got != tc.want {
				t.Fatalf("resolveMetaInsightAppID=%q want %q", got, tc.want)
			}
		})
	}
}

func TestMetaInsightBaseURL(t *testing.T) {
	if u := metaInsightBaseURL("1251036673", "ap-beijing"); u == nil ||
		u.String() != "https://1251036673.ci.ap-beijing.myqcloud.com" {
		t.Fatalf("unexpected url: %v", u)
	}
	if metaInsightBaseURL("", "ap-beijing") != nil {
		t.Fatal("empty appid should yield nil")
	}
	if metaInsightBaseURL("1251036673", "") != nil {
		t.Fatal("empty region should yield nil")
	}
}

func TestDocSearcherInterfaceCoverage(t *testing.T) {
	// 编译期：*cosStore 实现 DocSearcher
	var _ DocSearcher = (*cosStore)(nil)
	// 运行时：S3 不支持此能力（未实现 DocSearch 方法）
	var s Store = (*s3Store)(nil)
	if _, ok := s.(DocSearcher); ok {
		t.Fatal("s3Store 不应实现 DocSearcher")
	}
}

func TestDocSearchValidation(t *testing.T) {
	ctx := context.Background()
	// 未构造 MetaInsightURL 的 store → 明确报错，而不是 panic
	c := &cosStore{}
	if _, err := c.DocSearch(ctx, DocSearchQuery{DatasetName: "d", SearchText: "q"}); err != ErrMetaInsightUnavailable {
		t.Fatalf("want ErrMetaInsightUnavailable, got %v", err)
	}
	// 可用但缺参数 → 参数错误（在发请求前拦下，inner 为 nil 也不会 panic）
	c2 := &cosStore{metaInsightOK: true}
	if _, err := c2.DocSearch(ctx, DocSearchQuery{SearchText: "q"}); err == nil {
		t.Fatal("want error when DatasetName empty")
	}
	if _, err := c2.DocSearch(ctx, DocSearchQuery{DatasetName: "d"}); err == nil {
		t.Fatal("want error when SearchText empty")
	}
}

// roundTripFunc 把 http.RoundTripper 适配成函数，用于无网络劫持请求。
type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// TestDocSearch_HybridSearchJSON 用一个劫持的 transport 走完整调用链，验证：
// 固定 Mode/Templates、参数透传、Accept:application/json 让 SDK 正确解 JSON、DocHit 映射。
func TestDocSearch_HybridSearchJSON(t *testing.T) {
	const body = `{"DocResult":[{"URI":"cos://b/knowledge/第16课知识点.md","Text":"〜ています 的两种用法","Score":94,"TextPage":2,"ImageUrls":{}}],"RequestId":"req-1"}`

	var gotReq *http.Request
	var gotBody map[string]interface{}
	rt := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		gotReq = r
		raw, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(raw, &gotBody)
		return &http.Response{
			StatusCode: 200,
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body:       io.NopCloser(strings.NewReader(body)),
		}, nil
	})

	bucketURL, _ := url.Parse("https://openclaw-backup-tx-1251036673.cos.ap-beijing.myqcloud.com")
	miURL, _ := url.Parse("https://1251036673.ci.ap-beijing.myqcloud.com")
	client := cos.NewClient(&cos.BaseURL{BucketURL: bucketURL, MetaInsightURL: miURL}, &http.Client{
		Transport: &cos.AuthorizationTransport{SecretID: "id", SecretKey: "key", Transport: rt},
	})
	c := &cosStore{inner: client, metaInsightOK: true}

	hits, err := c.DocSearch(context.Background(), DocSearchQuery{
		DatasetName:    "openclaw-backup-tx-doc-u85qww",
		SearchText:     "ている",
		Limit:          10,
		MatchThreshold: 50,
		Filter:         map[string]interface{}{"ContentType": "text/markdown"},
	})
	if err != nil {
		t.Fatalf("DocSearch error: %v", err)
	}

	if gotReq == nil {
		t.Fatal("no request sent")
	}
	if gotReq.Method != http.MethodPost {
		t.Fatalf("method=%s want POST", gotReq.Method)
	}
	if !strings.HasSuffix(gotReq.URL.Path, "/datasetquery/hybridsearch") {
		t.Fatalf("path=%s", gotReq.URL.Path)
	}
	if gotReq.Header.Get("Accept") != "application/json" || gotReq.Header.Get("Content-Type") != "application/json" {
		t.Fatalf("headers Accept=%q Content-Type=%q", gotReq.Header.Get("Accept"), gotReq.Header.Get("Content-Type"))
	}
	if gotBody["Mode"] != "text" || gotBody["Templates"] != "DocSearch" {
		t.Fatalf("Mode/Templates not fixed: %v", gotBody)
	}
	if gotBody["DatasetName"] != "openclaw-backup-tx-doc-u85qww" || gotBody["SearchText"] != "ている" {
		t.Fatalf("query not passed: %v", gotBody)
	}
	if int(gotBody["MatchThreshold"].(float64)) != 50 || int(gotBody["Limit"].(float64)) != 10 {
		t.Fatalf("threshold/limit not passed: %v", gotBody)
	}
	if _, ok := gotBody["Filter"]; !ok {
		t.Fatal("Filter not passed")
	}

	if len(hits) != 1 {
		t.Fatalf("len(hits)=%d want 1", len(hits))
	}
	h := hits[0]
	if h.Score != 94 || h.TextPage != 2 || h.Text != "〜ています 的两种用法" ||
		!strings.Contains(h.URI, "第16课知识点.md") {
		t.Fatalf("unexpected hit: %+v", h)
	}
}

// TestDocSearch_SDKVersionHasHybridSearch 保证依赖已升到含 HybridSearch 的版本。
// 若 cos-go-sdk-v5 被回退到 v0.7.73，本文件无法编译（方法不存在），即此约束。
func TestDocSearch_SDKVersionHasHybridSearch(t *testing.T) {
	// 仅引用一个新版本才有的类型，编译通过即代表 >= v0.7.74。
	var _ = cos.HybridSearchOptions{DatasetName: "x", Mode: "text", Templates: "DocSearch"}
}
