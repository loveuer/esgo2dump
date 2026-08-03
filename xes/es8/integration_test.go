package es8

import (
	"encoding/json"
	"strings"
	"testing"

	elastic "github.com/elastic/go-elasticsearch/v8"
	"github.com/loveuer/esgo2dump/internal/tool"
)

const integrationURI = "http://127.0.0.1:9200"

func requireMajor(t *testing.T, client *elastic.Client, major string) {
	t.Helper()

	res, err := client.Info(client.Info.WithContext(tool.Timeout(5)))
	if err != nil {
		t.Skipf("Skipping test - ES server not available: %v", err)
	}
	defer res.Body.Close()

	var info struct {
		Version struct {
			Number string `json:"number"`
		} `json:"version"`
	}
	if err := json.NewDecoder(res.Body).Decode(&info); err != nil {
		t.Fatalf("decode server info: %v", err)
	}

	if strings.Split(info.Version.Number, ".")[0] != major {
		t.Skipf("Skipping test - server es %s, want major %s", info.Version.Number, major)
	}
}

func deleteIndex(t *testing.T, client *elastic.Client, index string) {
	t.Helper()

	res, err := client.Indices.Delete([]string{index})
	if err != nil {
		t.Fatalf("delete index %s: %v", index, err)
	}
	defer res.Body.Close()

	if res.StatusCode != 200 && res.StatusCode != 404 {
		t.Fatalf("delete index %s: %s", index, res.String())
	}
}

func refresh(t *testing.T, client *elastic.Client, index string) {
	t.Helper()

	res, err := client.Indices.Refresh(client.Indices.Refresh.WithIndex(index))
	if err != nil {
		t.Fatalf("refresh %s: %v", index, err)
	}
	defer res.Body.Close()

	if res.StatusCode >= 300 {
		t.Fatalf("refresh %s: %s", index, res.String())
	}
}

func TestStreamer_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx := tool.Timeout(60)
	c, err := NewClient(ctx, integrationURI)
	if err != nil {
		t.Skipf("Skipping test - ES server not available: %v", err)
	}
	requireMajor(t, c, "8")

	srcIndex := "esgo2dump_it8_src"
	dstIndex := "esgo2dump_it8_dst"

	deleteIndex(t, c, srcIndex)
	deleteIndex(t, c, dstIndex)
	defer func() {
		deleteIndex(t, c, srcIndex)
		deleteIndex(t, c, dstIndex)
	}()

	// create source index with typeless mapping
	createBody := `{"settings":{"number_of_shards":1,"number_of_replicas":0},"mappings":{"properties":{"name":{"type":"keyword"},"age":{"type":"integer"}}}}`
	res, err := c.Indices.Create(
		srcIndex,
		c.Indices.Create.WithContext(ctx),
		c.Indices.Create.WithBody(strings.NewReader(createBody)),
	)
	if err != nil {
		t.Fatalf("create src index: %v", err)
	}
	res.Body.Close()
	if res.StatusCode >= 300 {
		t.Fatalf("create src index: %s", res.String())
	}

	streamer, err := NewStreamer(ctx, c, srcIndex)
	if err != nil {
		t.Fatalf("NewStreamer: %v", err)
	}

	items := []map[string]any{
		{"_id": "1", "_source": map[string]any{"name": "alice", "age": float64(30)}},
		{"_id": "2", "_source": map[string]any{"name": "bob", "age": float64(25)}},
		{"_id": "3", "_source": map[string]any{"name": "carol", "age": float64(35)}},
	}
	if n, err := streamer.WriteData(ctx, items); err != nil || n != 3 {
		t.Fatalf("WriteData got n=%d err=%v", n, err)
	}

	refresh(t, c, srcIndex)

	// scroll read: 2 + 1 + 0
	first, err := streamer.ReadData(ctx, 2, nil, nil, nil)
	if err != nil {
		t.Fatalf("ReadData batch 1: %v", err)
	}
	if len(first) != 2 {
		t.Fatalf("batch 1 len = %d, want 2", len(first))
	}

	second, err := streamer.ReadData(ctx, 2, nil, nil, nil)
	if err != nil {
		t.Fatalf("ReadData batch 2: %v", err)
	}
	if len(second) != 1 {
		t.Fatalf("batch 2 len = %d, want 1", len(second))
	}

	third, err := streamer.ReadData(ctx, 2, nil, nil, nil)
	if err != nil {
		t.Fatalf("ReadData batch 3: %v", err)
	}
	if len(third) != 0 {
		t.Fatalf("batch 3 len = %d, want 0", len(third))
	}
	streamer.Cleanup()

	// scroll returns arbitrary subsets; the two batches together must cover all 3 docs
	all := append(first, second...)
	ids := map[string]bool{}
	alice := false
	for _, it := range all {
		ids[it["_id"].(string)] = true
		if src, ok := it["_source"].(map[string]any); ok && src["name"] == "alice" {
			alice = true
		}
	}
	if len(ids) != 3 || !ids["1"] || !ids["2"] || !ids["3"] {
		t.Errorf("scroll ids = %v, want 1/2/3", ids)
	}
	if !alice {
		t.Errorf("scroll results missing alice: %v", all)
	}

	// write into a second index using the scroll-read items
	dst, err := NewStreamer(ctx, c, dstIndex)
	if err != nil {
		t.Fatalf("NewStreamer dst: %v", err)
	}
	if n, err := dst.WriteData(ctx, all); err != nil || n != 3 {
		t.Fatalf("dst WriteData got n=%d err=%v", n, err)
	}

	refresh(t, c, dstIndex)

	searchRes, err := c.Search(
		c.Search.WithContext(ctx),
		c.Search.WithIndex(dstIndex),
	)
	if err != nil {
		t.Fatalf("search dst: %v", err)
	}

	var result struct {
		Hits struct {
			Total struct {
				Value int `json:"value"`
			} `json:"total"`
			Hits []struct {
				ID      string         `json:"_id"`
				Content map[string]any `json:"_source"`
			} `json:"hits"`
		} `json:"hits"`
	}
	if err := json.NewDecoder(searchRes.Body).Decode(&result); err != nil {
		t.Fatalf("decode search: %v", err)
	}
	searchRes.Body.Close()

	if result.Hits.Total.Value != 3 {
		t.Errorf("dst total = %d, want 3", result.Hits.Total.Value)
	}

	found := false
	for _, hit := range result.Hits.Hits {
		if hit.ID == "1" && hit.Content["name"] == "alice" {
			found = true
		}
	}
	if !found {
		t.Errorf("dst missing expected doc, got %+v", result.Hits.Hits)
	}

	// mapping / setting read
	mapping, err := streamer.ReadMapping(ctx)
	if err != nil {
		t.Fatalf("ReadMapping: %v", err)
	}
	if _, ok := mapping[srcIndex]; !ok {
		t.Errorf("mapping missing index key: %v", mapping)
	}

	setting, err := streamer.ReadSetting(ctx)
	if err != nil {
		t.Fatalf("ReadSetting: %v", err)
	}
	if _, ok := setting[srcIndex]; !ok {
		t.Errorf("setting missing index key: %v", setting)
	}

	// old-style (5.x/6.x) mapping must be converted before create
	convIndex := "esgo2dump_it8_conv"
	deleteIndex(t, c, convIndex)
	defer deleteIndex(t, c, convIndex)

	oldMapping := map[string]any{
		"old_idx": map[string]any{
			"mappings": map[string]any{
				"doc": map[string]any{
					"_all": map[string]any{"enabled": false},
					"properties": map[string]any{
						"name": map[string]any{"type": "keyword"},
					},
				},
			},
		},
	}

	convStreamer, err := NewStreamer(ctx, c, convIndex)
	if err != nil {
		t.Fatalf("NewStreamer conv: %v", err)
	}
	if err := convStreamer.WriteMapping(ctx, oldMapping); err != nil {
		t.Fatalf("WriteMapping old-style: %v", err)
	}

	// verify the created mapping is typeless
	mappingRes, err := c.Indices.GetMapping(
		c.Indices.GetMapping.WithIndex(convIndex),
	)
	if err != nil {
		t.Fatalf("get mapping: %v", err)
	}
	var convMap map[string]any
	if err := json.NewDecoder(mappingRes.Body).Decode(&convMap); err != nil {
		t.Fatalf("decode mapping: %v", err)
	}
	mappingRes.Body.Close()

	idx := convMap[convIndex].(map[string]any)
	mappings := idx["mappings"].(map[string]any)
	if _, hasType := mappings["doc"]; hasType {
		t.Errorf("created mapping still has type layer: %v", mappings)
	}
	if _, hasProps := mappings["properties"]; !hasProps {
		t.Errorf("created mapping missing properties: %v", mappings)
	}
}
