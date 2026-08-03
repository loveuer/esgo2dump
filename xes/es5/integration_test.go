package es5

import (
	"encoding/json"
	"testing"

	"github.com/go-resty/resty/v2"
	"github.com/loveuer/esgo2dump/internal/tool"
	"github.com/loveuer/esgo2dump/pkg/model"
)

const integrationURI = "http://127.0.0.1:9200"

func raw(t *testing.T, client *Client, method, path, body string) *resty.Response {
	t.Helper()

	resp, err := client.c.R().
		SetHeader("Content-Type", "application/json").
		SetBody(body).
		Execute(method, path)
	if err != nil {
		t.Fatalf("raw %s %s: %v", method, path, err)
	}

	return resp
}

func TestStreamer_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx := tool.Timeout(60)
	client, err := NewClient(ctx, integrationURI)
	if err != nil {
		t.Skipf("Skipping test - ES server not available: %v", err)
	}

	srcIndex := "esgo2dump_it5_src"
	dstIndex := "esgo2dump_it5_dst"

	// clean leftovers from previous runs
	raw(t, client, resty.MethodDelete, "/"+srcIndex, "")
	raw(t, client, resty.MethodDelete, "/"+dstIndex, "")
	defer func() {
		raw(t, client, resty.MethodDelete, "/"+srcIndex, "")
		raw(t, client, resty.MethodDelete, "/"+dstIndex, "")
	}()

	// create source index with settings + mappings
	createBody := `{"settings":{"number_of_shards":1,"number_of_replicas":0},"mappings":{"doc":{"properties":{"name":{"type":"keyword"},"age":{"type":"integer"}}}}}`
	if resp := raw(t, client, resty.MethodPut, "/"+srcIndex, createBody); resp.StatusCode() >= 300 {
		t.Fatalf("create src index: %s", resp.String())
	}

	streamer, err := NewStreamer(ctx, client, srcIndex)
	if err != nil {
		t.Fatalf("NewStreamer: %v", err)
	}

	// write 3 docs (dump-style wrapped items)
	items := []map[string]any{
		{"_id": "1", "_type": "doc", "_source": map[string]any{"name": "alice", "age": float64(30)}},
		{"_id": "2", "_type": "doc", "_source": map[string]any{"name": "bob", "age": float64(25)}},
		{"_id": "3", "_type": "doc", "_source": map[string]any{"name": "carol", "age": float64(35)}},
	}
	if n, err := streamer.WriteData(ctx, items); err != nil || n != 3 {
		t.Fatalf("WriteData got n=%d err=%v", n, err)
	}

	raw(t, client, resty.MethodPost, "/"+srcIndex+"/_refresh", "")

	// scroll read: 2 + 1 + 0
	first, err := streamer.ReadData(ctx, 2, nil, nil, nil)
	if err != nil {
		t.Fatalf("ReadData batch 1: %v", err)
	}
	if len(first) != 2 {
		t.Fatalf("batch 1 len = %d, want 2", len(first))
	}

	item := first[0]
	if item["_id"] != "1" {
		t.Errorf("batch1 item0 _id = %v, want 1", item["_id"])
	}
	if item["_type"] != "doc" {
		t.Errorf("batch1 item0 _type = %v, want doc", item["_type"])
	}
	src, ok := item["_source"].(map[string]any)
	if !ok || src["name"] != "alice" {
		t.Errorf("batch1 item0 _source = %v", item["_source"])
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

	// write into a second index using the scroll-read items
	dst, err := NewStreamer(ctx, client, dstIndex)
	if err != nil {
		t.Fatalf("NewStreamer dst: %v", err)
	}
	all := append(first, second...)
	if n, err := dst.WriteData(ctx, all); err != nil || n != 3 {
		t.Fatalf("dst WriteData got n=%d err=%v", n, err)
	}

	raw(t, client, resty.MethodPost, "/"+dstIndex+"/_refresh", "")

	// verify dst contents
	searchResp := raw(t, client, resty.MethodGet, "/"+dstIndex+"/_search", "")
	var result struct {
		Hits struct {
			Total int `json:"total"`
			Hits  []struct {
				ID      string         `json:"_id"`
				Type    string         `json:"_type"`
				Content map[string]any `json:"_source"`
			} `json:"hits"`
		} `json:"hits"`
	}
	if err := json.Unmarshal(searchResp.Body(), &result); err != nil {
		t.Fatalf("unmarshal search: %v", err)
	}
	if result.Hits.Total != 3 {
		t.Errorf("dst total = %d, want 3", result.Hits.Total)
	}

	found := false
	for _, hit := range result.Hits.Hits {
		if hit.ID == "1" && hit.Type == "doc" && hit.Content["name"] == "alice" {
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
}

func TestWriteSetting_FiltersImmutable(t *testing.T) {
	setting := map[string]any{
		"idx": map[string]any{
			"settings": map[string]any{
				"index": map[string]any{
					"uuid":               "abc",
					"number_of_shards":   "1",
					"number_of_replicas": "0",
					"refresh_interval":   "1s",
					"provided_name":      "idx",
					"creation_date":      "123",
				},
			},
		},
	}

	payload := model.FilterUpdatableSettings(setting)
	if len(payload) != 2 {
		t.Errorf("filtered payload keys = %v, want number_of_replicas + refresh_interval", payload)
	}
	if _, ok := payload["number_of_shards"]; ok {
		t.Errorf("number_of_shards should be filtered out, got %v", payload["number_of_shards"])
	}
	if _, ok := payload["uuid"]; ok {
		t.Errorf("uuid should be filtered out, got %v", payload["uuid"])
	}
}
