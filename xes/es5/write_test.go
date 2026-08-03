package es5

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestUnwrapItem(t *testing.T) {
	tests := []struct {
		name     string
		item     map[string]any
		wantID   string
		wantType string
		wantSrc  map[string]any
	}{
		{
			name: "dump style item",
			item: map[string]any{
				"_id":     "1",
				"_index":  "src_index",
				"_type":   "doc",
				"_source": map[string]any{"name": "alice"},
			},
			wantID:   "1",
			wantType: "doc",
			wantSrc:  map[string]any{"name": "alice"},
		},
		{
			name: "dump style item without type falls back to default",
			item: map[string]any{
				"_id":     "2",
				"_source": map[string]any{"name": "bob"},
			},
			wantID:   "2",
			wantType: DefaultType,
			wantSrc:  map[string]any{"name": "bob"},
		},
		{
			name:     "plain ndjson line is the source itself",
			item:     map[string]any{"name": "carol", "age": float64(30)},
			wantID:   "",
			wantType: DefaultType,
			wantSrc:  map[string]any{"name": "carol", "age": float64(30)},
		},
		{
			name: "_source of wrong type treated as plain doc",
			item: map[string]any{
				"_id":     "3",
				"_source": "not-a-map",
			},
			wantID:   "",
			wantType: DefaultType,
			wantSrc:  map[string]any{"_id": "3", "_source": "not-a-map"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			id, typ, src := unwrapItem(tt.item)
			if id != tt.wantID {
				t.Errorf("id = %q, want %q", id, tt.wantID)
			}
			if typ != tt.wantType {
				t.Errorf("type = %q, want %q", typ, tt.wantType)
			}

			got, _ := json.Marshal(src)
			want, _ := json.Marshal(tt.wantSrc)
			if string(got) != string(want) {
				t.Errorf("src = %s, want %s", got, want)
			}
		})
	}
}

func TestBuildBulkBody(t *testing.T) {
	items := []map[string]any{
		{
			"_id":     "1",
			"_type":   "doc",
			"_source": map[string]any{"name": "alice"},
		},
		{
			"name": "bob",
		},
	}

	body, err := buildBulkBody(items)
	if err != nil {
		t.Fatalf("buildBulkBody error: %v", err)
	}

	lines := strings.Split(strings.TrimSpace(string(body)), "\n")
	if len(lines) != 4 {
		t.Fatalf("expected 4 lines, got %d: %s", len(lines), body)
	}

	var action map[string]map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &action); err != nil {
		t.Fatalf("unmarshal action line: %v", err)
	}

	meta := action["index"]
	if meta["_id"] != "1" || meta["_type"] != "doc" {
		t.Errorf("unexpected action meta: %v", meta)
	}

	var source map[string]any
	if err := json.Unmarshal([]byte(lines[1]), &source); err != nil {
		t.Fatalf("unmarshal source line: %v", err)
	}
	if source["name"] != "alice" {
		t.Errorf("source = %v, want alice", source)
	}

	var action2 map[string]map[string]any
	if err := json.Unmarshal([]byte(lines[2]), &action2); err != nil {
		t.Fatalf("unmarshal action line 2: %v", err)
	}
	meta2 := action2["index"]
	if _, ok := meta2["_id"]; ok {
		t.Errorf("plain doc should not carry an _id, got %v", meta2)
	}
	if meta2["_type"] != DefaultType {
		t.Errorf("plain doc type = %v, want %q", meta2["_type"], DefaultType)
	}
}

func TestCountBulkFailures(t *testing.T) {
	okBody := []byte(`{"took":1,"errors":false,"items":[{"index":{"_index":"i","_type":"doc","_id":"1","status":201}},{"index":{"_index":"i","_type":"doc","_id":"2","status":201}}]}`)
	failed, err := countBulkFailures(okBody)
	if err != nil {
		t.Fatalf("countBulkFailures error: %v", err)
	}
	if failed != 0 {
		t.Errorf("failed = %d, want 0", failed)
	}

	badBody := []byte(`{"took":1,"errors":true,"items":[{"index":{"_index":"i","_type":"doc","_id":"1","status":201}},{"index":{"_index":"i","_type":"doc","_id":"2","status":400,"error":{"type":"mapper_parsing_exception"}}}]}`)
	failed, err = countBulkFailures(badBody)
	if err != nil {
		t.Fatalf("countBulkFailures error: %v", err)
	}
	if failed != 1 {
		t.Errorf("failed = %d, want 1", failed)
	}
}
