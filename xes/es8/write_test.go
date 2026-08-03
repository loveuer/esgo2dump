package es8

import (
	"encoding/json"
	"testing"
)

func TestConvertMapping(t *testing.T) {
	tests := []struct {
		name     string
		mapping  map[string]any
		wantErr  bool
		wantBody string
	}{
		{
			name: "5.x/6.x style single type unwraps",
			mapping: map[string]any{
				"mappings": map[string]any{
					"doc": map[string]any{
						"_all": map[string]any{"enabled": false},
						"properties": map[string]any{
							"name": map[string]any{"type": "keyword"},
						},
					},
				},
			},
			wantErr:  false,
			wantBody: `{"mappings":{"properties":{"name":{"type":"keyword"}}}}`,
		},
		{
			name: "_doc type unwraps and keeps meta keys",
			mapping: map[string]any{
				"mappings": map[string]any{
					"_doc": map[string]any{
						"properties": map[string]any{
							"age": map[string]any{"type": "integer"},
						},
					},
				},
			},
			wantErr:  false,
			wantBody: `{"mappings":{"properties":{"age":{"type":"integer"}}}}`,
		},
		{
			name: "typeless mapping passes through",
			mapping: map[string]any{
				"mappings": map[string]any{
					"properties": map[string]any{
						"name": map[string]any{"type": "keyword"},
					},
				},
			},
			wantErr:  false,
			wantBody: `{"mappings":{"properties":{"name":{"type":"keyword"}}}}`,
		},
		{
			name: "multiple types rejected",
			mapping: map[string]any{
				"mappings": map[string]any{
					"doc":  map[string]any{"properties": map[string]any{}},
					"logs": map[string]any{"properties": map[string]any{}},
				},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := convertMapping(tt.mapping)
			if (err != nil) != tt.wantErr {
				t.Fatalf("convertMapping() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}

			bs, _ := json.Marshal(got)
			if string(bs) != tt.wantBody {
				t.Errorf("convertMapping() body = %s, want %s", bs, tt.wantBody)
			}
		})
	}
}
