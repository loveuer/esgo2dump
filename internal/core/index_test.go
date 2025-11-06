package core

import (
	"testing"
)

func TestExtractIndexName(t *testing.T) {
	tests := []struct {
		name     string
		inputURI string
		want     string
	}{
		{
			name:     "valid ES URL with index",
			inputURI: "http://127.0.0.1:9200/my_index",
			want:     "my_index",
		},
		{
			name:     "valid ES URL with index and query params",
			inputURI: "http://127.0.0.1:9200/my_index?ping=false",
			want:     "my_index",
		},
		{
			name:     "valid HTTPS ES URL",
			inputURI: "https://user:pass@127.0.0.1:9200/my_index",
			want:     "my_index",
		},
		{
			name:     "valid ES URL with multiple indices pattern",
			inputURI: "http://127.0.0.1:9200/index1,index2",
			want:     "index1,index2",
		},
		{
			name:     "invalid scheme - file path",
			inputURI: "/path/to/file.json",
			want:     "",
		},
		{
			name:     "invalid scheme - relative path",
			inputURI: "./data.json",
			want:     "",
		},
		{
			name:     "empty path",
			inputURI: "http://127.0.0.1:9200/",
			want:     "",
		},
		{
			name:     "no path",
			inputURI: "http://127.0.0.1:9200",
			want:     "",
		},
		{
			name:     "invalid URL",
			inputURI: "://invalid",
			want:     "",
		},
		{
			name:     "index with special characters",
			inputURI: "http://127.0.0.1:9200/my-index_123",
			want:     "my-index_123",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ExtractIndexName(tt.inputURI)
			if got != tt.want {
				t.Errorf("ExtractIndexName() = %v, want %v", got, tt.want)
			}
		})
	}
}
