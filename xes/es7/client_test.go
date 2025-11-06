package es7

import (
	"testing"

	"github.com/loveuer/esgo2dump/internal/tool"
)

// TestNewClient tests client creation (requires actual ES server)
// This test is skipped by default and can be run with: go test -tags=integration
func TestNewClient(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}
	
	uri := "http://es1.dev:9200,es2.dev:9200"

	c, err := NewClient(tool.Timeout(5), uri)
	if err != nil {
		t.Skipf("Skipping test - ES server not available: %v", err)
		return
	}

	t.Log("success!!!")
	_ = c
}

// TestNewClient_InvalidURI tests error handling for invalid URIs
func TestNewClient_InvalidURI(t *testing.T) {
	tests := []struct {
		name string
		uri  string
	}{
		{"invalid URL", "://invalid"},
		{"empty URI", ""},
		{"invalid scheme", "ftp://example.com"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewClient(tool.Timeout(5), tt.uri)
			if err == nil {
				t.Errorf("NewClient() with invalid URI should return error, got nil")
			}
		})
	}
}
