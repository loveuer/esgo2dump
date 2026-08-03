package es8

import (
	"testing"

	"github.com/loveuer/esgo2dump/internal/tool"
)

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
