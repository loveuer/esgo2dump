package es7

import (
	"testing"

	"github.com/loveuer/esgo2dump/internal/tool"
)

func TestNewClient(t *testing.T) {
	uri := "http://es1.dev:9200,es2.dev:9200"

	c, err := NewClient(tool.Timeout(5), uri)
	if err != nil {
		t.Fatal(err.Error())
	}

	t.Log("success!!!")
	_ = c
}
