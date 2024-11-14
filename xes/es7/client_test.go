package es7

import (
	"github.com/loveuer/esgo2dump/internal/util"
	"net/url"
	"testing"
)

func TestNewClient(t *testing.T) {
	uri := "http://es1.dev:9200,es2.dev:9200"
	ins, _ := url.Parse(uri)

	c, err := NewClient(util.Timeout(5), ins)
	if err != nil {
		t.Fatal(err.Error())
	}

	t.Log("success!!!")
	_ = c
}
