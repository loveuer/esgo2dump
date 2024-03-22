package xes

import (
	"esgo2dump/internal/util"
	elastic "github.com/elastic/go-elasticsearch/v7"
	"testing"
)

func TestGetESMapping(t *testing.T) {
	endpoint := "http://127.0.0.1:9200"
	index := "some_index"

	cli, err := elastic.NewClient(elastic.Config{
		Addresses: []string{endpoint},
	})
	if err != nil {
		t.Error(1, err)
		return
	}

	resp, err := cli.Info(cli.Info.WithContext(util.Timeout(5)))
	if err != nil {
		t.Error(2, err)
		return
	}

	t.Log("info:", resp.String())

	r, err := cli.Indices.GetMapping(
		cli.Indices.GetMapping.WithIndex(index),
	)
	if err != nil {
		t.Error(3, err)
		return
	}

	t.Log("get source:", r.String())
}
