package xes

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	elastic "github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	"github.com/elastic/go-elasticsearch/v7/esutil"
	"github.com/loveuer/esgo2dump/internal/interfaces"
	"github.com/loveuer/esgo2dump/internal/opt"
	"github.com/loveuer/esgo2dump/internal/util"
	"github.com/loveuer/esgo2dump/model"
	"github.com/loveuer/esgo2dump/xes/es7"
	"net/url"
	"strings"
)

type client struct {
	client *elastic.Client
	iot    interfaces.IO
	index  string
}

func NewClient(url *url.URL, iot interfaces.IO) (interfaces.DumpIO, error) {

	var (
		urlIndex = strings.TrimPrefix(url.Path, "/")
		cli      *elastic.Client
		err      error
	)

	if urlIndex == "" {
		return nil, fmt.Errorf("please specify index name: (like => http://127.0.0.1:9200/my_index)")
	}

	if cli, err = es7.NewClient(context.TODO(), url); err != nil {
		return nil, err
	}

	return &client{client: cli, iot: iot, index: urlIndex}, nil
}

func (c *client) checkResponse(r *esapi.Response) error {
	if r.StatusCode == 200 {
		return nil
	}

	return fmt.Errorf("status=%d msg=%s", r.StatusCode, r.String())
}

func (c *client) IOType() interfaces.IO {
	return c.iot
}

func (c *client) IsFile() bool {
	return false
}

func (c *client) Close() error {
	return nil
}

func (c *client) WriteData(ctx context.Context, docs []*model.ESSource) (int, error) {
	var (
		err     error
		indexer esutil.BulkIndexer
		count   int
		be      error
	)
	if indexer, err = esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
		Client:     c.client,
		Index:      c.index,
		ErrorTrace: true,
		OnError: func(ctx context.Context, err error) {

		},
	}); err != nil {
		return 0, err
	}

	for _, doc := range docs {
		var bs []byte

		if bs, err = json.Marshal(doc.Content); err != nil {
			return 0, err
		}

		if err = indexer.Add(context.Background(), esutil.BulkIndexerItem{
			Action:     "index",
			Index:      c.index,
			DocumentID: doc.DocId,
			Body:       bytes.NewReader(bs),
			OnFailure: func(ctx context.Context, item esutil.BulkIndexerItem, item2 esutil.BulkIndexerResponseItem, bulkErr error) {
				be = bulkErr
			},
		}); err != nil {
			return 0, err
		}
		count++
	}

	if err = indexer.Close(util.TimeoutCtx(ctx, opt.Timeout)); err != nil {
		return 0, err
	}

	if be != nil {
		return 0, be
	}

	stats := indexer.Stats()
	if stats.NumFailed > 0 {
		return count, fmt.Errorf("write to xes failed_count=%d bulk_count=%d", stats.NumFailed, count)
	}

	return count, nil
}

func (c *client) ReadData(ctx context.Context, size int, query map[string]any, source []string) (<-chan []*model.ESSource, <-chan error) {
	dch, ech := es7.ReadData(ctx, c.client, c.index, size, 0, query, source)

	return dch, ech
}

func (c *client) ReadMapping(ctx context.Context) (map[string]any, error) {
	r, err := c.client.Indices.GetMapping(
		c.client.Indices.GetMapping.WithIndex(c.index),
	)
	if err != nil {
		return nil, err
	}

	if r.StatusCode != 200 {
		return nil, fmt.Errorf("status=%d, msg=%s", r.StatusCode, r.String())
	}

	m := make(map[string]any)
	decoder := json.NewDecoder(r.Body)
	if err = decoder.Decode(&m); err != nil {
		return nil, err
	}

	return m, nil
}
func (c *client) WriteMapping(ctx context.Context, m map[string]any) error {
	var (
		err    error
		bs     []byte
		result *esapi.Response
	)

	for idxKey := range m {
		if bs, err = json.Marshal(m[idxKey]); err != nil {
			return err
		}

		if result, err = c.client.Indices.Create(
			c.index,
			c.client.Indices.Create.WithContext(util.TimeoutCtx(ctx, opt.Timeout)),
			c.client.Indices.Create.WithBody(bytes.NewReader(bs)),
		); err != nil {
			return err
		}

		if err = c.checkResponse(result); err != nil {
			return err
		}
	}

	return nil
}

func (c *client) ReadSetting(ctx context.Context) (map[string]any, error) {
	r, err := c.client.Indices.GetSettings(
		c.client.Indices.GetSettings.WithContext(util.TimeoutCtx(ctx, opt.Timeout)),
		c.client.Indices.GetSettings.WithIndex(c.index),
	)
	if err != nil {
		return nil, err
	}

	if r.StatusCode != 200 {
		return nil, fmt.Errorf("status=%d, msg=%s", r.StatusCode, r.String())
	}

	m := make(map[string]any)
	decoder := json.NewDecoder(r.Body)
	if err = decoder.Decode(&m); err != nil {
		return nil, err
	}

	return m, nil
}

func (c *client) WriteSetting(ctx context.Context, m map[string]any) error {
	var (
		err    error
		bs     []byte
		result *esapi.Response
	)

	if bs, err = json.Marshal(m); err != nil {
		return err
	}

	if result, err = c.client.Indices.PutSettings(
		bytes.NewReader(bs),
		c.client.Indices.PutSettings.WithContext(util.TimeoutCtx(ctx, opt.Timeout)),
	); err != nil {
		return err
	}

	return c.checkResponse(result)
}
