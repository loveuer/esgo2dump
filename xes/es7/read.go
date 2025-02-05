package es7

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/elastic/go-elasticsearch/v7/esutil"
	"github.com/loveuer/esgo2dump/internal/opt"
	"github.com/loveuer/esgo2dump/pkg/log"
	"time"

	elastic "github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	"github.com/loveuer/esgo2dump/internal/tool"
	"github.com/loveuer/esgo2dump/pkg/model"
	"github.com/samber/lo"
)

type streamer struct {
	ctx    context.Context
	client *elastic.Client
	index  string
	scroll string
}

// ReadData implements model.IO.
func (s *streamer) ReadData(ctx context.Context, limit int, query map[string]any, fields []string, sort []string) ([]map[string]any, error) {
	var (
		err    error
		qs     []func(*esapi.SearchRequest)
		resp   *esapi.Response
		result = new(model.ESResponseV7[map[string]any])
	)

	if limit == 0 {
		return nil, nil
	}

	if s.scroll != "" {
		if resp, err = s.client.Scroll(
			s.client.Scroll.WithContext(tool.TimeoutCtx(s.ctx)),
			s.client.Scroll.WithScrollID(s.scroll),
			s.client.Scroll.WithScroll(35*time.Second),
		); err != nil {
			return nil, err
		}

		goto HandleResp
	}

	qs = []func(*esapi.SearchRequest){
		s.client.Search.WithContext(tool.TimeoutCtx(s.ctx)),
		s.client.Search.WithIndex(s.index),
		s.client.Search.WithSize(limit),
		s.client.Search.WithScroll(35 * time.Second),
	}

	if len(fields) > 0 {
		qs = append(qs, s.client.Search.WithSourceIncludes(fields...))
	}

	if len(sort) > 0 {
		qs = append(qs, s.client.Search.WithSort(sort...))
	}

	if len(query) > 0 {
		queryBs, err := json.Marshal(map[string]any{"query": query})
		if err != nil {
			return nil, err
		}

		qs = append(qs, s.client.Search.WithBody(bytes.NewReader(queryBs)))
	}

	if resp, err = s.client.Search(qs...); err != nil {
		return nil, err
	}

HandleResp:

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("resp status=%d, resp=%s", resp.StatusCode, resp.String())
	}

	if err = json.NewDecoder(resp.Body).Decode(result); err != nil {
		return nil, err
	}

	s.scroll = result.ScrollId

	return lo.Slice(
		lo.Map(
			result.Hits.Hits,
			func(item *model.ESSource[map[string]any], _ int) map[string]any {
				return item.Content
			},
		),
		0,
		limit,
	), nil
}

// WriteData implements model.IO.
func (s *streamer) WriteData(ctx context.Context, items []map[string]any) (int, error) {
	var (
		err     error
		indexer esutil.BulkIndexer
		total   int
	)

	if len(items) == 0 {
		return 0, nil
	}

	count := 0

	if indexer, err = esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
		NumWorkers:    0,
		FlushBytes:    0,
		FlushInterval: 0,
		Client:        s.client,
		Decoder:       nil,
		OnError: func(ctx context.Context, err error) {
			log.Error("es7.writer: on error log, err = %s", err.Error())
		},
		Index:               s.index,
		ErrorTrace:          true,
		FilterPath:          []string{},
		Header:              map[string][]string{},
		Human:               false,
		Pipeline:            "",
		Pretty:              false,
		Refresh:             "",
		Routing:             "",
		Source:              []string{},
		SourceExcludes:      []string{},
		SourceIncludes:      []string{},
		Timeout:             0,
		WaitForActiveShards: "",
	}); err != nil {
		return 0, err
	}

	for _, item := range items {
		var bs []byte

		if bs, err = json.Marshal(item); err != nil {
			return 0, err
		}

		if err = indexer.Add(context.Background(), esutil.BulkIndexerItem{
			Action: "index",
			Index:  s.index,
			Body:   bytes.NewReader(bs),
			OnFailure: func(ctx context.Context, item esutil.BulkIndexerItem, item2 esutil.BulkIndexerResponseItem, bulkErr error) {
				log.Error("es7.writer: on failure err log, err = %s", bulkErr.Error())
			},
		}); err != nil {
			return 0, err
		}

		count++
	}

	total += count

	if err = indexer.Close(ctx); err != nil {
		return 0, err
	}

	stats := indexer.Stats()

	return len(items) - int(stats.NumFailed), nil
}

func (s *streamer) ReadMapping(ctx context.Context) (map[string]any, error) {
	r, err := s.client.Indices.GetMapping(
		s.client.Indices.GetMapping.WithIndex(s.index),
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

func (s *streamer) WriteMapping(ctx context.Context, mapping map[string]any) error {
	var (
		err    error
		bs     []byte
		result *esapi.Response
	)

	for idxKey := range mapping {
		if bs, err = json.Marshal(mapping[idxKey]); err != nil {
			return err
		}

		if result, err = s.client.Indices.Create(
			s.index,
			s.client.Indices.Create.WithContext(tool.TimeoutCtx(ctx, opt.Timeout)),
			s.client.Indices.Create.WithBody(bytes.NewReader(bs)),
		); err != nil {
			return err
		}

		if result.StatusCode != 200 {
			return fmt.Errorf("status=%d, msg=%s", result.StatusCode, result.String())
		}
	}

	return nil
}

func (s *streamer) ReadSetting(ctx context.Context) (map[string]any, error) {
	r, err := s.client.Indices.GetSettings(
		s.client.Indices.GetSettings.WithContext(tool.TimeoutCtx(ctx, opt.Timeout)),
		s.client.Indices.GetSettings.WithIndex(s.index),
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

func (s *streamer) WriteSetting(ctx context.Context, setting map[string]any) error {
	var (
		err    error
		bs     []byte
		result *esapi.Response
	)

	if bs, err = json.Marshal(setting); err != nil {
		return err
	}

	if result, err = s.client.Indices.PutSettings(
		bytes.NewReader(bs),
		s.client.Indices.PutSettings.WithContext(tool.TimeoutCtx(ctx, opt.Timeout)),
	); err != nil {
		return err
	}

	if result.StatusCode != 200 {
		return fmt.Errorf("status=%d, msg=%s", result.StatusCode, result.String())
	}

	return nil
}

func NewStreamer(ctx context.Context, client *elastic.Client, index string) (model.IO[map[string]any], error) {
	s := &streamer{ctx: ctx, client: client, index: index}
	return s, nil
}
