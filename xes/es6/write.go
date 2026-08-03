package es6

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"github.com/elastic/go-elasticsearch/v6/esapi"
	"github.com/elastic/go-elasticsearch/v6/esutil"
	"github.com/loveuer/esgo2dump/internal/opt"
	"github.com/loveuer/esgo2dump/internal/tool"
	"github.com/loveuer/esgo2dump/pkg/log"
	"github.com/loveuer/esgo2dump/pkg/model"
)

// DefaultType is used when an item carries no _type (e.g. plain file input).
const DefaultType = "doc"

// WriteData implements model.IO. ES 6.x is single-type per index, but the
// _type from dumps (e.g. from 5.x) is preserved for lossless migration.
func (s *streamer) WriteData(ctx context.Context, items []map[string]any) (int, error) {
	var (
		err     error
		indexer esutil.BulkIndexer
	)

	if len(items) == 0 {
		return 0, nil
	}

	if indexer, err = esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
		NumWorkers:    0,
		FlushBytes:    0,
		FlushInterval: 0,
		Client:        s.client,
		OnError: func(ctx context.Context, err error) {
			log.Error("es6.writer: on error log, err = %s", err.Error())
		},
		Index:          s.index,
		DocumentType:   DefaultType,
		ErrorTrace:     true,
		FilterPath:     []string{},
		Header:         map[string][]string{},
		Human:          false,
		Pipeline:       "",
		Pretty:         false,
		Refresh:        "",
		Routing:        "",
		Source:         []string{},
		SourceExcludes: []string{},
		SourceIncludes: []string{},
		Timeout:        0,
	}); err != nil {
		return 0, err
	}

	for _, item := range items {
		id, typ, src, _ := model.UnwrapItem(item)
		if typ == "" {
			typ = DefaultType
		}

		var bs []byte

		if bs, err = json.Marshal(src); err != nil {
			return 0, err
		}

		if err = indexer.Add(context.Background(), esutil.BulkIndexerItem{
			Action:       "index",
			Index:        s.index,
			DocumentID:   id,
			DocumentType: typ,
			Body:         bytes.NewReader(bs),
			OnFailure: func(ctx context.Context, item esutil.BulkIndexerItem, item2 esutil.BulkIndexerResponseItem, bulkErr error) {
				log.Error("es6.writer: on failure err log, err = %s", bulkErr.Error())
			},
		}); err != nil {
			return 0, err
		}
	}

	if err = indexer.Close(ctx); err != nil {
		return 0, err
	}

	stats := indexer.Stats()

	return len(items) - int(stats.NumFailed), nil
}

// WriteMapping creates the index with the dumped mapping body.
// mapping is the raw GetMapping response: {index: {mappings: {...}}}.
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

// WriteSetting updates index settings, filtering out immutable keys.
// setting is the raw GetSettings response: {index: {settings: {...}}}.
func (s *streamer) WriteSetting(ctx context.Context, setting map[string]any) error {
	payload := model.FilterUpdatableSettings(setting)

	if len(payload) == 0 {
		return nil
	}

	bs, err := json.Marshal(map[string]any{"index": payload})
	if err != nil {
		return err
	}

	result, err := s.client.Indices.PutSettings(
		bytes.NewReader(bs),
		s.client.Indices.PutSettings.WithContext(tool.TimeoutCtx(ctx, opt.Timeout)),
	)
	if err != nil {
		return err
	}

	if result.StatusCode != 200 {
		return fmt.Errorf("status=%d, msg=%s", result.StatusCode, result.String())
	}

	return nil
}
