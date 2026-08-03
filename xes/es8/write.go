package es8

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"github.com/elastic/go-elasticsearch/v8/esutil"
	"github.com/loveuer/esgo2dump/internal/opt"
	"github.com/loveuer/esgo2dump/internal/tool"
	"github.com/loveuer/esgo2dump/pkg/log"
	"github.com/loveuer/esgo2dump/pkg/model"
)

// WriteData implements model.IO. ES 8.x is typeless; _type from old dumps
// (5.x/6.x) is dropped here.
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
			log.Error("es8.writer: on error log, err = %s", err.Error())
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
		id, _, src, _ := model.UnwrapItem(item)

		var bs []byte

		if bs, err = json.Marshal(src); err != nil {
			return 0, err
		}

		if err = indexer.Add(context.Background(), esutil.BulkIndexerItem{
			Action:     "index",
			Index:      s.index,
			DocumentID: id,
			Body:       bytes.NewReader(bs),
			OnFailure: func(ctx context.Context, item esutil.BulkIndexerItem, item2 esutil.BulkIndexerResponseItem, bulkErr error) {
				log.Error("es8.writer: on failure err log, err = %s", bulkErr.Error())
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

// convertMapping normalizes an index-level mapping ({mappings: {...}}) for es8:
//   - unwraps the single mapping type layer (doc/_doc) from 5.x/6.x dumps,
//     since es8 mappings are typeless;
//   - drops _all, which was removed in ES 6.
func convertMapping(mapping map[string]any) (map[string]any, error) {
	result := make(map[string]any, len(mapping))
	for k, v := range mapping {
		result[k] = v
	}

	raw, ok := result["mappings"].(map[string]any)
	if !ok {
		return result, nil
	}

	delete(raw, "_all")

	if _, hasProps := raw["properties"]; hasProps {
		return result, nil
	}

	var typeMapping map[string]any
	for _, tv := range raw {
		tm, ok := tv.(map[string]any)
		if !ok {
			continue
		}

		if typeMapping != nil {
			return nil, fmt.Errorf("mapping contains multiple types, cannot convert to typeless es8 mapping")
		}
		typeMapping = tm
	}

	if typeMapping != nil {
		delete(typeMapping, "_all")
		result["mappings"] = typeMapping
	}

	return result, nil
}

// WriteMapping creates the index with the dumped mapping body.
func (s *streamer) WriteMapping(ctx context.Context, mapping map[string]any) error {
	for idxKey := range mapping {
		idxMap, ok := mapping[idxKey].(map[string]any)
		if !ok {
			continue
		}

		converted, err := convertMapping(idxMap)
		if err != nil {
			return err
		}

		bs, err := json.Marshal(converted)
		if err != nil {
			return err
		}

		result, err := s.client.Indices.Create(
			s.index,
			s.client.Indices.Create.WithContext(tool.TimeoutCtx(ctx, opt.Timeout)),
			s.client.Indices.Create.WithBody(bytes.NewReader(bs)),
		)
		if err != nil {
			return err
		}

		if result.StatusCode != 200 {
			return fmt.Errorf("status=%d, msg=%s", result.StatusCode, result.String())
		}
	}

	return nil
}

// WriteSetting updates index settings, filtering out immutable keys.
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
