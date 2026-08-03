package es8

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"time"

	elastic "github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	"github.com/loveuer/esgo2dump/internal/opt"
	"github.com/loveuer/esgo2dump/internal/tool"
	"github.com/loveuer/esgo2dump/pkg/log"
	"github.com/loveuer/esgo2dump/pkg/model"
	"github.com/samber/lo"
)

type streamer struct {
	ctx    context.Context
	client *elastic.Client
	index  string
	scroll string
}

func (s *streamer) Cleanup() {
	defer func() { s.scroll = "" }()

	if s.scroll == "" {
		return
	}

	res, err := s.client.ClearScroll(
		s.client.ClearScroll.WithContext(s.ctx),
		s.client.ClearScroll.WithScrollID(s.scroll),
	)
	if err != nil {
		log.Warn("cleanup scroll failed, err = %s", err.Error())
		return
	}

	if res.StatusCode != 200 {
		log.Warn("cleanup scroll failed, message = %s", res.String())
	}
}

// ReadData implements model.IO. ES 8.x is typeless, so returned items contain
// _id/_index/_source.
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
		bm := map[string]any{
			"scroll":    "35s",
			"scroll_id": s.scroll,
		}

		bs, _ := json.Marshal(bm)

		if resp, err = s.client.Scroll(
			s.client.Scroll.WithContext(tool.TimeoutCtx(s.ctx)),
			s.client.Scroll.WithBody(bytes.NewReader(bs)),
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

	return lo.Map(
		result.Hits.Hits,
		func(item *model.ESSource[map[string]any], _ int) map[string]any {
			return map[string]any{
				"_id":     item.DocId,
				"_index":  item.Index,
				"_source": item.Content,
			}
		},
	), nil
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

func NewStreamer(ctx context.Context, client *elastic.Client, index string) (model.IO[map[string]any], error) {
	s := &streamer{ctx: ctx, client: client, index: index}
	return s, nil
}
