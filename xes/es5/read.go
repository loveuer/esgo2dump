package es5

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/go-resty/resty/v2"
	"github.com/loveuer/esgo2dump/internal/opt"
	"github.com/loveuer/esgo2dump/internal/tool"
	"github.com/loveuer/esgo2dump/pkg/log"
	"github.com/loveuer/esgo2dump/pkg/model"
	"github.com/samber/lo"
)

type streamer struct {
	ctx    context.Context
	client *Client
	index  string
	scroll string
}

type es5Hits struct {
	Total int                    `json:"total"`
	Hits  []*model.ESSource[any] `json:"hits"`
}

type es5SearchResponse struct {
	ScrollId string  `json:"_scroll_id"`
	Hits     es5Hits `json:"hits"`
}

func (s *streamer) Cleanup() {
	defer func() { s.scroll = "" }()

	if s.scroll == "" {
		return
	}

	bs, _ := json.Marshal(map[string]any{"scroll_id": []string{s.scroll}})

	resp, err := s.client.c.R().
		SetContext(s.ctx).
		SetHeader("Content-Type", "application/json").
		SetBody(bs).
		Delete("/_search/scroll")
	if err != nil {
		log.Warn("cleanup scroll failed, err = %s", err.Error())
		return
	}

	if resp.StatusCode() != 200 {
		log.Warn("cleanup scroll failed, message = %s", resp.String())
	}
}

// ReadData implements model.IO. ES 5.x docs carry a mandatory _type,
// so returned items contain _id/_index/_type/_source.
func (s *streamer) ReadData(ctx context.Context, limit int, query map[string]any, fields []string, sort []string) ([]map[string]any, error) {
	var (
		err  error
		resp *resty.Response
	)

	if limit == 0 {
		return nil, nil
	}

	if s.scroll != "" {
		bs, _ := json.Marshal(map[string]any{
			"scroll":    "35s",
			"scroll_id": s.scroll,
		})

		resp, err = s.client.c.R().
			SetContext(tool.TimeoutCtx(s.ctx)).
			SetHeader("Content-Type", "application/json").
			SetBody(bs).
			Post("/_search/scroll")
	} else {
		qp := url.Values{}
		qp.Set("size", strconv.Itoa(limit))
		qp.Set("scroll", (35 * time.Second).String())

		// ES 5.x uses singular _source_include/_source_exclude params
		if len(fields) > 0 {
			qp.Set("_source_include", strings.Join(fields, ","))
		}

		if len(sort) > 0 {
			qp.Set("sort", strings.Join(sort, ","))
		}

		r := s.client.c.R().
			SetContext(tool.TimeoutCtx(s.ctx)).
			SetQueryParamsFromValues(qp).
			SetHeader("Content-Type", "application/json")

		if len(query) > 0 {
			queryBs, err := json.Marshal(map[string]any{"query": query})
			if err != nil {
				return nil, err
			}
			r = r.SetBody(queryBs)
		}

		resp, err = r.Post("/" + s.index + "/_search")
	}

	if err != nil {
		return nil, err
	}

	if resp.StatusCode() != 200 {
		return nil, fmt.Errorf("resp status=%d, resp=%s", resp.StatusCode(), resp.String())
	}

	result := new(es5SearchResponse)
	if err = json.Unmarshal(resp.Body(), result); err != nil {
		return nil, err
	}

	s.scroll = result.ScrollId

	return lo.Map(
		result.Hits.Hits,
		func(item *model.ESSource[any], _ int) map[string]any {
			return map[string]any{
				"_id":     item.DocId,
				"_index":  item.Index,
				"_type":   item.Type,
				"_source": item.Content,
			}
		},
	), nil
}

func (s *streamer) ReadMapping(ctx context.Context) (map[string]any, error) {
	resp, err := s.client.c.R().
		SetContext(tool.TimeoutCtx(ctx, opt.Timeout)).
		Get("/" + s.index + "/_mapping")
	if err != nil {
		return nil, err
	}

	if resp.StatusCode() != 200 {
		return nil, fmt.Errorf("status=%d, msg=%s", resp.StatusCode(), resp.String())
	}

	m := make(map[string]any)
	if err = json.Unmarshal(resp.Body(), &m); err != nil {
		return nil, err
	}

	return m, nil
}

func (s *streamer) ReadSetting(ctx context.Context) (map[string]any, error) {
	resp, err := s.client.c.R().
		SetContext(tool.TimeoutCtx(ctx, opt.Timeout)).
		Get("/" + s.index + "/_settings")
	if err != nil {
		return nil, err
	}

	if resp.StatusCode() != 200 {
		return nil, fmt.Errorf("status=%d, msg=%s", resp.StatusCode(), resp.String())
	}

	m := make(map[string]any)
	if err = json.Unmarshal(resp.Body(), &m); err != nil {
		return nil, err
	}

	return m, nil
}

func NewStreamer(ctx context.Context, client *Client, index string) (model.IO[map[string]any], error) {
	s := &streamer{ctx: ctx, client: client, index: index}
	return s, nil
}
