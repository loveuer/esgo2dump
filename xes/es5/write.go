package es5

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"github.com/go-resty/resty/v2"
	"github.com/loveuer/esgo2dump/internal/opt"
	"github.com/loveuer/esgo2dump/internal/tool"
	"github.com/loveuer/esgo2dump/pkg/model"
)

// DefaultType is used when an item carries no _type (e.g. plain file input).
// ES 5.x bulk requires an explicit mapping type.
const DefaultType = "doc"

// unwrapItem splits a dump item into document id / type / source.
func unwrapItem(item map[string]any) (id string, typ string, src map[string]any) {
	id, typ, src, _ = model.UnwrapItem(item)
	if typ == "" {
		typ = DefaultType
	}
	return
}

// buildBulkBody converts items into the NDJSON body of an ES 5.x bulk request.
func buildBulkBody(items []map[string]any) ([]byte, error) {
	var buf bytes.Buffer

	for _, item := range items {
		id, typ, src := unwrapItem(item)

		meta := map[string]any{
			"_type": typ,
		}
		if id != "" {
			meta["_id"] = id
		}

		action := map[string]any{
			"index": meta,
		}

		actionBs, err := json.Marshal(action)
		if err != nil {
			return nil, err
		}

		srcBs, err := json.Marshal(src)
		if err != nil {
			return nil, err
		}

		buf.Write(actionBs)
		buf.WriteByte('\n')
		buf.Write(srcBs)
		buf.WriteByte('\n')
	}

	return buf.Bytes(), nil
}

type bulkItemResult struct {
	Status int `json:"status"`
	Error  any `json:"error"`
}

// countBulkFailures returns how many bulk items failed.
func countBulkFailures(body []byte) (int, error) {
	var result struct {
		Errors bool                        `json:"errors"`
		Items  []map[string]bulkItemResult `json:"items"`
	}

	if err := json.Unmarshal(body, &result); err != nil {
		return 0, err
	}

	failed := 0
	for _, item := range result.Items {
		for _, r := range item {
			if r.Error != nil || r.Status >= 300 {
				failed++
			}
		}
	}

	return failed, nil
}

// WriteData implements model.IO.
func (s *streamer) WriteData(ctx context.Context, items []map[string]any) (int, error) {
	if len(items) == 0 {
		return 0, nil
	}

	body, err := buildBulkBody(items)
	if err != nil {
		return 0, err
	}

	var resp *resty.Response
	if resp, err = s.client.c.R().
		SetContext(ctx).
		SetHeader("Content-Type", "application/x-ndjson").
		SetBody(body).
		Post("/" + s.index + "/_bulk"); err != nil {
		return 0, err
	}

	if resp.StatusCode() != 200 {
		return 0, fmt.Errorf("bulk status=%d, msg=%s", resp.StatusCode(), resp.String())
	}

	failed, err := countBulkFailures(resp.Body())
	if err != nil {
		return 0, err
	}

	return len(items) - failed, nil
}

// WriteMapping creates the index with the dumped mapping body.
// mapping is the raw GetMapping response: {index: {mappings: {...}}}.
func (s *streamer) WriteMapping(ctx context.Context, mapping map[string]any) error {
	for idxKey := range mapping {
		bs, err := json.Marshal(mapping[idxKey])
		if err != nil {
			return err
		}

		resp, err := s.client.c.R().
			SetContext(tool.TimeoutCtx(ctx, opt.Timeout)).
			SetHeader("Content-Type", "application/json").
			SetBody(bs).
			Put("/" + s.index)
		if err != nil {
			return err
		}

		if resp.StatusCode() >= 300 {
			return fmt.Errorf("create index status=%d, msg=%s", resp.StatusCode(), resp.String())
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

	resp, err := s.client.c.R().
		SetContext(tool.TimeoutCtx(ctx, opt.Timeout)).
		SetHeader("Content-Type", "application/json").
		SetBody(bs).
		Put("/" + s.index + "/_settings")
	if err != nil {
		return err
	}

	if resp.StatusCode() != 200 {
		return fmt.Errorf("put settings status=%d, msg=%s", resp.StatusCode(), resp.String())
	}

	return nil
}
