package es6

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"time"

	elastic "github.com/elastic/go-elasticsearch/v6"
	"github.com/elastic/go-elasticsearch/v6/esapi"
	"github.com/loveuer/esgo2dump/internal/tool"
	"github.com/loveuer/esgo2dump/model"
	"github.com/loveuer/nf/nft/log"
	"github.com/samber/lo"
)

func ReadData(ctx context.Context, client *elastic.Client, index string, size, max int, query map[string]any, source []string, sort []string) (<-chan []*model.ESSource, <-chan error) {
	var (
		dataCh = make(chan []*model.ESSource)
		errCh  = make(chan error)
	)

	go func() {
		var (
			err      error
			resp     *esapi.Response
			result   = new(model.ESResponseV6)
			scrollId string
			total    int
		)

		defer func() {
			close(dataCh)

			if scrollId != "" {
				bs, _ := json.Marshal(map[string]string{
					"scroll_id": scrollId,
				})

				var rr *esapi.Response

				if rr, err = client.ClearScroll(
					client.ClearScroll.WithContext(tool.Timeout(3)),
					client.ClearScroll.WithBody(bytes.NewReader(bs)),
				); err != nil {
					log.Warn("clear scroll id=%s err=%v", scrollId, err)
					return
				}

				if rr.StatusCode != 200 {
					log.Warn("clear scroll id=%s status=%d msg=%s", scrollId, rr.StatusCode, rr.String())
				}
			}
		}()

		if client == nil {
			errCh <- fmt.Errorf("client is nil")
		}

		qs := []func(*esapi.SearchRequest){
			client.Search.WithContext(tool.TimeoutCtx(ctx, 20)),
			client.Search.WithIndex(index),
			client.Search.WithSize(int(size)),
			client.Search.WithFrom(0),
			client.Search.WithScroll(time.Duration(120) * time.Second),
		}

		if len(source) > 0 {
			qs = append(qs, client.Search.WithSourceIncludes(source...))
		}

		if len(sort) > 0 {
			sorts := lo.Filter(sort, func(item string, index int) bool {
				return item != ""
			})

			if len(sorts) > 0 {
				qs = append(qs, client.Search.WithSort(sorts...))
			}
		}

		if query != nil && len(query) > 0 {
			queryBs, _ := json.Marshal(map[string]any{"query": query})
			qs = append(qs, client.Search.WithBody(bytes.NewReader(queryBs)))
		}

		if resp, err = client.Search(qs...); err != nil {
			errCh <- err
			return
		}

		if resp.StatusCode != 200 {
			errCh <- fmt.Errorf("resp status=%d, resp=%s", resp.StatusCode, resp.String())
			return
		}

		decoder := json.NewDecoder(resp.Body)
		if err = decoder.Decode(result); err != nil {
			errCh <- err
			return
		}

		scrollId = result.ScrollId

		dataCh <- result.Hits.Hits
		total += len(result.Hits.Hits)

		if len(result.Hits.Hits) < size || (max > 0 && total >= max) {
			return
		}

		for {
			if resp, err = client.Scroll(
				client.Scroll.WithScrollID(scrollId),
				client.Scroll.WithScroll(time.Duration(120)*time.Second),
			); err != nil {
				errCh <- err
				return
			}

			result = new(model.ESResponseV6)

			decoder = json.NewDecoder(resp.Body)
			if err = decoder.Decode(result); err != nil {
				errCh <- err
				return
			}

			if resp.StatusCode != 200 {
				errCh <- fmt.Errorf("resp status=%d, resp=%s", resp.StatusCode, resp.String())
				return
			}

			dataCh <- result.Hits.Hits
			total += len(result.Hits.Hits)

			if len(result.Hits.Hits) < size || (max > 0 && total >= max) {
				break
			}
		}
	}()

	return dataCh, errCh
}
