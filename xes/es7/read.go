package es7

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	elastic "github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	"github.com/loveuer/esgo2dump/internal/util"
	"github.com/loveuer/esgo2dump/log"
	"github.com/loveuer/esgo2dump/model"
	"github.com/samber/lo"
	"time"
)

// ReadData
// Deprecated
// @param[source]: a list of include fields to extract and return from the _source field.
// @param[sort]:   a list of <field>:<direction> pairs.
func ReadData(ctx context.Context, client *elastic.Client, index string, size, max int, query map[string]any, source []string, sort []string) (<-chan []*model.ESSource, <-chan error) {
	var (
		dataCh = make(chan []*model.ESSource)
		errCh  = make(chan error)
	)

	go func() {
		var (
			err      error
			resp     *esapi.Response
			result   = new(model.ESResponseV7)
			scrollId string
			total    int
		)

		defer func() {
			close(dataCh)
			close(errCh)

			if scrollId != "" {
				bs, _ := json.Marshal(map[string]string{
					"scroll_id": scrollId,
				})

				var (
					rr *esapi.Response
				)

				if rr, err = client.ClearScroll(
					client.ClearScroll.WithContext(util.Timeout(3)),
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
			client.Search.WithContext(util.TimeoutCtx(ctx, 20)),
			client.Search.WithIndex(index),
			client.Search.WithSize(size),
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

			result = new(model.ESResponseV7)

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

// ReadDataV2 es7 read data
/*
	- @param[source]: a list of include fields to extract and return from the _source field.
    - @param[sort]:   a list of <field>:<direction> pairs.
*/
func ReadDataV2(
	ctx context.Context,
	client *elastic.Client,
	index string,
	size, max uint64,
	query map[string]any,
	source []string,
	sort []string,
) (<-chan []*model.ESSource, <-chan error) {
	var (
		dataCh = make(chan []*model.ESSource)
		errCh  = make(chan error)
	)

	go func() {
		var (
			err         error
			bs          []byte
			resp        *esapi.Response
			searchAfter        = make([]any, 0)
			total       uint64 = 0
			body               = make(map[string]any)
			qs          []func(request *esapi.SearchRequest)
		)

		if sort == nil {
			sort = []string{}
		}

		if query != nil && len(query) > 0 {
			body["query"] = query
		}

		sort = append(sort, "_id:ASC")

		sorts := lo.Filter(sort, func(item string, index int) bool {
			return item != ""
		})

		defer func() {
			close(dataCh)
			close(errCh)
		}()

		fina_size := util.AbsMin(size, max-total)
		log.Debug("es7.read: size = %d, max = %d, total = %d, fina size = %d", size, max, total, fina_size)

		for {
			qs = []func(*esapi.SearchRequest){
				client.Search.WithContext(util.TimeoutCtx(ctx, 30)),
				client.Search.WithIndex(index),
				client.Search.WithSize(int(fina_size)),
				client.Search.WithSort(sorts...),
			}

			if len(source) > 0 {
				qs = append(qs, client.Search.WithSourceIncludes(source...))
			}

			delete(body, "search_after")
			if len(searchAfter) > 0 {
				body["search_after"] = searchAfter
			}

			if bs, err = json.Marshal(body); err != nil {
				errCh <- err
				return
			}

			log.Debug("body raw: %s", string(bs))

			qs = append(qs, client.Search.WithBody(bytes.NewReader(bs)))
			if resp, err = client.Search(qs...); err != nil {
				errCh <- err
				return
			}

			if resp.StatusCode != 200 {
				errCh <- fmt.Errorf("resp status=%d, resp=%s", resp.StatusCode, resp.String())
				return
			}

			var result = new(model.ESResponseV7)
			decoder := json.NewDecoder(resp.Body)
			if err = decoder.Decode(result); err != nil {
				errCh <- err
				return
			}

			if resp.StatusCode != 200 {
				errCh <- fmt.Errorf("resp status=%d, resp=%s", resp.StatusCode, resp.String())
				return
			}

			dataCh <- result.Hits.Hits
			total += uint64(len(result.Hits.Hits))

			log.Debug("es7.read: total: %d", total)

			if uint64(len(result.Hits.Hits)) < size || (max > 0 && total >= max) {
				break
			}

			searchAfter = result.Hits.Hits[len(result.Hits.Hits)-1].Sort
		}

	}()

	return dataCh, errCh

}
