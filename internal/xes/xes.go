package xes

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	elastic "github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	"github.com/elastic/go-elasticsearch/v7/esutil"
	"github.com/loveuer/esgo2dump/internal/interfaces"
	"github.com/loveuer/esgo2dump/internal/opt"
	"github.com/loveuer/esgo2dump/internal/util"
	"github.com/sirupsen/logrus"
)

func NewClient(url *url.URL, iot interfaces.IO, qm map[string]any) (interfaces.DumpIO, error) {

	var (
		err      error
		endpoint = fmt.Sprintf("%s://%s", url.Scheme, url.Host)
		c        *elastic.Client
		infoResp *esapi.Response
		index    = strings.TrimPrefix(url.Path, "/")
		username string
		password string
	)

	if url.User != nil {
		username = url.User.Username()
		if p, ok := url.User.Password(); ok {
			password = p
		}
	}

	logrus.Debugf("xes.NewClient: endpoint=%s index=%s (username=%s password=%s)", endpoint, index, username, password)

	if index == "" {
		return nil, fmt.Errorf("please specify index name: (like => http://127.0.0.1:9200/my_index)")
	}

	if c, err = elastic.NewClient(
		elastic.Config{
			Addresses:     []string{endpoint},
			Username:      username,
			Password:      password,
			CACert:        nil,
			RetryOnStatus: []int{429},
			MaxRetries:    3,
			RetryBackoff:  nil,
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
				DialContext:     (&net.Dialer{Timeout: 5 * time.Second}).DialContext,
			},
		},
	); err != nil {
		logrus.Debugf("xes.NewClient: elastic new client with endpont=%s err=%v", endpoint, err)
		return nil, err
	}

	if infoResp, err = c.Info(); err != nil {
		logrus.Debugf("xes.NewClient: ping err=%v", err)
		return nil, err
	}

	if infoResp.StatusCode != 200 {
		return nil, fmt.Errorf("info xes status=%d", infoResp.StatusCode)
	}

	return &client{c: c, index: index, queryMap: qm, iot: iot}, nil
}

type client struct {
	c        *elastic.Client
	iot      interfaces.IO
	index    string
	from     int
	scrollId string
	queryMap map[string]any
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

func (c *client) WriteData(ctx context.Context, docs []*interfaces.ESSource) (int, error) {
	var (
		err     error
		indexer esutil.BulkIndexer
		count   int
	)
	if indexer, err = esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
		Client:  c.c,
		Index:   c.index,
		Refresh: "",
	}); err != nil {
		return 0, err
	}

	for _, doc := range docs {
		var bs []byte

		if bs, err = json.Marshal(doc.Content); err != nil {
			return 0, err
		}

		logrus.Debugf("xes.Write: doc content=%s", string(bs))

		if err = indexer.Add(context.Background(), esutil.BulkIndexerItem{
			Action:     "index",
			Index:      c.index,
			DocumentID: doc.DocId,
			Body:       bytes.NewReader(bs),
		}); err != nil {
			return 0, err
		}
		count++
	}

	if err = indexer.Close(util.TimeoutCtx(ctx, opt.Timeout)); err != nil {
		return 0, err
	}

	stats := indexer.Stats()
	if stats.NumFailed > 0 {
		return count, fmt.Errorf("write to xes failed=%d", stats.NumFailed)
	}

	return count, nil
}

func (c *client) ReadData(ctx context.Context, i int) ([]*interfaces.ESSource, error) {
	var (
		err    error
		resp   *esapi.Response
		result = new(interfaces.ESResponse)
	)

	if c.scrollId == "" {
		qs := []func(*esapi.SearchRequest){
			c.c.Search.WithContext(util.TimeoutCtx(ctx, opt.Timeout)),
			c.c.Search.WithIndex(c.index),
			c.c.Search.WithSize(i),
			c.c.Search.WithFrom(0),
			c.c.Search.WithScroll(time.Duration(opt.ScrollDurationSeconds) * time.Second),
		}

		if len(c.queryMap) > 0 {
			queryBs, _ := json.Marshal(map[string]any{"query": c.queryMap})
			qs = append(qs, c.c.Search.WithBody(bytes.NewReader(queryBs)))
		}

		if resp, err = c.c.Search(qs...); err != nil {
			return nil, err
		}

		if resp.StatusCode != 200 {
			return nil, fmt.Errorf(resp.String())
		}

		decoder := json.NewDecoder(resp.Body)
		if err = decoder.Decode(result); err != nil {
			return nil, err
		}

		c.scrollId = result.ScrollId

		return result.Hits.Hits, nil
	}

	if resp, err = c.c.Scroll(
		c.c.Scroll.WithScrollID(c.scrollId),
		c.c.Scroll.WithScroll(time.Duration(opt.ScrollDurationSeconds)*time.Second),
	); err != nil {
		return result.Hits.Hits, nil
	}

	decoder := json.NewDecoder(resp.Body)
	if err = decoder.Decode(result); err != nil {
		return nil, err
	}

	return result.Hits.Hits, nil
}

func (c *client) ReadMapping(ctx context.Context) (map[string]any, error) {
	r, err := c.c.Indices.GetMapping(
		c.c.Indices.GetMapping.WithIndex(c.index),
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

		if result, err = c.c.Indices.Create(
			c.index,
			c.c.Indices.Create.WithContext(util.TimeoutCtx(ctx, opt.Timeout)),
			c.c.Indices.Create.WithBody(bytes.NewReader(bs)),
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
	r, err := c.c.Indices.GetSettings(
		c.c.Indices.GetSettings.WithContext(util.TimeoutCtx(ctx, opt.Timeout)),
		c.c.Indices.GetSettings.WithIndex(c.index),
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

	if result, err = c.c.Indices.PutSettings(
		bytes.NewReader(bs),
		c.c.Indices.PutSettings.WithContext(util.TimeoutCtx(ctx, opt.Timeout)),
	); err != nil {
		return err
	}

	return c.checkResponse(result)
}
