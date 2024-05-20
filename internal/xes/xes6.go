package xes

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"github.com/loveuer/esgo2dump/model"
	"github.com/loveuer/esgo2dump/xes/es6"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	elastic "github.com/elastic/go-elasticsearch/v6"
	"github.com/elastic/go-elasticsearch/v6/esapi"
	"github.com/elastic/go-elasticsearch/v6/esutil"
	"github.com/loveuer/esgo2dump/internal/interfaces"
	"github.com/loveuer/esgo2dump/internal/opt"
	"github.com/loveuer/esgo2dump/internal/util"
	"github.com/sirupsen/logrus"
)

func NewClientV6(url *url.URL, iot interfaces.IO) (interfaces.DumpIO, error) {

	var (
		address     = fmt.Sprintf("%s://%s", url.Scheme, url.Host)
		urlIndex    = strings.TrimPrefix(url.Path, "/")
		urlUsername string
		urlPassword string
		errCh       = make(chan error)
		cliCh       = make(chan *elastic.Client)
	)

	if url.User != nil {
		urlUsername = url.User.Username()
		if p, ok := url.User.Password(); ok {
			urlPassword = p
		}
	}

	logrus.
		WithField("action", "new es client v6").
		WithField("endpoint", address).
		WithField("index", urlIndex).
		WithField("username", urlUsername).
		WithField("password", urlPassword).
		Debug()

	if urlIndex == "" {
		return nil, fmt.Errorf("please specify index name: (like => http://127.0.0.1:9200/my_index)")
	}

	ncFunc := func(endpoints []string, username, password, index string) {
		var (
			err      error
			cli      *elastic.Client
			infoResp *esapi.Response
		)

		if cli, err = elastic.NewClient(
			elastic.Config{
				Addresses:     endpoints,
				Username:      username,
				Password:      password,
				CACert:        nil,
				RetryOnStatus: []int{429},
				MaxRetries:    3,
				RetryBackoff:  nil,
				Transport: &http.Transport{
					TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
					DialContext:     (&net.Dialer{Timeout: 10 * time.Second}).DialContext,
				},
			},
		); err != nil {
			logrus.
				WithField("action", "new es client v6 error").
				WithField("endpoints", endpoints).
				WithField("err", err).
				Debug()
			errCh <- err
			return
		}

		if infoResp, err = cli.Info(); err != nil {
			logrus.
				WithField("action", "es client v6 ping error").
				WithField("err", err).
				Debug()
			errCh <- err
			return
		}

		if infoResp.StatusCode != 200 {
			err = fmt.Errorf("info xes status=%d", infoResp.StatusCode)
			logrus.
				WithField("action", "es client v6 ping status error").
				WithField("status", infoResp.StatusCode).
				Debug()
			errCh <- err
			return
		}

		cliCh <- cli
	}

	go ncFunc([]string{address}, urlUsername, urlPassword, urlIndex)

	select {
	case <-util.Timeout(10).Done():
		return nil, fmt.Errorf("dial es=%s err=%v", address, context.DeadlineExceeded)
	case c := <-cliCh:
		return &clientv6{client: c, index: urlIndex, iot: iot}, nil
	case e := <-errCh:
		return nil, e
	}
}

type clientv6 struct {
	client *elastic.Client
	iot    interfaces.IO
	index  string
}

func (c *clientv6) checkResponse(r *esapi.Response) error {
	if r.StatusCode == 200 {
		return nil
	}

	return fmt.Errorf("status=%d msg=%s", r.StatusCode, r.String())
}

func (c *clientv6) IOType() interfaces.IO {
	return c.iot
}

func (c *clientv6) IsFile() bool {
	return false
}

func (c *clientv6) Close() error {
	return nil
}

func (c *clientv6) WriteData(ctx context.Context, docs []*model.ESSource) (int, error) {
	var (
		err     error
		indexer esutil.BulkIndexer
		count   int
		be      error
	)
	if indexer, err = esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
		Client:       c.client,
		Index:        c.index,
		DocumentType: "_doc",
		ErrorTrace:   true,
	}); err != nil {
		return 0, err
	}

	for _, doc := range docs {
		var bs []byte

		if bs, err = json.Marshal(doc.Content); err != nil {
			return 0, err
		}

		logrus.WithField("raw", string(bs)).Debug()

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

func (c *clientv6) ReadData(ctx context.Context, size int, query map[string]any, source []string) (<-chan []*model.ESSource, <-chan error) {
	dch, ech := es6.ReadData(ctx, c.client, c.index, size, 0, query, source)

	return dch, ech
}

func (c *clientv6) ReadMapping(ctx context.Context) (map[string]any, error) {
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
func (c *clientv6) WriteMapping(ctx context.Context, m map[string]any) error {
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

func (c *clientv6) ReadSetting(ctx context.Context) (map[string]any, error) {
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

func (c *clientv6) WriteSetting(ctx context.Context, m map[string]any) error {
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
