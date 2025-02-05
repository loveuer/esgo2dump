package es7

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	elastic "github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	"github.com/loveuer/esgo2dump/internal/tool"
	"github.com/samber/lo"
)

// NewClient
// new esv7 client
// uri example:
//   - http://127.0.0.1:9200
//   - https://<username>:<password>@node1.dev:9200,node2.dev:19200,node3.dev:29200
func NewClient(ctx context.Context, uri string) (*elastic.Client, error) {
	var (
		err      error
		username string
		password string
		client   *elastic.Client
		ins      *url.URL
	)

	if ins, err = url.Parse(uri); err != nil {
		return nil, err
	}

	endpoints := lo.Map(
		strings.Split(ins.Host, ","),
		func(item string, index int) string {
			return fmt.Sprintf("%s://%s", ins.Scheme, item)
		},
	)

	if ins.User != nil {
		username = ins.User.Username()
		password, _ = ins.User.Password()
	}

	query := ins.Query()

	if client, err = elastic.NewClient(
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
			DiscoverNodesOnStart: lo.If(query.Get("sniff") == "true", true).Else(false),
		},
	); err != nil {
		return nil, err
	}

	if query.Get("ping") != "false" {
		var res *esapi.Response
		if res, err = client.Ping(client.Ping.WithContext(tool.TimeoutCtx(ctx, 5))); err != nil {
			return nil, err
		}

		if res.StatusCode != 200 {
			err = fmt.Errorf("ping es server response: %s", res.String())
			return nil, err
		}
	}

	return client, nil
}
