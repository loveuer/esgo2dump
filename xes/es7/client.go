package es7

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	elastic "github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	"github.com/loveuer/esgo2dump/internal/tool"
	"github.com/samber/lo"
)

// Deprecated. use uri query: http://<username>:<password>@example.com:port?ping=false&...
type Config struct {
	DisablePing bool
}

type UriConfig struct {
	Ping  bool `json:"ping"`
	Sniff bool `json:"sniff"`
}

// NewClient
// new esv7 client
// uri example:
//   - http://127.0.0.1:9200
//   - https://<username>:<password>@node1.dev:9200,node2.dev:19200,node3.dev:29200
func NewClient(ctx context.Context, uri string, configs ...Config) (*elastic.Client, error) {
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

	cfg := Config{}
	if len(configs) > 0 {
		cfg = configs[0]
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

	cfg2 := &UriConfig{}
	cfg2.Ping, _ = strconv.ParseBool(query.Get("ping"))
	cfg2.Sniff, _ = strconv.ParseBool(query.Get("sniff"))

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
			DiscoverNodesOnStart: cfg2.Sniff,
		},
	); err != nil {
		return nil, err
	}

	// Deprecated.
	cfg.DisablePing = cfg.DisablePing || cfg2.Ping
	if cfg.DisablePing {
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
