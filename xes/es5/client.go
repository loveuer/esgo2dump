package es5

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/go-resty/resty/v2"
	"github.com/loveuer/esgo2dump/internal/tool"
)

// Client is a minimal Elasticsearch 5.x http client.
// ES 5.x has no official go client, so we talk REST directly.
type Client struct {
	base string
	c    *resty.Client
}

// NewClient creates an es5 client.
// uri example:
//   - http://127.0.0.1:9200
//   - https://<username>:<password>@node1.dev:9200,node2.dev:19200/my_index?ping=false
func NewClient(ctx context.Context, uri string) (*Client, error) {
	var (
		err error
		ins *url.URL
	)

	if ins, err = url.Parse(uri); err != nil {
		return nil, err
	}

	if err = tool.ValidScheme(ins.Scheme); err != nil {
		return nil, err
	}

	hosts := strings.Split(ins.Host, ",")
	if len(hosts) == 0 || hosts[0] == "" {
		return nil, fmt.Errorf("uri without host: %s", uri)
	}

	// ES 5.x client uses the first node as entry point.
	base := fmt.Sprintf("%s://%s", ins.Scheme, hosts[0])

	rc := resty.New().
		SetBaseURL(base).
		SetTLSClientConfig(&tls.Config{InsecureSkipVerify: true}).
		SetTimeout(30 * time.Second).
		SetRetryCount(3).
		SetRetryWaitTime(500 * time.Millisecond).
		SetRetryMaxWaitTime(3 * time.Second)

	if ins.User != nil {
		username := ins.User.Username()
		password, _ := ins.User.Password()
		rc.SetBasicAuth(username, password)
	}

	c := &Client{base: base, c: rc}

	if ins.Query().Get("ping") != "false" {
		resp, err := rc.R().
			SetContext(tool.TimeoutCtx(ctx, 5)).
			Get("/")
		if err != nil {
			return nil, err
		}

		if resp.StatusCode() != 200 {
			return nil, fmt.Errorf("ping es5 server response: %s", resp.String())
		}
	}

	return c, nil
}
