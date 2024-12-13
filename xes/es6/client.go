package es6

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"time"

	elastic "github.com/elastic/go-elasticsearch/v6"
	"github.com/elastic/go-elasticsearch/v6/esapi"
	"github.com/loveuer/esgo2dump/internal/tool"
)

func NewClient(ctx context.Context, url *url.URL) (*elastic.Client, error) {
	var (
		err         error
		urlUsername string
		urlPassword string
		client      *elastic.Client
		errCh       = make(chan error)
		cliCh       = make(chan *elastic.Client)
		address     = fmt.Sprintf("%s://%s", url.Scheme, url.Host)
	)

	if url.User != nil {
		urlUsername = url.User.Username()
		if p, ok := url.User.Password(); ok {
			urlPassword = p
		}
	}

	ncFunc := func(endpoints []string, username, password string) {
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
			errCh <- err
			return
		}

		if infoResp, err = cli.Info(); err != nil {
			errCh <- err
			return
		}

		if infoResp.StatusCode != 200 {
			err = fmt.Errorf("info es7 status=%d", infoResp.StatusCode)
			errCh <- err
			return
		}

		cliCh <- cli
	}

	go ncFunc([]string{address}, urlUsername, urlPassword)
	timeout := tool.TimeoutCtx(ctx, 10)

	select {
	case <-timeout.Done():
		return nil, fmt.Errorf("dial es=%s err=%v", address, context.DeadlineExceeded)
	case client = <-cliCh:
		return client, nil
	case err = <-errCh:
		return nil, err
	}
}
