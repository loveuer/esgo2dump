package es7

import (
	"context"
	"crypto/tls"
	"fmt"
	elastic "github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	"github.com/loveuer/esgo2dump/internal/util"
	"github.com/samber/lo"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

func NewClient(ctx context.Context, url *url.URL) (*elastic.Client, error) {
	var (
		err         error
		urlUsername string
		urlPassword string
		client      *elastic.Client
		errCh       = make(chan error)
		cliCh       = make(chan *elastic.Client)
		endpoints   = lo.Map(
			strings.Split(url.Host, ","),
			func(item string, index int) string {
				return fmt.Sprintf("%s://%s", url.Scheme, item)
			},
		)
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

	go ncFunc(endpoints, urlUsername, urlPassword)
	timeout := util.TimeoutCtx(ctx, 10)

	select {
	case <-timeout.Done():
		return nil, fmt.Errorf("dial es=%v err=%v", endpoints, context.DeadlineExceeded)
	case client = <-cliCh:
		return client, nil
	case err = <-errCh:
		return nil, err
	}
}
