package opt

import (
	"crypto/tls"

	"github.com/go-resty/resty/v2"
)

const (
	ScrollDurationSeconds = 10 * 60
	DefaultSize           = 100
)

var (
	Version = "vx.x.x"
	Timeout int

	BuffSize    = 5 * 1024 * 1024   // 5M
	MaxBuffSize = 100 * 1024 * 1024 // 100M, default elastic_search doc max size

	HttpClient = resty.New().SetTLSClientConfig(&tls.Config{InsecureSkipVerify: true})
)
