package core

import (
	"context"
	"encoding/json"
	"fmt"
	elastic7 "github.com/elastic/go-elasticsearch/v7"
	"github.com/go-resty/resty/v2"
	"github.com/loveuer/esgo2dump/internal/opt"
	"github.com/loveuer/esgo2dump/internal/tool"
	"github.com/loveuer/esgo2dump/internal/xfile"
	"github.com/loveuer/esgo2dump/pkg/log"
	"github.com/loveuer/esgo2dump/pkg/model"
	"github.com/loveuer/esgo2dump/xes/es7"
	"net/url"
	"strings"
)

func NewIO(ctx context.Context, uri string, ioType model.IOType) (model.IO[map[string]any], error) {
	type Version struct {
		Name    string
		Version struct {
			Number string `json:"number"`
		} `json:"version"`
	}

	var (
		err    error
		target *url.URL
		rr     *resty.Response
		v      Version
	)

	if target, err = url.Parse(uri); err != nil {
		log.Debug("parse uri failed, type = %s, uri = %s, err = %s", ioType, uri, err.Error())
		return xfile.NewClient(uri, ioType)
	}

	if err = tool.ValidScheme(target.Scheme); err != nil {
		log.Debug("uri scheme check failed, type = %s, uri = %s", ioType, uri)
		return xfile.NewClient(uri, ioType)
	}

	// elastic uri
	index := strings.TrimPrefix(target.Path, "/")
	if index == "" {
		return nil, fmt.Errorf("uri invalid without index(path)")
	}

	log.Debug("%s uri es index = %s", ioType, index)

	versionURL := fmt.Sprintf("%s://%s", target.Scheme, strings.Split(target.Host, ",")[0])
	log.Debug("%s version url = %s", ioType, versionURL)
	if rr, err = opt.HttpClient.R().Get(versionURL); err != nil {
		log.Debug("get uri es version failed, type = %s, uri = %s, version_url = %s, err = %s", ioType, uri, versionURL, err.Error())
	}

	if err = json.Unmarshal(rr.Body(), &v); err != nil {
		log.Debug("decode uri es version failed, type = %s, uri = %s, version_url = %s, err = %s", ioType, uri, versionURL, err.Error())
		return nil, err
	}

	log.Debug("%s uri es version = %s", ioType, v.Version.Number)

	mainVersion := strings.Split(v.Version.Number, ".")[0]
	switch mainVersion {
	case "8":
	case "7":
		var client *elastic7.Client
		if client, err = es7.NewClient(ctx, uri); err != nil {
			return nil, err
		}

		return es7.NewStreamer(ctx, client, index)
	case "6":
	default:
		return nil, fmt.Errorf("es version not supported yet: %s", mainVersion)
	}

	return nil, nil
}
