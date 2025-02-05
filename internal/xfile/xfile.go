package xfile

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/loveuer/esgo2dump/internal/opt"
	"github.com/loveuer/esgo2dump/pkg/log"
	"github.com/loveuer/esgo2dump/pkg/model"
)

type client struct {
	info    os.FileInfo
	f       *os.File
	scanner *bufio.Scanner
}

func (c *client) ReadData(ctx context.Context, limit int, query map[string]any, fields []string, sort []string) ([]map[string]any, error) {
	if len(query) != 0 {
		return nil, fmt.Errorf("file with query is unsupported")
	}

	if len(sort) != 0 {
		return nil, fmt.Errorf("file with sort is unsupported")
	}

	list := make([]map[string]any, 0, limit)

	for c.scanner.Scan() {
		line := c.scanner.Bytes()
		item := make(map[string]any)

		if err := json.Unmarshal(line, &item); err != nil {
			return nil, err
		}

		if len(fields) > 0 {
			// todo: pick fields
		}

		list = append(list, item)

		if len(list) >= limit {
			return list, nil
		}
	}

	return list, nil
}

func (c *client) WriteData(ctx context.Context, items []map[string]any) (int, error) {
	total := 0
	for _, item := range items {
		bs, err := json.Marshal(item)
		if err != nil {
			return total, err
		}

		if _, err = c.f.Write(bs); err != nil {
			return total, err
		}

		total++

		if _, err = c.f.WriteString("\n"); err != nil {
			return total, err
		}
	}

	return total, nil
}

func (c *client) ReadMapping(ctx context.Context) (map[string]any, error) {
	var (
		err error
		bs  []byte
	)

	if bs, err = io.ReadAll(c.f); err != nil {
		return nil, err
	}

	m := make(map[string]any)

	if err = json.Unmarshal(bs, &m); err != nil {
		return nil, err
	}

	return m, nil
}

func (c *client) WriteMapping(ctx context.Context, mapping map[string]any) error {
	bs, err := json.Marshal(mapping)
	if err != nil {
		return err
	}

	_, err = c.f.Write(bs)

	return err
}

func (c *client) ReadSetting(ctx context.Context) (map[string]any, error) {
	var (
		err error
		bs  []byte
	)

	if bs, err = io.ReadAll(c.f); err != nil {
		return nil, err
	}

	m := make(map[string]any)

	if err = json.Unmarshal(bs, &m); err != nil {
		return nil, err
	}

	return m, nil
}

func (c *client) WriteSetting(ctx context.Context, setting map[string]any) error {
	bs, err := json.Marshal(setting)
	if err != nil {
		return err
	}

	_, err = c.f.Write(bs)

	return err
}

func NewClient(path string, t model.IOType) (model.IO[map[string]any], error) {
	var (
		info os.FileInfo
		err  error
		f    *os.File
	)

	switch t {
	case model.Input:
		if info, err = os.Stat(path); err != nil {
			return nil, err
		}

		log.Debug("input file: %s, size: %d", path, info.Size())

		if f, err = os.Open(path); err != nil {
			return nil, err
		}
	case model.Output:
		if info, err = os.Stat(path); err == nil {
			return nil, fmt.Errorf("file already exists: %s", path)
		}

		if !os.IsNotExist(err) {
			return nil, err
		}

		if f, err = os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC|os.O_APPEND, 0o644); err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("unknown type: %s", t)
	}

	c := &client{f: f, info: info}
	buf := make([]byte, opt.BuffSize)
	scanner := bufio.NewScanner(c.f)
	scanner.Buffer(buf, opt.MaxBuffSize)
	c.scanner = scanner

	return c, nil
}
