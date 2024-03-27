package xfile

import (
	"bufio"
	"context"
	"encoding/json"
	"github.com/loveuer/esgo2dump/internal/opt"
	"io"
	"os"

	"github.com/loveuer/esgo2dump/internal/interfaces"
)

type client struct {
	f       *os.File
	iot     interfaces.IO
	scanner *bufio.Scanner
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

func (c *client) WriteMapping(ctx context.Context, m map[string]any) error {
	bs, err := json.Marshal(m)
	if err != nil {
		return err
	}

	_, err = c.f.Write(bs)

	return err
}

func (c *client) WriteSetting(ctx context.Context, m map[string]any) error {
	bs, err := json.Marshal(m)
	if err != nil {
		return err
	}

	_, err = c.f.Write(bs)

	return err
}

func (c *client) IOType() interfaces.IO {
	return c.iot
}

func (c *client) IsFile() bool {
	return true
}

func (c *client) ResetOffset() {}

func (c *client) WriteData(ctx context.Context, docs []*interfaces.ESSource) (int, error) {
	var (
		err   error
		bs    []byte
		count = 0
	)

	for _, doc := range docs {
		if bs, err = json.Marshal(doc); err != nil {
			return count, err
		}

		bs = append(bs, '\n')

		if _, err = c.f.Write(bs); err != nil {
			return count, err
		}

		count++
	}

	return count, nil
}

func (c *client) ReadData(ctx context.Context, i int, _ map[string]any) ([]*interfaces.ESSource, error) {
	var (
		err   error
		count = 0
		list  = make([]*interfaces.ESSource, 0, i)
	)

	for c.scanner.Scan() {
		line := c.scanner.Text()

		item := new(interfaces.ESSource)
		if err = json.Unmarshal([]byte(line), item); err != nil {
			return list, err
		}

		list = append(list, item)

		count++
		if count >= i {
			break
		}
	}

	if err = c.scanner.Err(); err != nil {
		return list, err
	}

	return list, nil
}

func (c *client) Close() error {
	return c.f.Close()
}

func NewClient(file *os.File, ioType interfaces.IO) (interfaces.DumpIO, error) {
	c := &client{f: file, iot: ioType}

	if ioType == interfaces.IOInput {
		c.scanner = bufio.NewScanner(c.f)
		buf := make([]byte, opt.BuffSize)
		c.scanner.Buffer(buf, opt.MaxBuffSize)
	}

	return c, nil
}
