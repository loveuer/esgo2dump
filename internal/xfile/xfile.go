package xfile

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"os"

	"github.com/loveuer/esgo2dump/internal/opt"
	"github.com/loveuer/esgo2dump/model"
	"github.com/loveuer/nf/nft/log"

	"github.com/loveuer/esgo2dump/internal/interfaces"
)

type client struct {
	f       *os.File
	iot     interfaces.IO
	scanner *bufio.Scanner
}

func (c *client) WriteData(ctx context.Context, docsCh <-chan []*model.ESSource) error {
	total := 0
	for line := range docsCh {
		for _, doc := range line {
			bs, err := json.Marshal(doc)
			if err != nil {
				return err
			}

			if _, err = c.f.Write(append(bs, '\n')); err != nil {
				return err
			}
		}

		count := len(line)
		total += count

		log.Info("Dump: succeed=%d total=%d docs succeed!!!", count, total)
	}

	return nil
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

func (c *client) ReadData(ctx context.Context, size int, _ map[string]any, _ []string, _ []string) (<-chan []*model.ESSource, <-chan error) {
	var (
		err   error
		count int = 0
		list      = make([]*model.ESSource, 0, size)
		dch       = make(chan []*model.ESSource)
		ech       = make(chan error)
		ready     = make(chan bool)
		total     = 0
	)

	go func(ctx context.Context) {
		defer func() {
			close(dch)
			close(ech)
		}()

		ready <- true

		for c.scanner.Scan() {
			select {
			case <-ctx.Done():
				return
			default:
				item := new(model.ESSource)
				line := c.scanner.Bytes()

				if err = json.Unmarshal(line, item); err != nil {
					ech <- err
					return
				}

				list = append(list, item)
				count++
				total++

				if count >= size {
					dch <- list
					list = list[:0]
					count = 0
				}
			}
		}

		if len(list) > 0 {
			dch <- list
			list = list[:0]
			count = 0
		}

		if err = c.scanner.Err(); err != nil {
			ech <- err
		}

		log.Debug("read: read file succeed! total=%d", total)
	}(ctx)

	<-ready

	return dch, ech
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
