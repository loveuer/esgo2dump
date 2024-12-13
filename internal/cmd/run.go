package cmd

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync"

	"github.com/loveuer/esgo2dump/model"
	"github.com/loveuer/nf/nft/log"

	"github.com/loveuer/esgo2dump/internal/interfaces"
	"github.com/loveuer/esgo2dump/internal/opt"
	"github.com/loveuer/esgo2dump/internal/xes"
	"github.com/loveuer/esgo2dump/internal/xfile"
	"github.com/samber/lo"
	"github.com/spf13/cobra"
)

func check(cmd *cobra.Command) error {
	if opt.Cfg.Args.Input == "" {
		return cmd.Help()
		// return fmt.Errorf("must specify input(example: data.json/http://127.0.0.1:9200/my_index)")
	}

	if opt.Cfg.Args.Limit == 0 || opt.Cfg.Args.Limit > 10000 {
		return fmt.Errorf("invalid limit(1 - 10000)")
	}

	if opt.Cfg.Args.Query != "" && opt.Cfg.Args.QueryFile != "" {
		return fmt.Errorf("cannot specify both query and query_file at the same time")
	}

	switch opt.Cfg.Args.Type {
	case "data", "mapping", "setting":
	default:
		return fmt.Errorf("unknown type=%s", opt.Cfg.Args.Type)
	}

	return nil
}

func run(cmd *cobra.Command, args []string) error {
	var (
		err error
		ioi interfaces.DumpIO
		ioo interfaces.DumpIO
	)

	if err = check(cmd); err != nil {
		return err
	}

	if ioi, err = newIO(opt.Cfg.Args.Input, interfaces.IOInput, es_iversion); err != nil {
		return err
	}

	if ioo, err = newIO(opt.Cfg.Args.Output, interfaces.IOOutput, es_oversion); err != nil {
		return err
	}

	defer func() {
		_ = ioi.Close()
		_ = ioo.Close()
	}()

	if (opt.Cfg.Args.Query != "" || opt.Cfg.Args.QueryFile != "") && ioi.IsFile() {
		return fmt.Errorf("with file input, query or query_file can't be supported")
	}

	if (opt.Cfg.Args.Source != "") && ioi.IsFile() {
		return fmt.Errorf("with file input, source can't be supported")
	}

	switch opt.Cfg.Args.Type {
	case "data":
		if err = executeData(cmd.Context(), ioi, ioo); err != nil {
			return err
		}

		log.Info("Dump: write data succeed!!!")

		return nil
	case "mapping":
		var mapping map[string]any
		if mapping, err = ioi.ReadMapping(cmd.Context()); err != nil {
			return err
		}

		if err = ioo.WriteMapping(cmd.Context(), mapping); err != nil {
			return err
		}

		log.Info("Dump: write mapping succeed!!!")

		return nil
	case "setting":
		var setting map[string]any
		if setting, err = ioi.ReadSetting(cmd.Context()); err != nil {
			return err
		}

		if err = ioo.WriteSetting(cmd.Context(), setting); err != nil {
			return err
		}

		log.Info("Dump: write setting succeed!!!")

		return nil
	default:
		return fmt.Errorf("unknown type=%s", opt.Cfg.Args.Type)
	}
}

func executeData(ctx context.Context, input, output interfaces.DumpIO) error {
	var (
		err     error
		queries = make([]map[string]any, 0)
		sources = make([]string, 0)
	)

	if opt.Cfg.Args.Source != "" {
		sources = lo.Map(strings.Split(opt.Cfg.Args.Source, ";"), func(item string, idx int) string {
			return strings.TrimSpace(item)
		})
	}

	if opt.Cfg.Args.Query != "" {
		query := make(map[string]any)
		if err = json.Unmarshal([]byte(opt.Cfg.Args.Query), &query); err != nil {
			return fmt.Errorf("invalid query err=%v", err)
		}

		queries = append(queries, query)
	}

	if opt.Cfg.Args.QueryFile != "" {
		var qf *os.File

		if qf, err = os.Open(opt.Cfg.Args.QueryFile); err != nil {
			return fmt.Errorf("open query_file err=%v", err)
		}

		defer func() {
			_ = qf.Close()
		}()

		scanner := bufio.NewScanner(qf)
		scanner.Buffer(make([]byte, 1*1024*1024), 5*1024*1024)
		lineCount := 1
		for scanner.Scan() {
			line := scanner.Text()
			oq := make(map[string]any)
			if err = json.Unmarshal([]byte(line), &oq); err != nil {
				return fmt.Errorf("query file line=%d invalid err=%v", lineCount, err)
			}

			queries = append(queries, oq)

			if len(queries) > 10000 {
				return fmt.Errorf("query_file support max lines=%d", 10000)
			}

			lineCount++
		}

	}

	if len(queries) == 0 {
		queries = append(queries, nil)
	}

	var (
		ok   bool
		docs []*model.ESSource
		dch  <-chan []*model.ESSource
		ech  <-chan error

		e2ch = make(chan error)
		wch  = make(chan []*model.ESSource)
		wg   = sync.WaitGroup{}
	)

	go func() {
		wg.Add(1)
		if err = output.WriteData(ctx, wch); err != nil {
			e2ch <- err
		}

		wg.Done()
	}()

	log.Info("Query: got queries=%d", len(queries))

Loop:
	for queryIdx, query := range queries {
		bs, _ := json.Marshal(query)

		log.Debug("Query[%d]: %s", queryIdx, string(bs))

		dch, ech = input.ReadData(ctx, opt.Cfg.Args.Limit, query, sources, []string{opt.Cfg.Args.Sort})

		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case err, ok = <-ech:
				if err != nil {
					return err
				}

				continue Loop
			case err, _ = <-e2ch:
				return err
			case docs, ok = <-dch:
				if !ok || len(docs) == 0 {
					continue Loop
				}

				wch <- docs
			}
		}
	}

	close(wch)

	wg.Wait()

	return nil
}

func newIO(source string, ioType interfaces.IO, esv string) (interfaces.DumpIO, error) {
	var (
		err  error
		iurl *url.URL
		file *os.File
		qm   = make(map[string]any)
	)

	log.Debug("action=%s, type=%s, source=%s, es_version=%s", "new_io", ioType.Code(), source, esv)

	if iurl, err = url.Parse(source); err != nil {
		log.Debug("action=%s, type=%s, source=%s, err=%s", "new_io url parse err", ioType.Code(), source, err.Error())
		goto ClientByFile
	}

	if !(iurl.Scheme == "http" || iurl.Scheme == "https") {
		log.Debug("action=%s, type=%s, source=%s, scheme=%s", "new_io url scheme error", ioType.Code(), source, iurl.Scheme)
		goto ClientByFile
	}

	if iurl.Host == "" {
		log.Debug("action=%s, type=%s, source=%s", "new_io url host empty", ioType.Code(), source)
		goto ClientByFile
	}

	if ioType == interfaces.IOInput && opt.Cfg.Args.Query != "" {
		if err = json.Unmarshal([]byte(opt.Cfg.Args.Query), &qm); err != nil {
			log.Debug("action=%s, type=%s, source=%s, query=%s", "new_io query string invalid", ioType.Code(), source, opt.Cfg.Args.Query)
			return nil, fmt.Errorf("invalid query err=%v", err)
		}
	}

	switch esv {
	case "7":
		return xes.NewClient(iurl, ioType)
	case "6":
		return xes.NewClientV6(iurl, ioType)
	case "8":
		return nil, errors.New("es version 8 coming soon")
	default:
		return nil, fmt.Errorf("unknown es version=%s", esv)
	}

ClientByFile:
	if ioType == interfaces.IOOutput {
		if _, err = os.Stat(source); !os.IsNotExist(err) {
			return nil, fmt.Errorf("output_file=%s already exist", source)
		}
	}

	if file, err = os.OpenFile(source, os.O_CREATE|os.O_RDWR, 0o644); err != nil {
		return nil, err
	}

	return xfile.NewClient(file, ioType)
}
