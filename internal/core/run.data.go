package core

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/loveuer/esgo2dump/internal/opt"
	"github.com/loveuer/esgo2dump/internal/tool"
	"github.com/loveuer/esgo2dump/pkg/log"
	"github.com/loveuer/esgo2dump/pkg/model"
	"github.com/samber/lo"
	"github.com/spf13/cobra"
)

func RunData(cmd *cobra.Command, input, output model.IO[map[string]any]) error {
	var (
		err error
		// query chan
		qc = make(chan map[string]any)
		// error chan
		ec = make(chan error)
		// done chan
		wc    = &sync.WaitGroup{}
		total = 0
	)

	wc.Add(2)

	go func() {
		var ok bool

		defer wc.Done()

		err, ok = <-ec
		if !ok {
			return
		}

		log.Error(err.Error())
		os.Exit(1)

	}()

	go func() {
		var (
			wroteCount = 0
			items      []map[string]any
		)

		defer wc.Done()

		for query := range qc {
			for {
				limit := tool.CalculateLimit(opt.Cfg.Args.Limit, total, opt.Cfg.Args.Max)
				log.Debug("one-step dump begin: arg.limit = %d, total = %d, arg.max = %d, calculate.limit = %d", opt.Cfg.Args.Limit, total, opt.Cfg.Args.Max, limit)
				if limit == 0 {
					break
				}

				log.Debug("one-step dump start read: arg.limit = %d, total = %d, arg.max = %d, calculate.limit = %d", opt.Cfg.Args.Limit, total, opt.Cfg.Args.Max, limit)
				if items, err = input.ReadData(
					cmd.Context(),
					limit,
					query,
					lo.Filter(strings.Split(opt.Cfg.Args.Field, ","), func(x string, _ int) bool { return x != "" }),
					lo.Filter(strings.Split(opt.Cfg.Args.Sort, ","), func(x string, _ int) bool { return x != "" }),
				); err != nil {
					ec <- err
					return
				}

				if len(items) == 0 {
					input.Cleanup()
					break
				}

				log.Debug("one-step dump start write: arg.limit = %d, total = %d, arg.max = %d, calculate.limit = %d, got = %d", opt.Cfg.Args.Limit, total, opt.Cfg.Args.Max, limit, len(items))
				if wroteCount, err = output.WriteData(cmd.Context(), items); err != nil {
					ec <- err
					return
				}

				total += wroteCount

				if wroteCount != len(items) {
					ec <- fmt.Errorf("got items %d, but wrote %d", len(items), wroteCount)
					return
				}

				log.Info("Dump: dump data success = %d total = %d", wroteCount, total)
			}
		}
	}()

	switch {
	case opt.Cfg.Args.QueryFile != "":
		var (
			// query file
			qf         *os.File
			queryCount = 0
		)
		if qf, err = os.Open(opt.Cfg.Args.QueryFile); err != nil {
			return err
		}

		scanner := bufio.NewScanner(qf)
		for scanner.Scan() {
			queryCount++
			qm := make(map[string]any)
			bs := scanner.Bytes()
			if err = json.Unmarshal(bs, &qm); err != nil {
				return err
			}

			qc <- qm

			log.Debug("Dump: queries[%06d] = %s", queryCount, string(bs))
		}
	case opt.Cfg.Args.Query != "":
		var (
			qm = make(map[string]any)
		)

		if err = json.Unmarshal([]byte(opt.Cfg.Args.Query), &qm); err != nil {
			log.Debug("unmarshal arg.query string err, query = %s ,err = %s", opt.Cfg.Args.Query, err.Error())
			return err
		}

		qc <- qm
	default:
		qc <- nil
	}

	// close query chan to stop trans_io_goroutine
	close(qc)
	close(ec)

	wc.Wait()

	log.Info("Dump: dump all data success, total = %d", total)

	return nil
}
