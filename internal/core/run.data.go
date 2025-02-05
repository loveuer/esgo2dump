package core

import (
	"bufio"
	"encoding/json"
	"fmt"
	"github.com/loveuer/esgo2dump/internal/opt"
	"github.com/loveuer/esgo2dump/internal/tool"
	"github.com/loveuer/esgo2dump/pkg/log"
	"github.com/loveuer/esgo2dump/pkg/model"
	"github.com/samber/lo"
	"github.com/spf13/cobra"
	"os"
	"strings"
	"sync"
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

	wc.Add(1)

	go func() {
		var (
			wroteCount = 0
			items      []map[string]any
		)

		defer wc.Done()

		for query := range qc {
			for {
				limit := tool.CalculateLimit(opt.Cfg.Args.Limit, total, opt.Cfg.Args.Max)
				log.Debug("one-step dump: arg.limit = %d, total = %d, arg.max = %d, calculate.limit = %d", opt.Cfg.Args.Limit, total, opt.Cfg.Args.Max, limit)
				if limit == 0 {
					break
				}

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
					break
				}

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
			if err = json.Unmarshal(scanner.Bytes(), &qm); err != nil {
				return err
			}

			qc <- qm

			log.Debug("Dump: queries[%06d]", queryCount)
		}
	case opt.Cfg.Args.Query != "":
		var (
			qm = make(map[string]any)
		)

		if err = json.Unmarshal([]byte(opt.Cfg.Args.Query), &qm); err != nil {
			return err
		}

		qc <- qm
	default:
		qc <- nil
	}

	// close query chan to stop trans_io_goroutine
	close(qc)

	wc.Wait()

	log.Info("Dump: dump all data success, total = %d", total)

	return nil
}
