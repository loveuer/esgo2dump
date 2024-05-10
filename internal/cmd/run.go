package cmd

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/loveuer/esgo2dump/internal/log"
	"net/url"
	"os"
	"strings"

	"github.com/loveuer/esgo2dump/internal/interfaces"
	"github.com/loveuer/esgo2dump/internal/opt"
	"github.com/loveuer/esgo2dump/internal/xes"
	"github.com/loveuer/esgo2dump/internal/xfile"
	"github.com/samber/lo"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

func check(cmd *cobra.Command) error {
	if f_input == "" {
		return cmd.Help()
		//return fmt.Errorf("must specify input(example: data.json/http://127.0.0.1:9200/my_index)")
	}

	if f_limit == 0 || f_limit > 10000 {
		return fmt.Errorf("invalid limit(1 - 10000)")
	}

	if f_query != "" && f_query_file != "" {
		return fmt.Errorf("cannot specify both query and query_file at the same time")
	}

	switch f_type {
	case "data", "mapping", "setting":
	default:
		return fmt.Errorf("unknown type=%s", f_type)
	}

	return nil
}

func run(cmd *cobra.Command, args []string) error {
	var (
		err error
		ioi interfaces.DumpIO
		ioo interfaces.DumpIO
	)

	if opt.Debug {
		logrus.SetLevel(logrus.DebugLevel)
		logrus.SetReportCaller(true)
		logrus.SetFormatter(&logrus.JSONFormatter{})
	}

	if f_version {
		fmt.Printf("esgo2dump (Version: %s)\n", opt.Version)
		os.Exit(0)
	}

	if err = check(cmd); err != nil {
		return err
	}

	if ioi, err = newIO(f_input, interfaces.IOInput, es_iversion); err != nil {
		return err
	}

	if ioo, err = newIO(f_output, interfaces.IOOutput, es_oversion); err != nil {
		return err
	}

	defer func() {
		_ = ioi.Close()
		_ = ioo.Close()
	}()

	if (f_query_file != "" || f_query != "") && ioi.IsFile() {
		return fmt.Errorf("with file input, query or query_file can't be supported")
	}

	if (f_source != "") && ioi.IsFile() {
		return fmt.Errorf("with file input, source can't be supported")
	}

	switch f_type {
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
		return fmt.Errorf("unknown type=%s", f_type)
	}
}

func executeData(ctx context.Context, input, output interfaces.DumpIO) error {
	var (
		err     error
		ch      = make(chan []*interfaces.ESSource, 1)
		errCh   = make(chan error)
		queries = make([]map[string]any, 0)
		sources = make([]string, 0)
	)

	if f_source != "" {
		sources = lo.Map(strings.Split(f_source, ";"), func(item string, idx int) string {
			return strings.TrimSpace(item)
		})
	}

	if f_query != "" {
		query := make(map[string]any)
		if err = json.Unmarshal([]byte(f_query), &query); err != nil {
			return fmt.Errorf("invalid query err=%v", err)
		}

		queries = append(queries, query)
	}

	if f_query_file != "" {
		var (
			qf *os.File
		)

		if qf, err = os.Open(f_query_file); err != nil {
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

	go func(c context.Context) {
		var (
			lines []*interfaces.ESSource
		)

		defer func() {
			close(ch)
		}()

	Loop:
		for _, query := range queries {
			for {
				select {
				case <-c.Done():
					return
				default:
					if lines, err = input.ReadData(c, f_limit, query, sources); err != nil {
						errCh <- err
						return
					}

					logrus.
						WithField("action", "input read data got lines").
						WithField("lines", len(lines)).
						Debug()

					if len(lines) == 0 {
						input.ResetOffset()
						if query != nil {
							bs, _ := json.Marshal(query)
							log.Info("Dump: query_file query=%s read done!!!", string(bs))
						}
						continue Loop
					}

					ch <- lines
				}
			}
		}
	}(ctx)

	var (
		succeed int
		total   int
		docs    []*interfaces.ESSource
		ok      bool
	)

	for {
		select {
		case <-ctx.Done():
		case err = <-errCh:
			return err
		case docs, ok = <-ch:
			if !ok {
				return err
			}

			if len(docs) == 0 {
				return nil
			}

			if succeed, err = output.WriteData(ctx, docs); err != nil {
				return err
			}

			logrus.
				WithField("action", "output wrote data lines").
				WithField("lines", succeed).
				Debug()

			if succeed != len(docs) {
				return fmt.Errorf("output got lines=%d, only succeed=%d", len(docs), succeed)
			}

			total += succeed

			log.Info("Dump: succeed=%d total=%d docs succeed!!!", succeed, total)
		}
	}
}

func newIO(source string, ioType interfaces.IO, esv string) (interfaces.DumpIO, error) {
	var (
		err  error
		iurl *url.URL
		file *os.File
		qm   = make(map[string]any)
	)

	logrus.
		WithField("action", "new_io").
		WithField("type", ioType.Code()).
		WithField("source", source).
		WithField("es_version", esv).
		Debug()

	if iurl, err = url.Parse(source); err != nil {
		logrus.
			WithField("action", "new_io url parse error").
			WithField("type", ioType.Code()).
			WithField("source", source).
			WithField("err", err).
			Debug()
		goto ClientByFile
	}

	if !(iurl.Scheme == "http" || iurl.Scheme == "https") {
		logrus.
			WithField("action", "new_io url scheme error").
			WithField("type", ioType.Code()).
			WithField("source", source).
			WithField("scheme", iurl.Scheme).
			Debug()
		goto ClientByFile
	}

	if iurl.Host == "" {
		logrus.
			WithField("action", "new_io url host empty").
			WithField("type", ioType.Code()).
			WithField("source", source).
			Debug()
		goto ClientByFile
	}

	if ioType == interfaces.IOInput && f_query != "" {
		if err = json.Unmarshal([]byte(f_query), &qm); err != nil {
			logrus.
				WithField("action", "new_io query string invalid").
				WithField("type", ioType.Code()).
				WithField("source", source).
				WithField("query", f_query).
				Debug()
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

	if file, err = os.OpenFile(source, os.O_CREATE|os.O_RDWR, 0644); err != nil {
		return nil, err
	}

	return xfile.NewClient(file, ioType)
}
