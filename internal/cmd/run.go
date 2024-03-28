package cmd

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"os"

	"github.com/loveuer/esgo2dump/internal/interfaces"
	"github.com/loveuer/esgo2dump/internal/opt"
	"github.com/loveuer/esgo2dump/internal/xes"
	"github.com/loveuer/esgo2dump/internal/xfile"
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
	}

	if err = check(cmd); err != nil {
		return err
	}

	if ioi, err = newIO(f_input, interfaces.IOInput); err != nil {
		return err
	}

	if ioo, err = newIO(f_output, interfaces.IOOutput); err != nil {
		return err
	}

	defer func() {
		_ = ioi.Close()
		_ = ioo.Close()
	}()

	if (f_query_file != "" || f_query != "") && ioi.IsFile() {
		return fmt.Errorf("with file input, query or query_file can't be supported")
	}

	switch f_type {
	case "data":
		if err = executeData(cmd.Context(), ioi, ioo); err != nil {
			return err
		}

		logrus.Info("Dump: write data succeed!!!")

		return nil
	case "mapping":
		var mapping map[string]any
		if mapping, err = ioi.ReadMapping(cmd.Context()); err != nil {
			return err
		}

		if err = ioo.WriteMapping(cmd.Context(), mapping); err != nil {
			return err
		}

		logrus.Info("Dump: write mapping succeed!!!")

		return nil
	case "setting":
		var setting map[string]any
		if setting, err = ioi.ReadSetting(cmd.Context()); err != nil {
			return err
		}

		if err = ioo.WriteSetting(cmd.Context(), setting); err != nil {
			return err
		}

		logrus.Info("Dump: write setting succeed!!!")

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
	)

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
					if lines, err = input.ReadData(c, f_limit, query); err != nil {
						errCh <- err
						return
					}

					logrus.Debugf("executeData: input read_data got lines=%d", len(lines))

					if len(lines) == 0 {
						input.ResetOffset()
						if query != nil {
							bs, _ := json.Marshal(query)
							logrus.Infof("Dump: query_file query=%s read done!!!", string(bs))
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

			logrus.Debugf("executeData: output write_data succeed lines=%d", succeed)

			if succeed != len(docs) {
				return fmt.Errorf("cmd.run: got lines=%d, only succeed=%d", len(docs), succeed)
			}

			total += succeed

			logrus.Infof("Dump: succeed=%d total=%d docs succeed!!!", succeed, total)
		}
	}
}

func newIO(source string, ioType interfaces.IO) (interfaces.DumpIO, error) {
	var (
		err  error
		iurl *url.URL
		file *os.File
		qm   = make(map[string]any)
	)

	logrus.Debugf("newIO.%s: source string=%s", ioType.Code(), source)

	if iurl, err = url.Parse(source); err != nil {
		logrus.Debugf("newIO.%s: url parse source err=%v", ioType.Code(), err)
		goto ClientByFile
	}

	if !(iurl.Scheme == "http" || iurl.Scheme == "https") {
		logrus.Debugf("newIO.%s: url scheme=%s invalid", ioType.Code(), iurl.Scheme)
		goto ClientByFile
	}

	if iurl.Host == "" {
		logrus.Debugf("newIO.%s: url host empty", ioType.Code())
		goto ClientByFile
	}

	if ioType == interfaces.IOInput && f_query != "" {
		if err = json.Unmarshal([]byte(f_query), &qm); err != nil {
			logrus.Debugf("newIO.%s: query=%s invalid to map[string]any", ioType.Code(), f_query)
			return nil, fmt.Errorf("invalid query err=%v", err)
		}
	}

	logrus.Debugf("newIO.%s: source as url=%+v", ioType.Code(), *iurl)

	return xes.NewClient(iurl, ioType)

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
