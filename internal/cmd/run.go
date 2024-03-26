package cmd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"

	"github.com/loveuer/esgo2dump/internal/interfaces"
	"github.com/loveuer/esgo2dump/internal/opt"
	"github.com/loveuer/esgo2dump/internal/xes"
	"github.com/loveuer/esgo2dump/internal/xfile"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

func run(cmd *cobra.Command, args []string) error {
	var (
		err error
		ioi interfaces.DumpIO
		ioo interfaces.DumpIO
	)

	if opt.Debug {
		logrus.SetLevel(logrus.DebugLevel)
	}

	switch f_type {
	case "data", "mapping", "setting":
	default:
		return fmt.Errorf("unknown type=%s", f_type)
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

	switch f_type {
	case "data":
		return executeData(cmd.Context(), ioi, ioo)
	case "mapping":
		var mapping map[string]any
		if mapping, err = ioi.ReadMapping(cmd.Context()); err != nil {
			return err
		}

		return ioo.WriteMapping(cmd.Context(), mapping)
	case "setting":
		var setting map[string]any
		if setting, err = ioi.ReadSetting(cmd.Context()); err != nil {
			return err
		}

		return ioo.WriteSetting(cmd.Context(), setting)
	default:
		return fmt.Errorf("unknown type=%s", f_type)
	}
}

func executeData(ctx context.Context, input, output interfaces.DumpIO) error {
	var (
		err     error
		lines   []*interfaces.ESSource
		succeed int
	)

	for {

		if lines, err = input.ReadData(ctx, f_limit); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}

			return err
		}

		if len(lines) == 0 {
			return nil
		}

		if succeed, err = output.WriteData(ctx, lines); err != nil {
			return err
		}

		if succeed != len(lines) {
			return fmt.Errorf("cmd.run: got lines=%d, only succeed=%d", len(lines), succeed)
		}

		logrus.Infof("Dump: %d docs succeed!!!", succeed)

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

	return xes.NewClient(iurl, ioType, qm)

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
