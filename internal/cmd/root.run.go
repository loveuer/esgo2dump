package cmd

import (
	"fmt"
	"github.com/loveuer/esgo2dump/internal/core"
	"github.com/loveuer/esgo2dump/internal/opt"
	"github.com/loveuer/esgo2dump/internal/tool"
	"github.com/loveuer/esgo2dump/pkg/log"
	"github.com/loveuer/esgo2dump/pkg/model"
	"github.com/spf13/cobra"
	"os"
)

func preRun(cmd *cobra.Command, args []string) error {
	if opt.Cfg.Debug {
		log.SetLogLevel(log.LogLevelDebug)
	}

	if opt.Cfg.Args.Version {
		fmt.Printf("esgo2dump version: %s\n", opt.Version)
		os.Exit(0)
	}

	if opt.Cfg.Debug {
		tool.TablePrinter(opt.Cfg)
	}

	// check args
	if opt.Cfg.Args.Input == "" {
		return cmd.Help()
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

	// validate split-limit
	if opt.Cfg.Args.SplitLimit > 0 {
		if opt.Cfg.Args.Type != "data" {
			return fmt.Errorf("split-limit only supports type=data")
		}
		// check if output is a directory
		info, err := os.Stat(opt.Cfg.Args.Output)
		if err != nil {
			if os.IsNotExist(err) {
				// directory doesn't exist, try to create it
				if err = os.MkdirAll(opt.Cfg.Args.Output, 0755); err != nil {
					return fmt.Errorf("failed to create output directory: %w", err)
				}
			} else {
				return fmt.Errorf("failed to check output path: %w", err)
			}
		} else {
			if !info.IsDir() {
				return fmt.Errorf("when split-limit > 0, output must be a directory, but got: %s", opt.Cfg.Args.Output)
			}
		}
	}

	return nil
}

func run(cmd *cobra.Command, args []string) error {
	var (
		err    error
		input  model.IO[map[string]any]
		output model.IO[map[string]any]
	)

	if input, err = core.NewIO(cmd.Context(), opt.Cfg.Args.Input, model.Input); err != nil {
		return err
	}

	if output, err = core.NewIO(cmd.Context(), opt.Cfg.Args.Output, model.Output); err != nil {
		return err
	}

	switch opt.Cfg.Args.Type {
	case "data":
		return core.RunData(cmd, input, output)
	case "mapping":
		return core.RunMapping(cmd, input, output)
	case "setting":
		return core.RunSetting(cmd, input, output)
	}
	return fmt.Errorf("unknown args: type = %s", opt.Cfg.Args.Type)
}
