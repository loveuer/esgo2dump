package cmd

import (
	"context"
	"fmt"
	"os"

	"github.com/loveuer/nf/nft/log"

	"github.com/loveuer/esgo2dump/internal/opt"
	"github.com/loveuer/esgo2dump/internal/tool"
	"github.com/spf13/cobra"
)

var (
	rootCommand = &cobra.Command{
		Use:           "esgo2dump",
		Short:         "esgo2dump is alternative to elasticdump",
		SilenceUsage:  true,
		SilenceErrors: true,
		RunE:          run,
		PersistentPreRun: func(cmd *cobra.Command, args []string) {
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
		},
		Example: `
esgo2dump --input=http://127.0.0.1:9200/some_index --output=./data.json

esgo2dump --input=http://127.0.0.1:9200/some_index --output=http://192.168.1.1:9200/some_index --limit=5000

esgo2dump --input=http://127.0.0.1:9200/some_index --i-version 6 --output=./data.json

esgo2dump --output=http://127.0.0.1:9200/some_index --o-version 6 --input=./data.json

esgo2dump --input=https://username:password@127.0.0.1:9200/some_index --output=./data.json

esgo2dump --input=http://127.0.0.1:9200/some_index --source='id;name;age;address' --output=./data.json

esgo2dump --input=http://127.0.0.1:9200/some_index --output=./data.json --query='{"match": {"name": "some_name"}}'

esgo2dump --input=http://127.0.0.1:9200/some_index --output=./data.json --query_file=my_queries.json`,
	}

	es_iversion, es_oversion string
)

func init() {
	rootCommand.PersistentFlags().BoolVar(&opt.Cfg.Debug, "debug", false, "")
	rootCommand.PersistentFlags().BoolVar(&opt.Cfg.Dev, "dev", false, "")
	rootCommand.PersistentFlags().BoolVarP(&opt.Cfg.Args.Version, "version", "v", false, "print esgo2dump version")

	rootCommand.Flags().IntVar(&opt.Cfg.Args.Timeout, "timeout", 30, "max timeout seconds per operation with limit")
	rootCommand.Flags().StringVarP(&opt.Cfg.Args.Input, "input", "i", "", "*required: input file or es url (example :data.json / http://127.0.0.1:9200/my_index)")
	rootCommand.Flags().StringVarP(&opt.Cfg.Args.Output, "output", "o", "output.json", "")
	rootCommand.Flags().StringVar(&es_iversion, "i-version", "7", "input(es) version")
	rootCommand.Flags().StringVar(&es_oversion, "o-version", "7", "output(es) version")
	rootCommand.Flags().StringVarP(&opt.Cfg.Args.Type, "type", "t", "data", "data/mapping/setting")
	rootCommand.Flags().StringVar(&opt.Cfg.Args.Source, "source", "", "query source, use ';' to separate")
	rootCommand.Flags().StringVar(&opt.Cfg.Args.Sort, "sort", "", "sort, <field>:<direction> format, for example: time:desc or name:asc")
	rootCommand.Flags().StringVar(&opt.Cfg.Args.Query, "query", "", `query dsl, example: {"bool":{"must":[{"term":{"name":{"value":"some_name"}}}],"must_not":[{"range":{"age":{"gte":18,"lt":60}}}]}}`)
	rootCommand.Flags().StringVar(&opt.Cfg.Args.QueryFile, "query_file", "", `query json file (will execute line by line)`)
	rootCommand.Flags().IntVar(&opt.Cfg.Args.Limit, "limit", 100, "")
}

func Start(ctx context.Context) error {
	return rootCommand.ExecuteContext(ctx)
}
