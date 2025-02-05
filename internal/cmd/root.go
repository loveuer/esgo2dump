package cmd

import (
	"context"
	"github.com/loveuer/esgo2dump/internal/opt"
	"github.com/spf13/cobra"
)

const (
	example = `
esgo2dump -i https://<user>:<password>@<es_node1_host>:<es_node1_port>,<es_node2_host>:<es_node2_port>/some_index?ping=false&sniff=false -o ./data.json

esgo2dump --input=http://127.0.0.1:9200/some_index --output=./data.json

esgo2dump --input=http://127.0.0.1:9200/some_index --output=http://192.168.1.1:9200/some_index --limit=5000

esgo2dump --input=https://username:password@127.0.0.1:9200/some_index --output=./data.json

esgo2dump --input=http://127.0.0.1:9200/some_index --source='id;name;age;address' --output=./data.json

esgo2dump --input=http://127.0.0.1:9200/some_index --output=./data.json --query='{"match": {"name": "some_name"}}'

esgo2dump --input=http://127.0.0.1:9200/some_index --output=./data.json --query_file=my_queries.json`
)

var rootCommand = &cobra.Command{
	Use:           "esgo2dump",
	Short:         "esgo2dump is alternative to elasticdump",
	Example:       example,
	SilenceUsage:  true,
	SilenceErrors: true,
	PreRunE:       preRun,
	RunE:          run,
}

func initRoot(cmds ...*cobra.Command) *cobra.Command {
	rootCommand.PersistentFlags().BoolVar(&opt.Cfg.Debug, "debug", false, "")
	rootCommand.PersistentFlags().BoolVar(&opt.Cfg.Dev, "dev", false, "")
	rootCommand.PersistentFlags().BoolVar(&opt.Cfg.DisablePing, "disable-ping", false, "")
	rootCommand.PersistentFlags().BoolVarP(&opt.Cfg.Args.Version, "version", "v", false, "print esgo2dump version")

	// rootCommand.Flags().IntVar(&opt.Cfg.Args.Timeout, "timeout", 30, "max timeout seconds per operation with limit")
	rootCommand.Flags().StringVarP(&opt.Cfg.Args.Input, "input", "i", "", "*required: input file or es url (example :data.json / http://127.0.0.1:9200/my_index)")
	rootCommand.Flags().StringVarP(&opt.Cfg.Args.Output, "output", "o", "output.json", "")
	rootCommand.Flags().StringVarP(&opt.Cfg.Args.Type, "type", "t", "data", "data/mapping/setting")
	rootCommand.Flags().StringVar(&opt.Cfg.Args.Field, "field", "", "query include field, use ',' to separate")
	rootCommand.Flags().StringVar(&opt.Cfg.Args.Sort, "sort", "", "sort, <field>:<direction> format, for example: time:desc or name:asc, user ',' to separate")
	rootCommand.Flags().StringVar(&opt.Cfg.Args.Query, "query", "", `query dsl, example: {"bool":{"must":[{"term":{"name":{"value":"some_name"}}}],"must_not":[{"range":{"age":{"gte":18,"lt":60}}}]}}`)
	rootCommand.Flags().StringVar(&opt.Cfg.Args.QueryFile, "query_file", "", `query json file (will execute line by line)`)
	rootCommand.Flags().IntVar(&opt.Cfg.Args.Limit, "limit", 100, "")
	rootCommand.Flags().IntVar(&opt.Cfg.Args.Max, "max", 0, "max dump records")

	rootCommand.AddCommand(cmds...)

	return rootCommand
}

func Run(ctx context.Context) error {
	return rootCommand.ExecuteContext(ctx)
}
