package cmd

import (
	"context"

	"github.com/loveuer/esgo2dump/internal/opt"
	"github.com/spf13/cobra"
)

var (
	rootCommand = &cobra.Command{
		Use:           "esgo2dump",
		Short:         "esgo2dump is alternative to elasticdump",
		SilenceUsage:  true,
		SilenceErrors: true,
		RunE:          run,
		Example: `
esgo2dump --input=http://127.0.0.1:9200/some_index --output=./data.json

esgo2dump --input=http://127.0.0.1:9200/some_index --output=http://192.168.1.1:9200/some_index --limit=5000

esgo2dump --input=https://username:password@127.0.0.1:9200/some_index --output=./data.json

esgo2dump --input=http://127.0.0.1:9200/some_index --output=./data.json --query='{"match": {"name": "some_name"}}'`,
	}

	f_input  string
	f_output string
	f_limit  int
	f_type   string
	f_query  string
)

func init() {
	rootCommand.Flags().BoolVar(&opt.Debug, "debug", false, "")
	rootCommand.Flags().IntVar(&opt.Timeout, "timeout", 30, "max timeout seconds per operation with limit")

	rootCommand.Flags().StringVarP(&f_input, "input", "i", "http://127.0.0.1:9200/my_index", "")
	rootCommand.Flags().StringVarP(&f_output, "output", "o", "output.json", "")
	rootCommand.Flags().StringVarP(&f_type, "type", "t", "data", "data/mapping/setting")
	rootCommand.Flags().StringVarP(&f_query, "query", "q", "", `query dsl, example: {"bool":{"must":[{"term":{"name":{"value":"some_name"}}}],"must_not":[{"range":{"age":{"gte":18,"lt":60}}}]}}`)
	rootCommand.Flags().IntVarP(&f_limit, "limit", "l", 100, "")
}

func Start(ctx context.Context) error {
	return rootCommand.ExecuteContext(ctx)
}
