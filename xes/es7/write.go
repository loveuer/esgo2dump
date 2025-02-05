package es7

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	elastic "github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esutil"
	"github.com/loveuer/esgo2dump/pkg/log"
	"github.com/loveuer/esgo2dump/pkg/model"
)

func WriteData[T any](ctx context.Context, client *elastic.Client, index string, docs ...*model.ESSource[T]) error {
	var (
		err     error
		indexer esutil.BulkIndexer
		total   int
	)

	if len(docs) == 0 {
		return nil
	}

	count := 0

	if indexer, err = esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
		NumWorkers:    0,
		FlushBytes:    0,
		FlushInterval: 0,
		Client:        client,
		Decoder:       nil,
		OnError: func(ctx context.Context, err error) {
			log.Error("es7.writer: on error log, err = %s", err.Error())
		},
		Index:               index,
		ErrorTrace:          true,
		FilterPath:          []string{},
		Header:              map[string][]string{},
		Human:               false,
		Pipeline:            "",
		Pretty:              false,
		Refresh:             "",
		Routing:             "",
		Source:              []string{},
		SourceExcludes:      []string{},
		SourceIncludes:      []string{},
		Timeout:             0,
		WaitForActiveShards: "",
	}); err != nil {
		return err
	}

	for _, doc := range docs {
		var bs []byte

		if bs, err = json.Marshal(doc.Content); err != nil {
			return err
		}

		if err = indexer.Add(context.Background(), esutil.BulkIndexerItem{
			Action:     "index",
			Index:      index,
			DocumentID: doc.DocId,
			Body:       bytes.NewReader(bs),
			OnFailure: func(ctx context.Context, item esutil.BulkIndexerItem, item2 esutil.BulkIndexerResponseItem, bulkErr error) {
				log.Error("es7.writer: on failure err log, err = %s", bulkErr.Error())
			},
		}); err != nil {
			return err
		}

		count++
	}

	total += count

	if err = indexer.Close(ctx); err != nil {
		return err
	}

	stats := indexer.Stats()
	if stats.NumFailed > 0 {
		return fmt.Errorf("write to es failed_count=%d bulk_count=%d", stats.NumFailed, count)
	}

	return nil
}
