package es7

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	elastic "github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esutil"
	"github.com/loveuer/esgo2dump/log"
	"github.com/loveuer/esgo2dump/model"
)

func WriteData(ctx context.Context, client *elastic.Client, index string, docsCh <-chan []*model.ESSource, logs ...log.WroteLogger) error {
	var (
		err     error
		indexer esutil.BulkIndexer
		total   int
	)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case docs, ok := <-docsCh:
			if !ok {
				return nil
			}

			if len(docs) == 0 {
				continue
			}

			count := 0

			if indexer, err = esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
				Client:     client,
				Index:      index,
				ErrorTrace: true,
				OnError: func(ctx context.Context, err error) {

				},
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

			if len(logs) > 0 && logs[0] != nil {
				logs[0].Info("Dump: succeed=%d total=%d docs succeed!!!", count, total)
			}
		}
	}
}
