package xfile

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/loveuer/esgo2dump/pkg/log"
	"github.com/loveuer/esgo2dump/pkg/model"
)

type splitClient struct {
	dir          string
	indexName    string
	splitLimit   int
	currentFile  *os.File
	currentCount int
	fileIndex    int
	mu           sync.Mutex
}

func (c *splitClient) Cleanup() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.currentFile != nil {
		c.currentFile.Close()
		c.currentFile = nil
	}
}

func (c *splitClient) ReadData(ctx context.Context, limit int, query map[string]any, fields []string, sort []string) ([]map[string]any, error) {
	return nil, fmt.Errorf("split client does not support read")
}

func (c *splitClient) WriteData(ctx context.Context, items []map[string]any) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	var total int
	for _, item := range items {
		// Check if we need to create a new file
		if c.currentFile == nil || c.currentCount >= c.splitLimit {
			if err := c.rotateFile(); err != nil {
				return total, err
			}
		}

		bs, err := json.Marshal(item)
		if err != nil {
			return total, err
		}

		if _, err = c.currentFile.Write(bs); err != nil {
			return total, err
		}

		if _, err = c.currentFile.WriteString("\n"); err != nil {
			return total, err
		}

		c.currentCount++
		total++
	}

	return total, nil
}

func (c *splitClient) rotateFile() error {
	// Close current file if exists
	if c.currentFile != nil {
		if err := c.currentFile.Close(); err != nil {
			log.Warn("failed to close current split file: %s", err.Error())
		}
		c.currentFile = nil
		c.currentCount = 0
	}

	// Create new file
	c.fileIndex++
	filename := fmt.Sprintf("%s-%d.json", c.indexName, c.fileIndex)
	filepath := filepath.Join(c.dir, filename)

	f, err := os.OpenFile(filepath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("failed to create split file %s: %w", filepath, err)
	}

	c.currentFile = f
	c.currentCount = 0

	log.Debug("created new split file: %s", filepath)
	return nil
}

func (c *splitClient) ReadMapping(ctx context.Context) (map[string]any, error) {
	return nil, fmt.Errorf("split client does not support read mapping")
}

func (c *splitClient) WriteMapping(ctx context.Context, mapping map[string]any) error {
	return fmt.Errorf("split client does not support write mapping")
}

func (c *splitClient) ReadSetting(ctx context.Context) (map[string]any, error) {
	return nil, fmt.Errorf("split client does not support read setting")
}

func (c *splitClient) WriteSetting(ctx context.Context, setting map[string]any) error {
	return fmt.Errorf("split client does not support write setting")
}

func NewSplitClient(dir string, indexName string, splitLimit int) (model.IO[map[string]any], error) {
	if splitLimit <= 0 {
		return nil, fmt.Errorf("splitLimit must be > 0")
	}

	// Ensure directory exists
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create output directory: %w", err)
	}

	c := &splitClient{
		dir:        dir,
		indexName:  indexName,
		splitLimit: splitLimit,
		fileIndex:  0,
	}

	return c, nil
}
