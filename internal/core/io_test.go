package core

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/loveuer/esgo2dump/internal/opt"
	"github.com/loveuer/esgo2dump/pkg/model"
)

func TestNewIO_FileInput(t *testing.T) {
	tmpDir := t.TempDir()
	inFile := filepath.Join(tmpDir, "input.json")
	// create empty file
	if err := os.WriteFile(inFile, []byte("{}\n"), 0o644); err != nil {
		t.Fatalf("write input: %v", err)
	}

	io, err := NewIO(context.Background(), inFile, model.Input)
	if err != nil || io == nil {
		t.Fatalf("NewIO(file input) got err=%v io=%v", err, io)
	}
}

func TestNewIO_SplitOutput(t *testing.T) {
	tmpDir := t.TempDir()
	opt.Cfg.Args.SplitLimit = 2
	defer func() { opt.Cfg.Args.SplitLimit = 0 }()
	opt.Cfg.Args.Input = "http://127.0.0.1:9200/my_index"

	io, err := NewIO(context.Background(), tmpDir, model.Output)
	if err != nil || io == nil {
		t.Fatalf("NewIO(split output) got err=%v io=%v", err, io)
	}
}
