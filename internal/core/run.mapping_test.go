package core

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/loveuer/esgo2dump/pkg/model"
	"github.com/loveuer/esgo2dump/internal/xfile"
	"github.com/spf13/cobra"
)

func TestRunMapping_FileToFile(t *testing.T) {
	tmpDir := t.TempDir()
	inPath := filepath.Join(tmpDir, "in_mapping.json")
	outPath := filepath.Join(tmpDir, "out_mapping.json")

	// prepare input mapping file
	inMap := map[string]any{
		"mappings": map[string]any{
			"properties": map[string]any{
				"id": map[string]any{"type": "keyword"},
			},
		},
	}
	bs, _ := json.Marshal(inMap)
	if err := os.WriteFile(inPath, bs, 0o644); err != nil {
		t.Fatalf("write input mapping: %v", err)
	}

	in, err := xfile.NewClient(inPath, model.Input)
	if err != nil {
		t.Fatalf("new input client: %v", err)
	}
	out, err := xfile.NewClient(outPath, model.Output)
	if err != nil {
		t.Fatalf("new output client: %v", err)
	}

	cmd := &cobra.Command{}
	cmd.SetContext(context.Background())
	if err := RunMapping(cmd, in, out); err != nil {
		t.Fatalf("RunMapping error: %v", err)
	}

	got, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("read output mapping: %v", err)
	}
	var gotMap map[string]any
	if err := json.Unmarshal(got, &gotMap); err != nil {
		t.Fatalf("unmarshal output: %v", err)
	}
	if _, ok := gotMap["mappings"]; !ok {
		t.Fatalf("output missing mappings key: %v", gotMap)
	}
}
