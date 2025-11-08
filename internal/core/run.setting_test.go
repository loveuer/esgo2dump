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

func TestRunSetting_FileToFile(t *testing.T) {
	tmpDir := t.TempDir()
	inPath := filepath.Join(tmpDir, "in_setting.json")
	outPath := filepath.Join(tmpDir, "out_setting.json")

	// prepare input setting file
	inSet := map[string]any{
		"settings": map[string]any{
			"number_of_shards":   1,
			"number_of_replicas": 0,
		},
	}
	bs, _ := json.Marshal(inSet)
	if err := os.WriteFile(inPath, bs, 0o644); err != nil {
		t.Fatalf("write input setting: %v", err)
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
	if err := RunSetting(cmd, in, out); err != nil {
		t.Fatalf("RunSetting error: %v", err)
	}

	got, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("read output setting: %v", err)
	}
	var gotSet map[string]any
	if err := json.Unmarshal(got, &gotSet); err != nil {
		t.Fatalf("unmarshal output: %v", err)
	}
	if _, ok := gotSet["settings"]; !ok {
		t.Fatalf("output missing settings key: %v", gotSet)
	}
}
