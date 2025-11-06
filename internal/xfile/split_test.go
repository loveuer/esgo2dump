package xfile

import (
	"bufio"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestNewSplitClient(t *testing.T) {
	tmpDir := t.TempDir()

	tests := []struct {
		name      string
		dir       string
		indexName string
		splitLimit int
		wantErr   bool
	}{
		{
			name:       "valid split client",
			dir:        tmpDir,
			indexName:  "test_index",
			splitLimit: 100,
			wantErr:    false,
		},
		{
			name:       "invalid split limit",
			dir:        tmpDir,
			indexName:  "test_index",
			splitLimit: 0,
			wantErr:    true,
		},
		{
			name:       "negative split limit",
			dir:        tmpDir,
			indexName:  "test_index",
			splitLimit: -1,
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, err := NewSplitClient(tt.dir, tt.indexName, tt.splitLimit)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewSplitClient() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && client == nil {
				t.Error("NewSplitClient() returned nil client without error")
			}
			if client != nil {
				client.Cleanup()
			}
		})
	}
}

func TestSplitClient_WriteData(t *testing.T) {
	tmpDir := t.TempDir()
	indexName := "test_index"
	splitLimit := 3 // Small limit for testing

	client, err := NewSplitClient(tmpDir, indexName, splitLimit)
	if err != nil {
		t.Fatalf("Failed to create split client: %v", err)
	}
	defer client.Cleanup()

	// Write items that should create multiple files
	items := []map[string]any{
		{"id": 1, "name": "item1"},
		{"id": 2, "name": "item2"},
		{"id": 3, "name": "item3"}, // First file should be full here
		{"id": 4, "name": "item4"}, // Should start new file
		{"id": 5, "name": "item5"},
		{"id": 6, "name": "item6"}, // Second file should be full
		{"id": 7, "name": "item7"}, // Should start third file
	}

	written, err := client.WriteData(context.Background(), items)
	if err != nil {
		t.Fatalf("WriteData() error = %v", err)
	}

	if written != len(items) {
		t.Errorf("WriteData() wrote %d items, want %d", written, len(items))
	}

	// Verify files were created
	expectedFiles := []string{
		filepath.Join(tmpDir, "test_index-1.json"),
		filepath.Join(tmpDir, "test_index-2.json"),
		filepath.Join(tmpDir, "test_index-3.json"),
	}

	for _, file := range expectedFiles {
		if _, err := os.Stat(file); os.IsNotExist(err) {
			t.Errorf("Expected file %s was not created", file)
		}
	}

	// Verify file contents
	verifyFileContent(t, expectedFiles[0], items[0:3])
	verifyFileContent(t, expectedFiles[1], items[3:6])
	verifyFileContent(t, expectedFiles[2], items[6:7])
}

func verifyFileContent(t *testing.T, filepath string, expectedItems []map[string]any) {
	t.Helper()

	file, err := os.Open(filepath)
	if err != nil {
		t.Fatalf("Failed to open file %s: %v", filepath, err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	var lines []map[string]any
	for scanner.Scan() {
		var item map[string]any
		if err := json.Unmarshal(scanner.Bytes(), &item); err != nil {
			t.Fatalf("Failed to unmarshal line in %s: %v", filepath, err)
		}
		lines = append(lines, item)
	}

	if err := scanner.Err(); err != nil {
		t.Fatalf("Error reading file %s: %v", filepath, err)
	}

	if len(lines) != len(expectedItems) {
		t.Errorf("File %s has %d lines, want %d", filepath, len(lines), len(expectedItems))
	}

	for i, expected := range expectedItems {
		if i >= len(lines) {
			t.Errorf("File %s: missing line %d", filepath, i)
			continue
		}
		
		gotID := lines[i]["id"]
		wantID := expected["id"]
		
		// JSON numbers are unmarshalled as float64, so we need to compare carefully
		gotFloat, gotOK := gotID.(float64)
		if !gotOK {
			if gotInt, ok := gotID.(int); ok {
				gotFloat = float64(gotInt)
				gotOK = true
			}
		}
		
		wantFloat, wantOK := wantID.(float64)
		if !wantOK {
			if wantInt, ok := wantID.(int); ok {
				wantFloat = float64(wantInt)
				wantOK = true
			}
		}
		
		if !gotOK || !wantOK || gotFloat != wantFloat {
			t.Errorf("File %s line %d: got id %v (type %T), want %v (type %T)", filepath, i, gotID, gotID, wantID, wantID)
		}
	}
}

func TestSplitClient_Cleanup(t *testing.T) {
	tmpDir := t.TempDir()
	client, err := NewSplitClient(tmpDir, "test_index", 10)
	if err != nil {
		t.Fatalf("Failed to create split client: %v", err)
	}

	// Write some data to create a file
	items := []map[string]any{{"id": 1}}
	_, err = client.WriteData(context.Background(), items)
	if err != nil {
		t.Fatalf("WriteData() error = %v", err)
	}

	// Cleanup should close the file
	client.Cleanup()

	// Try to write again - should create a new file
	items2 := []map[string]any{{"id": 2}}
	_, err = client.WriteData(context.Background(), items2)
	if err != nil {
		t.Fatalf("WriteData() after cleanup error = %v", err)
	}

	client.Cleanup()
}

func TestSplitClient_ReadData(t *testing.T) {
	tmpDir := t.TempDir()
	client, err := NewSplitClient(tmpDir, "test_index", 10)
	if err != nil {
		t.Fatalf("Failed to create split client: %v", err)
	}
	defer client.Cleanup()

	_, err = client.ReadData(context.Background(), 10, nil, nil, nil)
	if err == nil {
		t.Error("ReadData() should return error for split client")
	}
}
