package xes

import (
	"bufio"
	"fmt"
	"os"
	"testing"

	elastic "github.com/elastic/go-elasticsearch/v7"
	"github.com/loveuer/esgo2dump/internal/tool"
)

func TestGetESMapping(t *testing.T) {
	endpoint := "http://127.0.0.1:9200"
	index := "some_index"

	cli, err := elastic.NewClient(elastic.Config{
		Addresses: []string{endpoint},
	})
	if err != nil {
		t.Error(1, err)
		return
	}

	resp, err := cli.Info(cli.Info.WithContext(tool.Timeout(5)))
	if err != nil {
		t.Error(2, err)
		return
	}

	t.Log("info:", resp.String())

	r, err := cli.Indices.GetMapping(
		cli.Indices.GetMapping.WithIndex(index),
	)
	if err != nil {
		t.Error(3, err)
		return
	}

	t.Log("get source:", r.String())
}

func TestScanWithInterrupt(t *testing.T) {
	filename := "test_scan.txt"
	f, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		t.Error(1, err)
		return
	}
	defer func() {
		os.Remove(filename)
	}()
	f.WriteString(`line 01
line 02
line 03
line 04
line 05
line 06
line 07
line 08
line 09
line 10
line 11
line 12
line 13
line 14
line 15`)
	f.Close()

	of, err := os.Open(filename)
	if err != nil {
		t.Error(2, err)
		return
	}

	scanner := bufio.NewScanner(of)

	count := 0
	for scanner.Scan() {
		text := scanner.Text()
		fmt.Printf("[line: %2d] = %s\n", count, text)
		count++

		if count > 5 {
			break
		}
	}

	count = 0
	for scanner.Scan() {
		text := scanner.Text()
		fmt.Printf("[line: %2d] = %s\n", count, text)
		count++

		if count > 5 {
			break
		}
	}

	count = 0
	for scanner.Scan() {
		text := scanner.Text()
		fmt.Printf("[line: %2d] = %s\n", count, text)
		count++
	}
}
