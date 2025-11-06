# Testing Guide

This document describes how to run tests for esgo2dump.

## Running Tests

### Run All Tests

```bash
go test ./...
```

### Run Tests with Coverage

```bash
go test ./... -cover
```

### Run Tests with Verbose Output

```bash
go test ./... -v
```

### Run Short Tests (Skip Integration Tests)

```bash
go test ./... -short
```

### Run Tests with Race Detection

```bash
go test ./... -race
```

### Run Tests for a Specific Package

```bash
go test ./internal/core/...
go test ./internal/xfile/...
go test ./internal/tool/...
```

## Test Coverage

Current test coverage:
- `internal/core`: 6.0%
- `internal/tool`: 11.6%
- `internal/xfile`: 31.4%
- `xes/es7`: 9.4%

## Test Structure

### Unit Tests

All unit tests are located alongside their source files with the `_test.go` suffix:

- `internal/core/index_test.go` - Tests for index name extraction
- `internal/xfile/split_test.go` - Tests for split file client
- `internal/tool/min_test.go` - Tests for utility functions
- `xes/es7/client_test.go` - Tests for ES7 client (with integration test)

### Integration Tests

Integration tests that require external services (like Elasticsearch) are marked to skip in short mode:

```go
if testing.Short() {
    t.Skip("Skipping integration test in short mode")
}
```

To run integration tests, do not use the `-short` flag:

```bash
go test ./... -v
```

## Continuous Integration

GitHub Actions automatically runs tests on:
- Push to `master`, `main`, or `develop` branches
- Pull requests to `master`, `main`, or `develop` branches

The CI runs tests on:
- Multiple operating systems: Ubuntu, macOS, Windows
- Multiple Go versions: 1.18, 1.19, 1.20, 1.21

## Code Quality

The project uses `golangci-lint` for code quality checks. Configuration is in `.golangci.yml`.

To run linting locally:

```bash
golangci-lint run
```

## Writing New Tests

When adding new features, please:

1. Add unit tests for all new functions
2. Test edge cases and error conditions
3. Use table-driven tests when appropriate
4. Keep test names descriptive
5. Use `t.Helper()` for helper functions
6. Use `t.TempDir()` for temporary files/directories

### Example Test Structure

```go
func TestFunctionName(t *testing.T) {
    tests := []struct {
        name string
        input string
        want string
    }{
        {"test case 1", "input1", "expected1"},
        {"test case 2", "input2", "expected2"},
    }

    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            got := FunctionName(tt.input)
            if got != tt.want {
                t.Errorf("FunctionName() = %v, want %v", got, tt.want)
            }
        })
    }
}
```
