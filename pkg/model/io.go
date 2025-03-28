package model

import "context"

type IOType string

const (
	Input  IOType = "input"
	Output IOType = "output"
)

type IO[T any] interface {
	Cleanup()
	ReadData(ctx context.Context, limit int, query map[string]any, fields []string, sort []string) ([]T, error)
	WriteData(ctx context.Context, items []T) (int, error)
	ReadMapping(ctx context.Context) (map[string]any, error)
	WriteMapping(ctx context.Context, mapping map[string]any) error
	ReadSetting(ctx context.Context) (map[string]any, error)
	WriteSetting(ctx context.Context, setting map[string]any) error
}
