package interfaces

import "context"

type DumpIO interface {
	ReadData(ctx context.Context, size int, query map[string]any, includeFields []string) ([]*ESSource, error)
	WriteData(ctx context.Context, docs []*ESSource) (int, error)

	ResetOffset()

	ReadMapping(context.Context) (map[string]any, error)
	WriteMapping(context.Context, map[string]any) error

	ReadSetting(ctx context.Context) (map[string]any, error)
	WriteSetting(context.Context, map[string]any) error

	Close() error

	IOType() IO
	IsFile() bool
}
