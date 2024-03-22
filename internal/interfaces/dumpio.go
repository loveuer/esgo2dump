package interfaces

import "context"

type DumpIO interface {
	ReadData(context.Context, int) ([]*ESSource, error)
	WriteData(ctx context.Context, docs []*ESSource) (int, error)

	ReadMapping(context.Context) (map[string]any, error)
	WriteMapping(context.Context, map[string]any) error

	ReadSetting(ctx context.Context) (map[string]any, error)
	WriteSetting(context.Context, map[string]any) error

	Close() error

	IOType() IO
	IsFile() bool
}
