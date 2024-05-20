package interfaces

import (
	"context"
	"github.com/loveuer/esgo2dump/model"
)

type DumpIO interface {
	ReadData(ctx context.Context, size int, query map[string]any, includeFields []string) (<-chan []*model.ESSource, <-chan error)
	WriteData(ctx context.Context, docs []*model.ESSource) (int, error)

	ReadMapping(context.Context) (map[string]any, error)
	WriteMapping(context.Context, map[string]any) error

	ReadSetting(ctx context.Context) (map[string]any, error)
	WriteSetting(context.Context, map[string]any) error

	Close() error

	IOType() IO
	IsFile() bool
}
