package interfaces

type IO int64

const (
	IOInput IO = iota
	IOOutput
)

func (io IO) Code() string {
	switch io {
	case IOInput:
		return "input"
	case IOOutput:
		return "output"
	default:
		return "unknown"
	}
}

type DataType int64

const (
	DataTypeData DataType = iota
	DataTypeMapping
	DataTypeSetting
)
