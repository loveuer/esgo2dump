package opt

const (
	ScrollDurationSeconds = 10 * 60
)

var (
	Debug   bool
	Timeout int

	BuffSize    = 5 * 1024 * 1024   // 5M
	MaxBuffSize = 100 * 1024 * 1024 // 100M, default elastic_search doc max size
)
