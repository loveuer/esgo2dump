package log

import (
	"fmt"
	"os"
	"sync"
)

var (
	nilLogger    = func(prefix, timestamp, msg string, data ...any) {}
	normalLogger = func(prefix, timestamp, msg string, data ...any) {
		fmt.Printf(prefix+"| "+timestamp+" | "+msg+"\n", data...)
	}

	defaultLogger = &logger{
		Mutex:      sync.Mutex{},
		timeFormat: "2006-01-02T15:04:05",
		writer:     os.Stdout,
		level:      LogLevelInfo,
		debug:      nilLogger,
		info:       normalLogger,
		warn:       normalLogger,
		error:      normalLogger,
	}
)

func init() {
}

func SetTimeFormat(format string) {
	defaultLogger.SetTimeFormat(format)
}

func SetLogLevel(level LogLevel) {
	defaultLogger.SetLogLevel(level)
}
