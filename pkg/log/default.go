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

	panicLogger = func(prefix, timestamp, msg string, data ...any) {
		panic(fmt.Sprintf(prefix+"| "+timestamp+" | "+msg+"\n", data...))
	}

	fatalLogger = func(prefix, timestamp, msg string, data ...any) {
		fmt.Printf(prefix+"| "+timestamp+" | "+msg+"\n", data...)
		os.Exit(1)
	}

	DefaultLogger = &logger{
		Mutex:      sync.Mutex{},
		timeFormat: "2006-01-02T15:04:05",
		writer:     os.Stdout,
		level:      LogLevelInfo,
		debug:      nilLogger,
		info:       normalLogger,
		warn:       normalLogger,
		error:      normalLogger,
		panic:      panicLogger,
		fatal:      fatalLogger,
	}
)

func SetTimeFormat(format string) {
	DefaultLogger.SetTimeFormat(format)
}

func SetLogLevel(level LogLevel) {
	DefaultLogger.SetLogLevel(level)
}

func Debug(msg string, data ...any) {
	DefaultLogger.Debug(msg, data...)
}
func Info(msg string, data ...any) {
	DefaultLogger.Info(msg, data...)
}

func Warn(msg string, data ...any) {
	DefaultLogger.Warn(msg, data...)
}

func Error(msg string, data ...any) {
	DefaultLogger.Error(msg, data...)
}

func Panic(msg string, data ...any) {
	DefaultLogger.Panic(msg, data...)
}

func Fatal(msg string, data ...any) {
	DefaultLogger.Fatal(msg, data...)
}
