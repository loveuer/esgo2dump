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

	defaultLogger = &logger{
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
	defaultLogger.SetTimeFormat(format)
}

func SetLogLevel(level LogLevel) {
	defaultLogger.SetLogLevel(level)
}

func Debug(msg string, data ...any) {
	defaultLogger.Debug(msg, data...)
}
func Info(msg string, data ...any) {
	defaultLogger.Info(msg, data...)
}

func Warn(msg string, data ...any) {
	defaultLogger.Warn(msg, data...)
}

func Error(msg string, data ...any) {
	defaultLogger.Error(msg, data...)
}

func Panic(msg string, data ...any) {
	defaultLogger.Panic(msg, data...)
}

func Fatal(msg string, data ...any) {
	defaultLogger.Fatal(msg, data...)
}
