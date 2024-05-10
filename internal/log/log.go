package log

import (
	"bytes"
	"fmt"
	"github.com/fatih/color"
	"sync"
	"time"
)

var (
	red    = color.New(color.FgRed)
	green  = color.New(color.FgGreen)
	yellow = color.New(color.FgYellow)

	locker = &sync.Mutex{}

	timeFormat = "06/01/02T15:04:05"
)

func Info(msg string, data ...any) {
	buf := &bytes.Buffer{}
	_, _ = green.Fprint(buf, "Info  ")
	_, _ = fmt.Fprintf(buf, "| %s | ", time.Now().Format(timeFormat))
	_, _ = fmt.Fprintf(buf, msg, data...)
	fmt.Println(buf.String())
}

func Warn(msg string, data ...any) {
	buf := &bytes.Buffer{}
	_, _ = yellow.Fprint(buf, "Warn  ")
	_, _ = fmt.Fprintf(buf, "| %s | ", time.Now().Format(timeFormat))
	_, _ = fmt.Fprintf(buf, msg, data...)
	fmt.Println(buf.String())
}

func Error(msg string, data ...any) {
	buf := &bytes.Buffer{}
	_, _ = red.Fprint(buf, "Error ")
	_, _ = fmt.Fprintf(buf, "| %s | ", time.Now().Format(timeFormat))
	_, _ = fmt.Fprintf(buf, msg, data...)
	fmt.Println(buf.String())
}
