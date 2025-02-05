package tool

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"reflect"
	"strings"

	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/loveuer/esgo2dump/pkg/log"
)

func TablePrinter(data any, writers ...io.Writer) {
	var w io.Writer = os.Stdout
	if len(writers) > 0 && writers[0] != nil {
		w = writers[0]
	}

	t := table.NewWriter()
	structPrinter(t, "", data)
	_, _ = fmt.Fprintln(w, t.Render())
}

func structPrinter(w table.Writer, prefix string, item any) {
Start:
	rv := reflect.ValueOf(item)
	if rv.IsZero() {
		return
	}

	for rv.Type().Kind() == reflect.Pointer {
		rv = rv.Elem()
	}

	switch rv.Type().Kind() {
	case reflect.Invalid,
		reflect.Uintptr,
		reflect.Chan,
		reflect.Func,
		reflect.UnsafePointer:
	case reflect.Bool,
		reflect.Int,
		reflect.Int8,
		reflect.Int16,
		reflect.Int32,
		reflect.Int64,
		reflect.Uint,
		reflect.Uint8,
		reflect.Uint16,
		reflect.Uint32,
		reflect.Uint64,
		reflect.Float32,
		reflect.Float64,
		reflect.Complex64,
		reflect.Complex128,
		reflect.Interface:
		w.AppendRow(table.Row{strings.TrimPrefix(prefix, "."), rv.Interface()})
	case reflect.String:
		val := rv.String()
		if len(val) <= 160 {
			w.AppendRow(table.Row{strings.TrimPrefix(prefix, "."), val})
			return
		}

		w.AppendRow(table.Row{strings.TrimPrefix(prefix, "."), val[0:64] + "..." + val[len(val)-64:]})
	case reflect.Array, reflect.Slice:
		for i := 0; i < rv.Len(); i++ {
			p := strings.Join([]string{prefix, fmt.Sprintf("[%d]", i)}, ".")
			structPrinter(w, p, rv.Index(i).Interface())
		}
	case reflect.Map:
		for _, k := range rv.MapKeys() {
			structPrinter(w, fmt.Sprintf("%s.{%v}", prefix, k), rv.MapIndex(k).Interface())
		}
	case reflect.Pointer:
		goto Start
	case reflect.Struct:
		for i := 0; i < rv.NumField(); i++ {
			p := fmt.Sprintf("%s.%s", prefix, rv.Type().Field(i).Name)
			field := rv.Field(i)

			// log.Debug("TablePrinter: prefix: %s, field: %v", p, rv.Field(i))

			if !field.CanInterface() {
				return
			}

			structPrinter(w, p, field.Interface())
		}
	}
}

func TableMapPrinter(data []byte) {
	m := make(map[string]any)
	if err := json.Unmarshal(data, &m); err != nil {
		log.Warn(err.Error())
		return
	}

	t := table.NewWriter()
	addRow(t, "", m)
	fmt.Println(t.Render())
}

func addRow(w table.Writer, prefix string, m any) {
	rv := reflect.ValueOf(m)
	switch rv.Type().Kind() {
	case reflect.Map:
		for _, k := range rv.MapKeys() {
			key := k.String()
			if prefix != "" {
				key = strings.Join([]string{prefix, k.String()}, ".")
			}
			addRow(w, key, rv.MapIndex(k).Interface())
		}
	case reflect.Slice, reflect.Array:
		for i := 0; i < rv.Len(); i++ {
			addRow(w, fmt.Sprintf("%s[%d]", prefix, i), rv.Index(i).Interface())
		}
	default:
		w.AppendRow(table.Row{prefix, m})
	}
}
