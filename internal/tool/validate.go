package tool

import (
	"fmt"
	"strings"
)

func ValidScheme(scheme string) error {
	switch strings.ToLower(scheme) {
	case "http", "https":
		return nil
	default:
		return fmt.Errorf("invalid scheme: %s", scheme)
	}
}
