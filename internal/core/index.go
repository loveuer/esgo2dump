package core

import (
	"net/url"
	"strings"

	"github.com/loveuer/esgo2dump/internal/tool"
)

// ExtractIndexName extracts index name from input URI
// Returns empty string if URI is not an ES URL or index cannot be extracted
func ExtractIndexName(inputURI string) string {
	target, err := url.Parse(inputURI)
	if err != nil {
		return ""
	}

	if err = tool.ValidScheme(target.Scheme); err != nil {
		return ""
	}

	// elastic uri
	index := strings.TrimPrefix(target.Path, "/")
	return index
}
