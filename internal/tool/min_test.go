package tool

import (
	"testing"
)

func TestMin(t *testing.T) {
	tests := []struct {
		name string
		a    int
		b    int
		want int
	}{
		{"a < b", 1, 2, 1},
		{"a > b", 2, 1, 1},
		{"a == b", 2, 2, 2},
		{"negative numbers", -1, -2, -2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Min(tt.a, tt.b)
			if got != tt.want {
				t.Errorf("Min(%d, %d) = %d, want %d", tt.a, tt.b, got, tt.want)
			}
		})
	}
}

func TestCalculateLimit(t *testing.T) {
	tests := []struct {
		name string
		limit int
		total int
		max   int
		want  int
	}{
		{"no max limit", 100, 0, 0, 100},
		{"max limit not reached", 100, 0, 1000, 100},
		{"max limit reached", 100, 1000, 1000, 0},
		{"max limit partially reached", 100, 950, 1000, 50},
		{"limit exceeds remaining", 100, 990, 1000, 10},
		{"exact match", 100, 900, 1000, 100},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := CalculateLimit(tt.limit, tt.total, tt.max)
			if got != tt.want {
				t.Errorf("CalculateLimit(%d, %d, %d) = %d, want %d", tt.limit, tt.total, tt.max, got, tt.want)
			}
		})
	}
}
