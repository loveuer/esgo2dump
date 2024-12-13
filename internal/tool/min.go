package tool

import "github.com/loveuer/esgo2dump/internal/opt"

func Min[T ~string | ~int | ~int64 | ~uint64 | ~float64 | ~float32 | ~int32 | ~uint32 | ~int16 | ~uint16 | ~int8 | ~uint8](a, b T) T {
	if a <= b {
		return a
	}

	return b
}

func CalcSize(size, max, total int) int {
	fs := size
	if fs == 0 {
		fs = opt.DefaultSize
	}

	if max == 0 {
		return fs
	}

	if max > 0 && total >= max {
		return 0
	}

	if max-total > fs {
		return max - total
	}

	return fs
}
