package tool

func Min[T ~string | ~int | ~int64 | ~uint64 | ~float64 | ~float32 | ~int32 | ~uint32 | ~int16 | ~uint16 | ~int8 | ~uint8](a, b T) T {
	if a <= b {
		return a
	}

	return b
}

func CalculateLimit(limit, total, max int) int {
	if max == 0 {
		return limit
	}

	if max-total > 0 {
		return Min(max-total, limit)
	}

	return 0
}
