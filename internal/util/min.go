package util

func Min[T ~string | ~int | ~int64 | ~uint64 | ~float64 | ~float32 | ~int32 | ~uint32 | ~int16 | ~uint16 | ~int8 | ~uint8](a, b T) T {
	if a <= b {
		return a
	}

	return b
}
