package util

func Min[T ~string | ~int | ~int64 | ~uint64 | ~float64 | ~float32 | ~int32 | ~uint32 | ~int16 | ~uint16 | ~int8 | ~uint8](a, b T) T {
	if a <= b {
		return a
	}

	return b
}

func AbsMin(a, b uint64) uint64 {
	if a == 0 {
		return b
	}

	if b == 0 {
		return a
	}

	return Min(a, b)
}
