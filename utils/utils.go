package utils

import "fmt"

func Assert[T comparable](msg string, a, b T) {
	if a != b {
		panic(fmt.Sprintf("%s. Got a = %#v, b = %#v", msg, a, b))
	}
}

func Min[T ~int | ~uint64](a, b T) T {
	if a < b {
		return a
	}

	return b
}

func Max[T ~int | ~uint64](a, b T) T {
	if a > b {
		return a
	}

	return b
}
