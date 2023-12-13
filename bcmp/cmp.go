// Package bcmp is bytes comparetor.
package bcmp

import "bytes"

func Compare(a, b []byte) int {
	return bytes.Compare(a, b)
}

// Less return a < b.
func Less(a, b []byte) bool {
	return Compare(a, b) < 0
}

// LessEqual return a <= b.
func LessEqual(a, b []byte) bool {
	return Compare(a, b) <= 0
}

// Equal return a == b.
func Equal(a, b []byte) bool {
	return Compare(a, b) == 0
}

// GreatEqual return a >= b.
func GreatEqual(a, b []byte) bool {
	return Compare(a, b) >= 0
}

// Great return a > b.
func Great(a, b []byte) bool {
	return Compare(a, b) > 0
}

// Between return target is in [a,b].
func Between(target, a, b []byte) bool {
	return LessEqual(a, target) && LessEqual(target, b)
}

// Min
func Min(a, b []byte) []byte {
	if LessEqual(a, b) {
		return a
	}
	return b
}

// Max
func Max(a, b []byte) []byte {
	if GreatEqual(a, b) {
		return a
	}
	return b
}
