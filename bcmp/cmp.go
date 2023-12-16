// Package bcmp is bytes comparetor.
package bcmp

import (
	"bytes"
	"slices"
)

func Compare(a, b []byte) int { return bytes.Compare(a, b) }

// Less return a < b.
func Less(a, b []byte) bool { return Compare(a, b) < 0 }

// LessEqual return a <= b.
func LessEqual(a, b []byte) bool { return Compare(a, b) <= 0 }

// Equal return a == b.
func Equal(a, b []byte) bool { return Compare(a, b) == 0 }

// GreatEqual return a >= b.
func GreatEqual(a, b []byte) bool { return Compare(a, b) >= 0 }

// Great return a > b.
func Great(a, b []byte) bool { return Compare(a, b) > 0 }

// Between return target is in [a,b].
func Between(target, a, b []byte) bool { return LessEqual(a, target) && LessEqual(target, b) }

// Min
func Min(a, b []byte) []byte {
	if Less(a, b) {
		return a
	}
	return b
}

// Max
func Max(a, b []byte) []byte {
	if Great(a, b) {
		return a
	}
	return b
}

type Interval struct {
	Min, Max []byte
}

// MergeInterval
func MergeInterval(input []Interval) []Interval {
	slices.SortFunc(input, func(a, b Interval) int {
		return Compare(a.Min, b.Min)
	})

	res := make([]Interval, 0, len(input)/2)

	for _, i := range input {
		if len(res) == 0 || Less(res[len(res)-1].Max, i.Min) {
			res = append(res, i)
		} else {
			res[len(res)-1].Max = Max(res[len(res)-1].Max, i.Max)
		}
	}
	return res
}

// MergeIntervalIndex
func MergeIntervalIndex(input []Interval) (res [][]int) {
	var lp, rp int
	cur := input[0]
	for {
		if rp >= len(input) {
			res = append(res, makeSlice(lp, rp))
			break
		}
		if LessEqual(input[rp].Min, cur.Max) {
			cur.Max = Max(cur.Max, input[rp].Max)
			rp++

		} else {
			res = append(res, makeSlice(lp, rp))
			lp, rp = rp, rp+1
			cur = input[lp]
		}
	}
	return
}

func makeSlice(start, end int) []int {
	slice := make([]int, end-start)
	for i := range slice {
		slice[i] = start + i
	}
	return slice
}
