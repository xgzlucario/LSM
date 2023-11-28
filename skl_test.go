package lsm

import (
	"strconv"
	"testing"

	"github.com/andy-kimball/arenaskl"
)

func BenchmarkSkl(b *testing.B) {
	b.Run("skl-1mb", func(b *testing.B) {
		skl := NewSkiplist(1 * MB)
		var it arenaskl.Iterator
		it.Init(skl.Skiplist)

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			k := []byte(strconv.Itoa(i))
			it.Add(k, k, uint16(i))
		}
	})

	b.Run("skl-2mb", func(b *testing.B) {
		skl := NewSkiplist(2 * MB)
		var it arenaskl.Iterator
		it.Init(skl.Skiplist)

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			k := []byte(strconv.Itoa(i))
			it.Add(k, k, uint16(i))
		}
	})

	b.Run("skl-4mb", func(b *testing.B) {
		skl := NewSkiplist(4 * MB)
		var it arenaskl.Iterator
		it.Init(skl.Skiplist)

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			k := []byte(strconv.Itoa(i))
			it.Add(k, k, uint16(i))
		}
	})
}
