package lsm

import (
	"math"
	"os"
	"strconv"
	"testing"
)

func BenchmarkSSTGet(b *testing.B) {
	m := NewMemTable(math.MaxUint32)
	for i := 0; i < 10000; i++ {
		k := []byte(strconv.Itoa(i))
		m.Put(k, k)
	}
	if err := os.WriteFile("bench.sst", EncodeTable(m), 0644); err != nil {
		panic(err)
	}
	sst, err := NewSSTable("bench.sst")
	if err != nil {
		panic(err)
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		sst.findKey([]byte(strconv.Itoa(i)))
	}
}
