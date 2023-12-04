package lsm

import (
	"math"
	"os"
	"strconv"
	"testing"
)

func getMemTable(size int) *MemTable {
	m := NewMemTable(math.MaxUint32)
	for i := 0; i < size; i++ {
		k := []byte(strconv.Itoa(i))
		m.Put(k, k)
	}
	return m
}

func BenchmarkMemTable(b *testing.B) {
	b.Run("Put", func(b *testing.B) {
		m := NewMemTable(math.MaxUint32)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			k := []byte(strconv.Itoa(i))
			m.Put(k, k)
		}
	})

	b.Run("Get-10000", func(b *testing.B) {
		m := getMemTable(10000)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			m.Get([]byte(strconv.Itoa(i)))
		}
	})
}

func BenchmarkSSTGet(b *testing.B) {
	m := getMemTable(10000)
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
