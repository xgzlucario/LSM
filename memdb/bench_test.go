package memdb

import (
	"fmt"
	"testing"
)

func BenchmarkPut(b *testing.B) {
	db := New(testMemDBSize)
	for i := 0; i < b.N; i++ {
		k := []byte(fmt.Sprintf("%08d", i))

		if db.Put(k, k, typeVal) {
			db = New(testMemDBSize)
		}
	}
}

func BenchmarkPutReset(b *testing.B) {
	db := New(testMemDBSize)
	for i := 0; i < b.N; i++ {
		k := []byte(fmt.Sprintf("%08d", i))

		if db.Put(k, k, typeVal) {
			db.Reset()
		}
	}
}

func BenchmarkGet(b *testing.B) {
	db := New(testMemDBSize)
	for i := 0; i < 10000; i++ {
		k := []byte(fmt.Sprintf("%08d", i))
		db.Put(k, k, typeVal)
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		k := []byte(fmt.Sprintf("%08d", i))
		db.Get(k)
	}
}
