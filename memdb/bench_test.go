package memdb

import (
	"fmt"
	"testing"
)

func BenchmarkPut(b *testing.B) {
	mdb := New()
	for i := 0; i < b.N; i++ {
		k := []byte(fmt.Sprintf("%08d", i))
		
		if full, err := mdb.PutIsFull(k, k, typeVal); full {
			mdb = New()

		} else if err != nil {
			panic(err)
		}
	}
}

func BenchmarkGet(b *testing.B) {
	mdb := New()
	for i := 0; i < 10000; i++ {
		k := []byte(fmt.Sprintf("%08d", i))
		mdb.Put(k, k, typeVal)
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		k := []byte(fmt.Sprintf("%08d", i))
		mdb.Get(k)
	}
}
