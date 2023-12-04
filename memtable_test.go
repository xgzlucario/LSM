package lsm

import (
	"bytes"
	"strconv"
	"testing"
)

func TestMerge(t *testing.T) {
	m1 := NewMemTable()
	for i := 1000; i < 5000; i++ {
		k := []byte(strconv.Itoa(i))
		m1.Put(k, k)
	}

	m2 := NewMemTable()
	for i := 2000; i < 6000; i++ {
		k := []byte(strconv.Itoa(i))
		m2.Put(k, k)
	}

	m1.Merge(m2)
	// t.Error("m3 range:", string(m1.FirstKey()), string(m1.LastKey()))

	m1.Iter(func(key, value []byte, vtype uint16) {
		if !bytes.Equal(key, value) {
			t.Error("not equal")
		}
	})
}
