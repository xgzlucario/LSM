package lsm

import (
	"math"
	"os"
	"strconv"
	"testing"
	"time"
)

func TestSSTable(t *testing.T) {
	memtable := NewMemTable(math.MaxUint32)
	vmap := map[string]string{}

	// insert
	for i := 0; i < 5000; i++ {
		ts := time.Now().UnixNano()
		k := strconv.Itoa(int(ts))
		v := strconv.Itoa(int(ts))

		vmap[k] = v
		memtable.Put([]byte(k), []byte(v))
	}

	// dump
	src := DumpTable(memtable.it)
	os.WriteFile("test.sst", src, 0644)

	// find
	for k, v := range vmap {
		res, err := FindTable([]byte(k), "test.sst")
		if err != nil {
			t.Fatal(err)
		}
		if string(res) != v {
			t.Fatal("not equal")
		}
	}

	// error
	for i := 0; i < 5000; i++ {
		ts := time.Now().UnixNano()
		k := strconv.Itoa(int(ts))
		res, err := FindTable([]byte(k), "test.sst")
		if err != nil {
			t.Fatal(err)
		}
		if string(res) != "" {
			t.Fatal("should be nil")
		}
	}
}
