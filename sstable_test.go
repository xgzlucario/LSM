package lsm

import (
	"strconv"
	"testing"
)

func TestSSTable(t *testing.T) {
	cfg := DefaultConfig

	memtable := NewMemTable(cfg.MemTableSize)
	for i := 0; i < 100; i++ {
		k := []byte("key" + strconv.Itoa(i))
		memtable.Put(k, k, vtypeVal)
	}

	sstable := &SSTable{
		Config:   cfg,
		MemTable: memtable,
	}

	t.Error(sstable.DumpTable())
}
