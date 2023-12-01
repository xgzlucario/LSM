package lsm

import (
	"github.com/andy-kimball/arenaskl"
)

const (
	KB = 1 << 10
	MB = 1 << 20
	GB = 1 << 30
)

// MemTable
type MemTable struct {
	skl *arenaskl.Skiplist
	it  *arenaskl.Iterator
}

// NewMemTable
func NewMemTable(sizes ...uint32) *MemTable {
	size := MemTableSize
	if len(sizes) > 0 {
		size = sizes[0]
	}

	skl := arenaskl.NewSkiplist(arenaskl.NewArena(size))
	var it arenaskl.Iterator
	it.Init(skl)

	return &MemTable{
		skl: skl,
		it:  &it,
	}
}

// Put a key-value pair to the memtable.
func (mt *MemTable) Put(key, value []byte) error {
	return mt.it.Add(key, value, vtypeVal)
}

// Delete a key-value pair from the memtable.
func (mt *MemTable) Delete(key []byte) error {
	return mt.it.Add(key, nil, vtypeDel)
}

// Full
func (mt *MemTable) Full() bool {
	return mt.skl.Arena().Size() >= uint32(0.9*float64(mt.skl.Arena().Cap()))
}
