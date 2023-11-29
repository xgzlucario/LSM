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
	immu bool

	skl *arenaskl.Skiplist // based on arena skiplist.
	it  *arenaskl.Iterator
}

// NewMemTable
func NewMemTable(size uint32) *MemTable {
	skl := arenaskl.NewSkiplist(arenaskl.NewArena(size))
	var it arenaskl.Iterator
	it.Init(skl)

	return &MemTable{
		skl: skl,
		it:  &it,
	}
}

// Rotate a memtable to immutable state when it is full.
func (mt *MemTable) Rotate() {
	if mt.immu {
		panic("immutable")
	}
	mt.immu = true
}

// Put a key-value pair to the memtable.
func (mt *MemTable) Put(key, value []byte, vtype vtype) error {
	if mt.immu {
		panic("immutable")
	}
	return mt.it.Add(key, value, uint16(vtype))
}

// Full
func (mt *MemTable) Full() bool {
	return mt.skl.Size() >= 4*MB
}
