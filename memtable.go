package lsm

import (
	"github.com/andy-kimball/arenaskl"
	"github.com/xgzlucario/LSM/codeman"
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
func NewMemTable(arenaSize uint32) (*MemTable, error) {
	skl := arenaskl.NewSkiplist(arenaskl.NewArena(arenaSize))
	var it arenaskl.Iterator
	it.Init(skl)

	return &MemTable{
		skl: skl,
		it:  &it,
	}, nil
}

// Rotate a memtable to immutable state when it is full.
func (mt *MemTable) Rotate() bool {
	if mt.immu {
		return false
	}
	mt.immu = true
	return true
}

// Put a key-value pair to the memtable.
func (mt *MemTable) Put(key, value []byte) error {
	if mt.immu {
		panic("immutable")
	}
	return mt.it.Add(key, value, 0)
}

// Full
func (mt *MemTable) Full() bool {
	return mt.skl.Size() >= 4*MB
}

// MarshalBinary
func (mt *MemTable) MarshalBinary() ([]byte, error) {
	if !mt.immu {
		panic("only memtable can be flush")
	}

	mt.it.SeekToFirst()
	minBytes, _ := mt.it.Key(), mt.it.Value()

	mt.it.SeekToLast()
	maxBytes, _ := mt.it.Key(), mt.it.Value()

	codeman.NewCodec().Bytes(minBytes).Bytes(maxBytes)

	return nil, nil
}
