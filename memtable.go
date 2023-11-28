package lsm

import (
	"github.com/andy-kimball/arenaskl"
	"github.com/tidwall/wal"
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

	walIndex uint64
	wal      *wal.Log // wal log.
}

// NewMemTable
func NewMemTable(arenaSize uint32) (*MemTable, error) {
	skl := arenaskl.NewSkiplist(arenaskl.NewArena(arenaSize))
	var it arenaskl.Iterator
	it.Init(skl)

	log, err := wal.Open("wallog", nil)
	if err != nil {
		return nil, err
	}

	return &MemTable{
		skl: skl,
		it:  &it,
		wal: log,
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

	// write wal
	mt.walIndex++
	if err := mt.wal.Write(mt.walIndex, key); err != nil {
		return err
	}

	// update skl
	if err := mt.it.Add(key, value, 0); err != nil {
		return err
	}

	if mt.skl.Size() >= 4*MB {
		mt.Rotate()
	}

	return nil
}

// MarshalBinary
func (mt *MemTable) MarshalBinary() ([]byte, error) {
	return nil, nil
}
