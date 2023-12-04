package lsm

import (
	"math"

	"github.com/andy-kimball/arenaskl"
)

const (
	KB = 1 << 10
	MB = 1 << 20
	GB = 1 << 30

	maxNodeSize = math.MaxUint16
)

// Memable
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

// Get
func (m *MemTable) Get(key []byte) ([]byte, error) {
	m.it.Seek(key)
	if m.it.Valid() && m.it.Meta() == typeVal {
		return m.it.Value(), nil
	}

	return nil, ErrKeyNotFound
}

// PutRaw
func (m *MemTable) PutRaw(key, value []byte, vtype uint16) error {
	if len(key) > maxNodeSize || len(value) > maxNodeSize {
		return ErrInputToLarge
	}
	return m.it.Add(key, value, vtype)
}

// Put insert key-value pair to the memable.
func (m *MemTable) Put(key, value []byte) error {
	return m.PutRaw(key, value, typeVal)
}

// Delete insert a tombstone to the memable.
func (m *MemTable) Delete(key []byte) error {
	return m.PutRaw(key, nil, typeDel)
}

// Full
func (m *MemTable) Full() bool {
	remain := m.skl.Arena().Cap() - m.skl.Arena().Size()
	return remain < maxNodeSize*2
}

// FirstKey
func (m *MemTable) FirstKey() []byte {
	m.it.SeekToFirst()
	return m.it.Key()
}

// LastKey
func (m *MemTable) LastKey() []byte {
	m.it.SeekToLast()
	return m.it.Key()
}

// Iter
func (m *MemTable) Iter(f func([]byte, []byte, uint16)) {
	for m.it.SeekToFirst(); m.it.Valid(); m.it.Next() {
		f(m.it.Key(), m.it.Value(), m.it.Meta())
	}
}

// Exist
func (m *MemTable) Exist(key []byte) bool {
	m.it.Seek(key)
	return m.it.Valid()
}

// Merge merge tables to m.
func (m *MemTable) Merge(tables ...*MemTable) {
	size := m.skl.Arena().Cap()
	for _, t := range tables {
		size += t.skl.Arena().Cap()
	}

	newm := NewMemTable(size)

	m.Iter(func(key, value []byte, vtype uint16) {
		if err := newm.PutRaw(key, value, vtype); err != nil {
			panic(err)
		}
	})

	for _, t := range tables {
		t.Iter(func(key, value []byte, vtype uint16) {
			if !newm.Exist(key) {
				if err := newm.PutRaw(key, value, vtype); err != nil {
					panic(err)
				}
			}
		})
	}

	*m = *newm
}
