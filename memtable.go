package lsm

import (
	"github.com/andy-kimball/arenaskl"
)

const (
	KB = 1 << 10
	MB = 1 << 20
	GB = 1 << 30
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
	return m.it.Add(key, value, vtype)
}

// Put insert key-value pair to the memable.
func (m *MemTable) Put(key, value []byte) error {
	return m.it.Add(key, value, typeVal)
}

// Delete insert a tombstone to the memable.
func (m *MemTable) Delete(key []byte) error {
	return m.it.Add(key, nil, typeDel)
}

// Full
func (m *MemTable) Full() bool {
	return m.skl.Arena().Size() >= uint32(0.9*float64(m.skl.Arena().Cap()))
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

// Merge merge m2 to m, m2 is the newest table.
func (m *MemTable) Merge(m2 *MemTable) {
	m3 := NewMemTable(m.skl.Arena().Cap() + m2.skl.Arena().Cap())

	m.Iter(func(key, value []byte, vtype uint16) {
		if err := m3.PutRaw(key, value, vtype); err != nil {
			panic(err)
		}
	})

	m2.Iter(func(key, value []byte, vtype uint16) {
		m3.it.Seek(key)
		if m3.it.Valid() {
			return
		}
		if err := m3.PutRaw(key, value, vtype); err != nil {
			panic(err)
		}
	})

	*m = *m3
}
