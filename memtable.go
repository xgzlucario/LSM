package lsm

import "github.com/andy-kimball/arenaskl"

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

// Merge merge m2 to m, m2 is the newest table.
func (m *MemTable) Merge(m2 *MemTable) {
	for m2.it.SeekToFirst(); m2.it.Valid(); m2.it.Next() {
		m.it.Add(m2.it.Key(), m2.it.Value(), m2.it.Meta())
	}
}
