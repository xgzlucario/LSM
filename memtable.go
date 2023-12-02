package lsm

import "github.com/andy-kimball/arenaskl"

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

// Get
func (mt *MemTable) Get(key []byte) ([]byte, error) {
	mt.it.Seek(key)

	if !mt.it.Valid() || mt.it.Meta() == vtypeDel {
		return nil, ErrKeyNotFound
	}

	return mt.it.Value(), nil
}

// PutRaw
func (mt *MemTable) PutRaw(key, value []byte, vtype uint16) error {
	return mt.it.Add(key, value, vtype)
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

// FirstKey
func (mt *MemTable) FirstKey() []byte {
	mt.it.SeekToFirst()
	return mt.it.Key()
}

// LastKey
func (mt *MemTable) LastKey() []byte {
	mt.it.SeekToLast()
	return mt.it.Key()
}

// Iter
func (mt *MemTable) Iter(f func([]byte, []byte, uint16)) {
	for mt.it.SeekToFirst(); mt.it.Valid(); mt.it.Next() {
		f(mt.it.Key(), mt.it.Value(), mt.it.Meta())
	}
}

// Merge
func (m *MemTable) Merge(m2 *MemTable) {
	m.Iter(func(key []byte, value []byte, meta uint16) {
		m.it.Add(key, value, meta)
	})
}
