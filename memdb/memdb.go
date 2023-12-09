package memdb

import (
	"errors"

	"github.com/andy-kimball/arenaskl"
)

const (
	// key-value pair type.
	typeVal uint16 = 1
	typeDel uint16 = 2
)

var (
	ErrImmutable = errors.New("memdb: attempt to change an immutable db")
)

// DB is the memory db of LSM-Tree.
type DB struct {
	cap uint32
	skl *arenaskl.Skiplist
	it  *arenaskl.Iterator
}

// New
func New(cap uint32) *DB {
	skl := arenaskl.NewSkiplist(arenaskl.NewArena(cap))
	var it arenaskl.Iterator
	it.Init(skl)

	return &DB{cap: cap, skl: skl, it: &it}
}

// Get
func (db *DB) Get(key []byte) ([]byte, bool) {
	if db.seek(key) {
		return db.it.Value(), true
	}
	return nil, false
}

// Get
func (db *DB) Capacity() uint32 {
	return db.cap
}

// Put
func (db *DB) Put(key, value []byte, vtype uint16) error {
	return db.it.Add(key, value, vtype)
}

// PutFull
func (db *DB) PutIsFull(key, value []byte, vtype uint16) (bool, error) {
	err := db.Put(key, value, vtype)
	return errors.Is(err, arenaskl.ErrArenaFull), err
}

// FirstKey
func (db *DB) FirstKey() []byte {
	db.it.SeekToFirst()
	return db.it.Key()
}

// LastKey
func (db *DB) LastKey() []byte {
	db.it.SeekToLast()
	return db.it.Key()
}

// Iter
func (db *DB) Iter(f func([]byte, []byte, uint16)) {
	if db == nil {
		panic("memdb/Iter: nil db")
	}
	for db.it.SeekToFirst(); db.it.Valid(); db.it.Next() {
		f(db.it.Key(), db.it.Value(), db.it.Meta())
	}
}

// seek
func (db *DB) seek(key []byte) bool {
	db.it.Seek(key)
	return db.it.Valid()
}

// Merge
func (db *DB) Merge(dbs ...*DB) {
	cap := db.cap
	for _, m := range dbs {
		cap += m.cap
	}
	newdb := New(cap)

	db.Iter(func(key, value []byte, vtype uint16) {
		if err := newdb.Put(key, value, vtype); err != nil {
			panic(err)
		}
	})

	for _, m := range dbs {
		m.Iter(func(key, value []byte, vtype uint16) {
			if newdb.seek(key) {
				if err := newdb.it.Set(value, vtype); err != nil {
					panic(err)
				}
			} else {
				if err := newdb.Put(key, value, vtype); err != nil {
					panic(err)
				}
			}
		})
	}

	*db = *newdb
}
