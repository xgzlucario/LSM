package memdb

import (
	"errors"
	"fmt"

	"github.com/andy-kimball/arenaskl"
)

const (
	// key-value pair type.
	typeVal uint16 = 1
	typeDel uint16 = 2
)

// DB is the memory db of LSM-Tree.
type DB struct {
	arena *arenaskl.Arena
	skl   *arenaskl.Skiplist
	it    *arenaskl.Iterator
}

// New
func New(cap uint32) *DB {
	arena := arenaskl.NewArena(cap)
	skl := arenaskl.NewSkiplist(arena)
	var it arenaskl.Iterator
	it.Init(skl)

	return &DB{arena: arena, skl: skl, it: &it}
}

// New2 returns a db with a bit of redundant space.
// this is to prevent the areana from filling up and causing errors.
// MAKE SURE you really need to call this function!
func New2(cap uint32) *DB {
	return New(uint32(float64(cap) * 1.05))
}

// String
func (db *DB) String() string {
	return fmt.Sprintf("[memdb] len:%v, cap:%v, min:%s, max:%s\n",
		db.Len(), db.Capacity(), db.MinKey(), db.MaxKey())
}

// Reset
func (db *DB) Reset() {
	db.arena.Reset()
	db.skl = arenaskl.NewSkiplist(db.arena)
}

// Get
func (db *DB) Get(key []byte) ([]byte, bool) {
	if db.seek(key) {
		return db.it.Value(), true
	}
	return nil, false
}

// Len
func (db *DB) Len() int {
	var count int
	for db.it.SeekToFirst(); db.it.Valid(); db.it.Next() {
		count++
	}
	return count
}

// Capacity
func (db *DB) Capacity() uint32 {
	return db.arena.Cap()
}

// put
func (db *DB) put(key, value []byte, meta uint16) error {
	if db.seek(key) {
		return db.it.Set(value, meta)
	}
	return db.it.Add(key, value, meta)
}

// Put return true if memdb is full.
func (db *DB) Put(key, value []byte, meta uint16) bool {
	err := db.put(key, value, meta)
	if err == nil {
		return false
	}
	if errors.Is(err, arenaskl.ErrArenaFull) {
		return true
	}
	panic("bug: put memdb error")
}

// MinKey
func (db *DB) MinKey() []byte {
	db.it.SeekToFirst()
	return db.it.Key()
}

// MaxKey
func (db *DB) MaxKey() []byte {
	db.it.SeekToLast()
	return db.it.Key()
}

// Iter
func (db *DB) Iter(f func(key, value []byte, meta uint16)) {
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
func Merge(dbs ...*DB) *DB {
	var cap uint32
	for _, m := range dbs {
		cap += m.Capacity()
	}
	db := New2(cap)

	// merge memdbs sequentially.
	for _, m := range dbs {
		m.Iter(func(key, value []byte, meta uint16) {
			if db.Put(key, value, meta) {
				panic("bug: merge memdb error")
			}
		})
	}

	return db
}

// SplitFunc
func (db *DB) SplitFunc(eachBlockSize uint32, cb func(*DB) error) error {
	newdb := New(eachBlockSize)

	db.Iter(func(key, value []byte, meta uint16) {
		if newdb.Put(key, value, meta) {
			if err := cb(newdb); err != nil {
				panic(err)
			}

			newdb.Reset()
			if newdb.Put(key, value, meta) {
				panic("bug: put memdb error")
			}
		}
	})

	// dump last table.
	return cb(newdb)
}
