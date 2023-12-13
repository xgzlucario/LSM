package memdb

import (
	"errors"
	"fmt"

	"github.com/andy-kimball/arenaskl"
	"github.com/sourcegraph/conc/pool"
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

// String
func (db *DB) String() string {
	return fmt.Sprintf("[memdb] cap:%v, min:%s, max:%s\n",
		db.cap, db.MinKey(), db.MaxKey())
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
func (db *DB) Iter(f func([]byte, []byte, uint16)) {
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
		cap += m.cap
	}
	db := New(cap)

	// merge memdbs sequentially.
	for _, m := range dbs {
		m.Iter(func(key, value []byte, vtype uint16) {
			if db.seek(key) {
				if err := db.it.Set(value, vtype); err != nil {
					panic(err)
				}
			} else {
				if err := db.Put(key, value, vtype); err != nil {
					panic(err)
				}
			}
		})
	}

	return db
}

// Split
func (db *DB) SplitFunc(eachNewDBSize uint32, cb func(*DB) error) error {
	pool := pool.New().WithErrors()
	newdb := New(eachNewDBSize)

	db.Iter(func(key, value []byte, meta uint16) {
		if full, err := db.PutIsFull(key, value, meta); full {
			// dump table.
			pool.Go(func() error { return cb(newdb) })

			// create new memdb.
			newdb = New(eachNewDBSize)
			if err := db.Put(key, value, meta); err != nil {
				panic(err)
			}

		} else if err != nil {
			panic(err)
		}
	})

	// dump last table.
	pool.Go(func() error { return cb(newdb) })

	return pool.Wait()
}
