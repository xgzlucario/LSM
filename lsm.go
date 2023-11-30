package lsm

import (
	"fmt"
	"os"
	"path"
	"sync"
	"time"

	"github.com/tidwall/wal"
)

// configuration for LSM-Tree.
var (
	MemTableSize  uint32 = 2 * MB
	DataBlockSize uint32 = 4 * KB

	Level0MaxTables = 4

	MinorCompactInterval = 10 * time.Second
	MajorCompactInterval = 10 * time.Minute
)

// LSM-Tree defination.
type LSM struct {
	sync.Mutex
	path string

	mt  *MemTable
	imt []*MemTable // immutable memtables.

	walIndex uint64
	wal      *wal.Log // wal log.
}

// NewLSM
func NewLSM(dir string) (*LSM, error) {
	log, err := wal.Open(path.Join(dir, "wal"), nil)
	if err != nil {
		return nil, err
	}

	lsm := &LSM{
		path: dir,
		mt:   NewMemTable(),
		wal:  log,
	}
	go func() {
		for {
			time.Sleep(MinorCompactInterval)
			lsm.MinorCompact()
		}
	}()
	go func() {
		for {
			time.Sleep(MajorCompactInterval)
			lsm.MajorCompact()
		}
	}()

	return lsm, nil
}

// Put
func (lsm *LSM) Put(key, value []byte) error {
	// write wal.
	lsm.walIndex++
	if err := lsm.wal.Write(lsm.walIndex, key); err != nil {
		return err
	}

	lsm.Lock()
	defer lsm.Unlock()

	// write memtable.
	if err := lsm.mt.Put(key, value); err != nil {
		return err
	}

	// if memtable is full, turn to immutable.
	if lsm.mt.Full() {
		lsm.imt = append(lsm.imt, lsm.mt)
		lsm.mt = NewMemTable()
	}

	return nil
}

// MinorCompact
func (lsm *LSM) MinorCompact() error {
	lsm.Lock()
	defer lsm.Unlock()

	// write current memtable.
	lsm.imt = append(lsm.imt, lsm.mt)
	lsm.mt = NewMemTable()

	for _, mt := range lsm.imt {
		name := fmt.Sprintf("%s/L0-%d.sst", lsm.path, time.Now().UnixNano())
		if err := os.WriteFile(name, DumpTable(mt), 0644); err != nil {
			return err
		}
	}
	lsm.imt = lsm.imt[:0]

	return nil
}

// MajorCompact
func (lsm *LSM) MajorCompact() error {
	lsm.Lock()
	defer lsm.Unlock()

	return nil
}
