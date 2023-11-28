package lsm

import "github.com/tidwall/wal"

// LSM (Log Structured Merge Tree) defination.
type LSM struct {
	path string

	mt  *MemTable
	imt []*MemTable // immutable memtables.

	walIndex uint64
	wal      *wal.Log // wal log.
}

// NewLSM
func NewLSM(path string) (*LSM, error) {
	mt, err := NewMemTable(4 * MB)
	if err != nil {
		return nil, err
	}

	log, err := wal.Open(path, nil)
	if err != nil {
		return nil, err
	}

	return &LSM{
		path: path,
		mt:   mt,
		wal:  log,
	}, nil
}

// Put
func (lsm *LSM) Put(key, value []byte) error {
	// write wal
	lsm.walIndex++
	if err := lsm.wal.Write(lsm.walIndex, key); err != nil {
		return err
	}

	// write memtable
	if err := lsm.mt.Put(key, value); err != nil {
		return err
	}

	// if memtable is full, rotate it.
	if lsm.mt.Full() {
		lsm.mt.Rotate()
		lsm.imt = append(lsm.imt, lsm.mt)

		mt, err := NewMemTable(4 * MB)
		if err != nil {
			return err
		}
		lsm.mt = mt
	}

	return nil
}
