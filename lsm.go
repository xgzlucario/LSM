package lsm

import "github.com/tidwall/wal"

var (
	DefaultConfig = &Config{
		Path:          "data",
		MemTableSize:  2 * MB,
		DataBlockSize: 4 * KB,
	}
)

// Config for LSM-Tree.
type Config struct {
	Path string

	// memtable size.
	MemTableSize uint32

	// data block size.
	DataBlockSize uint32
}

// LSM-Tree defination.
type LSM struct {
	*Config

	mt  *MemTable
	imt []*MemTable // immutable memtables.

	walIndex uint64
	wal      *wal.Log // wal log.
}

// NewLSM
func NewLSM(cfg *Config) (*LSM, error) {
	log, err := wal.Open(cfg.Path, nil)
	if err != nil {
		return nil, err
	}

	return &LSM{
		Config: cfg,
		mt:     NewMemTable(cfg.MemTableSize),
		wal:    log,
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
	if err := lsm.mt.Put(key, value, vtypeVal); err != nil {
		return err
	}

	// if memtable is full, rotate it.
	if lsm.mt.Full() {
		lsm.mt.Rotate()
		lsm.imt = append(lsm.imt, lsm.mt)
		lsm.mt = NewMemTable(lsm.MemTableSize)
	}

	return nil
}
