package lsm

import (
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/tidwall/wal"
)

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

	mu  sync.Mutex // mutex for memtables.
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
	// write wal.
	lsm.walIndex++
	if err := lsm.wal.Write(lsm.walIndex, key); err != nil {
		return err
	}

	// write memtable.
	if err := lsm.mt.Put(key, value); err != nil {
		return err
	}

	// if memtable is full, rotate it.
	if lsm.mt.Full() {
		lsm.mu.Lock()
		lsm.mt.Rotate()
		lsm.imt = append(lsm.imt, lsm.mt)
		lsm.mt = NewMemTable(lsm.MemTableSize)
		lsm.mu.Unlock()
	}

	return nil
}

// MinorCompact
func (lsm *LSM) MinorCompact() error {
	lsm.mu.Lock()
	defer lsm.mu.Unlock()

	for _, mt := range lsm.imt {
		src := DumpTable(mt, lsm.Config)

		if err := os.WriteFile(fmt.Sprintf("L0-%d.sst", time.Now().UnixNano()), src, 0644); err != nil {
			return err
		}
	}

	return nil
}

// MajorCompact
func (lsm *LSM) MajorCompact() error {
	lsm.mu.Lock()
	defer lsm.mu.Unlock()

	return nil
}
