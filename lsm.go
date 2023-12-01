package lsm

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"log"
)

// configuration for LSM-Tree.
var (
	MemTableSize  uint32 = 4 * MB
	DataBlockSize uint32 = 4 * KB

	Level0MaxTables = 8

	MinorCompactInterval = 10 * time.Second
	MajorCompactInterval = 10 * time.Minute
)

// LSM-Tree defination.
type LSM struct {
	sync.Mutex
	path string

	ctx    context.Context
	cancel context.CancelFunc

	mt *MemTable
}

// NewLSM
func NewLSM(dir string) (*LSM, error) {
	os.MkdirAll(dir, 0755)

	ctx, cancel := context.WithCancel(context.Background())
	lsm := &LSM{
		path:   dir,
		ctx:    ctx,
		cancel: cancel,
		mt:     NewMemTable(),
	}

	// start minor compaction.
	go func() {
		for {
			select {
			case <-time.After(MajorCompactInterval):
				if err := lsm.MajorCompact(); err != nil {
					log.Fatal(err)
				}

			case <-lsm.ctx.Done():
				return
			}
		}
	}()

	return lsm, nil
}

// Put
func (lsm *LSM) Put(key, value []byte) error {
	if err := lsm.mt.Put(key, value); err != nil {
		return err
	}

	// if memtable is full, write to disk.
	if lsm.mt.Full() {
		lsm.Lock()
		oldmt := lsm.mt
		lsm.mt = NewMemTable()
		lsm.Unlock()

		go lsm.MinorCompact(oldmt)
	}

	return nil
}

// Get
func (lsm *LSM) Get(key []byte) ([]byte, error) {
	return nil, nil
}

// Close
func (lsm *LSM) Close() error {
	select {
	case <-lsm.ctx.Done():
		return nil
	default:
		lsm.cancel()
	}
	return nil
}

// MinorCompact
func (lsm *LSM) MinorCompact(mt *MemTable) {
	name := fmt.Sprintf("%s/L0-%d.sst", lsm.path, time.Now().UnixNano())
	if err := os.WriteFile(name, DumpTable(mt), 0644); err != nil {
		panic(err)
	}
}

// MajorCompact
func (lsm *LSM) MajorCompact() error {
	lsm.Lock()
	defer lsm.Unlock()

	// find all tables in level0.
	tables := make([]*MetaTable, 0, Level0MaxTables)

	files, err := os.ReadDir(lsm.path)
	if err != nil {
		return err
	}
	for _, file := range files {
		if strings.HasPrefix(file.Name(), "L0") {
			tables = append(tables, &MetaTable{
				Level: 0,
				Name:  file.Name(),
			})
		}
	}

	// TODO: add firstKey lastKey in indexBlock

	return nil
}
