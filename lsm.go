package lsm

import (
	"bytes"
	"context"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path"
	"slices"
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

	MinorCompactInterval = time.Second
	MajorCompactInterval = time.Second
)

// LSM-Tree defination.
type LSM struct {
	sync.Mutex
	path string

	ctx    context.Context
	cancel context.CancelFunc

	mt *MemTable

	logger *slog.Logger
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
		logger: slog.Default(),
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
	name := fmt.Sprintf("L0-%d.sst", time.Now().UnixNano())

	lsm.logger.Info(fmt.Sprintf("[MinorCompact] save %s", name))

	if err := os.WriteFile(path.Join(lsm.path, name), DumpTable(mt.it), 0644); err != nil {
		panic(err)
	}
}

// MajorCompact
func (lsm *LSM) MajorCompact() error {
	lsm.Lock()
	defer lsm.Unlock()

	files, err := os.ReadDir(lsm.path)
	if err != nil {
		return err
	}

	// filter.
	files = slices.DeleteFunc(files, func(a fs.DirEntry) bool {
		return !strings.HasPrefix(a.Name(), "L0")
	})
	slices.SortFunc(files, func(a, b fs.DirEntry) int {
		return strings.Compare(a.Name(), b.Name())
	})

	// load tables.
	tables := make([]*SSTable, 0, len(files))
	for _, file := range files {
		sst, err := NewSSTable(path.Join(lsm.path, file.Name()))
		if err != nil {
			return err
		}
		defer sst.Close()

		if err := sst.decodeData(); err != nil {
			return err
		}
		tables = append(tables, sst)
	}

	// merge tables.
	t0 := tables[0]
	for _, table := range tables[1:] {
		t0.merge(table)
	}

	// dump.
	name := fmt.Sprintf("L1-%d.sst", time.Now().UnixNano())

	lsm.logger.Info(fmt.Sprintf("[MajorCompact] merge %s", name))

	if err := os.WriteFile(path.Join(lsm.path, name), DumpTable(t0.it), 0644); err != nil {
		panic(err)
	}

	// remove old tables.
	for _, file := range files {
		if err := os.Remove(path.Join(lsm.path, file.Name())); err != nil {
			return err
		}
	}

	return nil
}

// FindTable
func FindTable(key []byte, path string) ([]byte, error) {
	sst, err := NewSSTable(path)
	if err != nil {
		return nil, err
	}
	defer sst.Close()

	return sst.findKey(key)
}

// IsOverlap
func (t *SSTable) IsOverlap(n *SSTable) bool {
	if bytes.Compare(t.indexBlock.FirstKey, n.indexBlock.FirstKey) <= 0 &&
		bytes.Compare(n.indexBlock.FirstKey, t.indexBlock.LastKey) <= 0 {
		return true
	}

	return bytes.Compare(n.indexBlock.FirstKey, t.indexBlock.FirstKey) <= 0 &&
		bytes.Compare(t.indexBlock.FirstKey, n.indexBlock.LastKey) <= 0
}
