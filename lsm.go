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
	MajorCompactInterval = 10 * time.Second
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

// DumpTable
func (lsm *LSM) DumpTable(level int, m *MemTable) error {
	tableName := fmt.Sprintf("L%d-%d.sst", level, time.Now().UnixNano())
	// log
	lsm.logger.Info(fmt.Sprintf("[MinorCompact] save %s", tableName))

	return os.WriteFile(path.Join(lsm.path, tableName), EncodeTable(m), 0644)
}

// MinorCompact
func (lsm *LSM) MinorCompact(m *MemTable) {
	if m.skl.Size() == 0 {
		return
	}
	if err := lsm.DumpTable(0, m); err != nil {
		panic(err)
	}
}

// loadLevelTables
func (lsm *LSM) loadLevelTables(level int) ([]*SSTable, error) {
	files, err := os.ReadDir(lsm.path)
	if err != nil {
		return nil, err
	}

	// filter.
	prefix := fmt.Sprintf("L%d", level)
	files = slices.DeleteFunc(files, func(a fs.DirEntry) bool {
		return !strings.HasPrefix(a.Name(), prefix)
	})
	slices.SortFunc(files, func(a, b fs.DirEntry) int {
		return strings.Compare(a.Name(), b.Name())
	})

	// load tables.
	tables := make([]*SSTable, 0, len(files))
	for _, file := range files {
		sst, err := NewSSTable(path.Join(lsm.path, file.Name()))
		if err != nil {
			return nil, err
		}
		defer sst.Close()

		if err := sst.decodeData(); err != nil {
			return nil, err
		}
		tables = append(tables, sst)
	}

	return tables, nil
}

// MajorCompact
func (lsm *LSM) MajorCompact() error {
	tables, err := lsm.loadLevelTables(0)
	if err != nil {
		return err
	}
	if len(tables) == 0 {
		return nil
	}

	// merge tables.
	t0 := tables[0]
	for _, table := range tables[1:] {
		t0.merge(table)
	}

	lsm.Lock()
	defer lsm.Unlock()

	// dump table.
	if err := lsm.DumpTable(1, t0.m); err != nil {
		panic(err)
	}

	// remove old tables.
	for _, table := range tables {
		if err := os.Remove(table.fd.Name()); err != nil {
			return err
		}
	}

	return nil
}

// FindTable
func FindTable(key []byte, path string) ([]byte, error) {
	table, err := NewSSTable(path)
	if err != nil {
		return nil, err
	}
	defer table.Close()

	return table.findKey(key)
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
