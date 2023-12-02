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
	"strconv"
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
	MajorCompactInterval = 5 * time.Second
)

// LSM-Tree defination.
type LSM struct {
	sync.Mutex
	path string

	ctx    context.Context
	cancel context.CancelFunc

	mt  *MemTable
	imt []*MemTable

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
			case <-time.After(MinorCompactInterval):
				lsm.MinorCompact()

			case <-lsm.ctx.Done():
				return
			}
		}
	}()

	// start major compaction.
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

	lsm.logger.Info("LSM-Tree started.")

	return lsm, nil
}

// Put
func (lsm *LSM) Put(key, value []byte) error {
	if err := lsm.mt.Put(key, value); err != nil {
		return err
	}

	// if memtable is full, rotate to immutable.
	if lsm.mt.Full() {
		m := NewMemTable()
		lsm.Lock()
		lsm.imt = append(lsm.imt, lsm.mt)
		lsm.mt = m
		lsm.Unlock()
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

// dumpTable
func (lsm *LSM) dumpTable(level int, m *MemTable) error {
	tableName := fmt.Sprintf("L%d-%s.sst", level, strconv.FormatInt(time.Now().UnixNano(), 36))
	// log
	lsm.logger.Info(fmt.Sprintf("[MinorCompact] save %s", tableName))

	return os.WriteFile(path.Join(lsm.path, tableName), EncodeTable(m), 0644)
}

// MinorCompact
func (lsm *LSM) MinorCompact() {
	lsm.Lock()
	defer lsm.Unlock()

	for _, m := range lsm.imt {
		if err := lsm.dumpTable(0, m); err != nil {
			panic(err)
		}
	}
	lsm.imt = lsm.imt[:0]
}

// loadLevelTables
func (lsm *LSM) loadLevelTables(level int) ([]*SSTable, error) {
	files, err := os.ReadDir(lsm.path)
	if err != nil {
		return nil, err
	}

	// filter.
	prefix := fmt.Sprintf("L%d", level)
	files = slices.DeleteFunc(files, func(fs fs.DirEntry) bool {
		return !strings.HasPrefix(fs.Name(), prefix)
	})
	slices.SortFunc(files, func(f1, f2 fs.DirEntry) int {
		return strings.Compare(f1.Name(), f2.Name())
	})

	// load tables.
	tables := make([]*SSTable, 0, len(files))
	for _, file := range files {
		sst, err := NewSSTable(path.Join(lsm.path, file.Name()))
		if err != nil {
			return nil, err
		}
		if err := sst.decodeIndex(); err != nil {
			return nil, err
		}
		tables = append(tables, sst)
	}

	return tables, nil
}

// MajorCompact
func (lsm *LSM) MajorCompact() error {
	// tables for current level.
	tables, err := lsm.loadLevelTables(0)
	if err != nil {
		return err
	}
	if len(tables) == 0 {
		return nil
	}

	// for level0, merge all tables.
	t0 := tables[0]
	for _, table := range tables[1:] {
		t0.merge(table)
	}

	lsm.Lock()
	defer lsm.Unlock()

	// dump table.
	if err := lsm.dumpTable(1, t0.m); err != nil {
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
