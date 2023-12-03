package lsm

import (
	"context"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path"
	"path/filepath"
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
	path string

	ref *RefCounter
	mu  sync.Mutex

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
		ref:    NewRefCounter(),
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
		lsm.mu.Lock()
		lsm.imt = append(lsm.imt, lsm.mt)
		lsm.mt = m
		lsm.mu.Unlock()
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
	lsm.mu.Lock()
	defer lsm.mu.Unlock()

	for _, m := range lsm.imt {
		if err := lsm.dumpTable(0, m); err != nil {
			panic(err)
		}
	}
	lsm.imt = lsm.imt[:0]
}

// loadTables
func (lsm *LSM) loadTables(level int) ([]*SSTable, error) {
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
		// add ref.
		lsm.ref.AddRef(file.Name())

		sst, err := NewSSTable(path.Join(lsm.path, file.Name()))
		if err != nil {
			return nil, err
		}
		tables = append(tables, sst)
	}

	return tables, nil
}

// MajorCompact
func (lsm *LSM) MajorCompact() error {
	if err := lsm.CompactLevel0(); err != nil {
		return err
	}
	for level := 1; ; level++ {
		if n, err := lsm.CompactLevelN(level); n == 0 || err != nil {
			return err
		}
	}
}

// CompactLevel0
func (lsm *LSM) CompactLevel0() error {
	// tables for current level.
	tables, err := lsm.loadTables(0)
	if err != nil {
		return err
	}
	if len(tables) == 0 {
		return nil
	}

	// for level0, merge all tables.
	t0 := tables[0]
	for _, table := range tables[1:] {
		t0.Merge(table)
	}

	// dump table.
	if err := lsm.dumpTable(1, t0.m); err != nil {
		panic(err)
	}

	// remove old tables.
	for _, table := range tables {
		name := filepath.Base(table.fd.Name())
		lsm.ref.DelRef(name, func() {
			if err := os.Remove(table.fd.Name()); err != nil {
				panic(err)
			}
		})
	}

	return nil
}

// CompactLevelN
func (lsm *LSM) CompactLevelN(level int) (mergedNum int, err error) {
	// tables for current level.
	tables, err := lsm.loadTables(level)
	if err != nil {
		return 0, err
	}
	if len(tables) <= 1 {
		return 0, nil
	}

	for i := range tables {
		t1 := tables[i]
		if t1 == nil {
			return
		}

		merged := false

		for j := i + 1; j < len(tables); j++ {
			t2 := tables[j]
			if t2 == nil {
				return
			}

			if t1.IsOverlap(t2) {
				t1.Merge(t2)

				// if table is merged, remove it.
				tables[j] = nil
				name := filepath.Base(t2.fd.Name())
				lsm.ref.DelRef(name, func() {
					if err := os.Remove(t2.fd.Name()); err != nil {
						panic(err)
					}
				})
				merged = true
				mergedNum++
			}
		}

		if merged {
			// dump table.
			if err := lsm.dumpTable(level+1, t1.m); err != nil {
				panic(err)
			}

			// remmove old table.
			name := filepath.Base(t1.fd.Name())
			lsm.ref.DelRef(name, func() {
				if err := os.Remove(t1.fd.Name()); err != nil {
					panic(err)
				}
			})
		}
	}

	return
}
