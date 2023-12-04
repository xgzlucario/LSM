package lsm

import (
	"context"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
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
	index uint64
	path  string

	// RefCounter have two parts:
	// 1. Storage System (indicating what is valid data)
	// 2. Query (referenced when querying or compaction)
	// sstable is removed from the file system when the ref count is 0.
	ref *RefCounter

	ctx    context.Context
	cancel context.CancelFunc

	mu sync.RWMutex
	m  *MemTable
	im []*MemTable

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
		m:      NewMemTable(),
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
				lsm.MajorCompact()

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
	// if memtable is full, rotate to immutable.
	if lsm.m.Full() {
		m := NewMemTable()
		lsm.mu.Lock()
		lsm.im = append(lsm.im, lsm.m)
		lsm.m = m
		lsm.mu.Unlock()
	}
	return lsm.m.Put(key, value)
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
	name := fmt.Sprintf("L%d-%06d_%s_%s.sst",
		level,
		atomic.AddUint64(&lsm.index, 1),
		m.FirstKey(),
		m.LastKey(),
	)

	// add storage ref count.
	lsm.ref.Incr(1, name)

	lsm.log("[MinorCompact] save %s", name)

	return os.WriteFile(path.Join(lsm.path, name), EncodeTable(m), 0644)
}

// MinorCompact
func (lsm *LSM) MinorCompact() {
	lsm.mu.Lock()
	defer lsm.mu.Unlock()

	for _, m := range lsm.im {
		if err := lsm.dumpTable(0, m); err != nil {
			panic(err)
		}
	}
	lsm.im = lsm.im[:0]
}

// loadTables
func (lsm *LSM) loadTables(level int) ([]*SSTable, []string, error) {
	tables := make([]*SSTable, 0, 8)
	names := make([]string, 0, 8)

	prefix := fmt.Sprintf("L%d", level)

	filepath.WalkDir(lsm.path, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			panic(err)
		}

		name := entry.Name()
		if !strings.HasPrefix(name, prefix) {
			return nil
		}

		// add query ref count.
		lsm.ref.Incr(1, name)

		sst, err := NewSSTable(path)
		if err != nil {
			panic(err)
		}
		tables = append(tables, sst)
		names = append(names, name)

		return nil
	})

	return tables, names, nil
}

// MajorCompact
func (lsm *LSM) MajorCompact() {
	if err := lsm.compactLevel0(); err != nil {
		panic(err)
	}

	for level := 1; ; level++ {
		lsm.ref.DelZero(func(s string) {
			if err := os.Remove(path.Join(lsm.path, s)); err != nil {
				panic(err)
			}
		})

		if n, err := lsm.compactLevelN(level); err != nil {
			panic(err)

		} else if n == 0 {
			return
		}
	}
}

// compactLevel0
func (lsm *LSM) compactLevel0() error {
	tables, names, err := lsm.loadTables(0)
	if err != nil {
		return err
	}
	// delete query ref count.
	defer lsm.ref.Incr(-1, names...)

	if len(tables) < Level0MaxTables {
		return nil
	}

	// merge all level 0 tables to level 1.
	t := tables[0]
	t.Merge(tables[1:]...)

	if err := lsm.dumpTable(1, t.m); err != nil {
		panic(err)
	}

	// delete storage ref count.
	lsm.ref.Incr(-1, names...)

	return nil
}

// compactLevelN
func (lsm *LSM) compactLevelN(level int) (mergedNum int, err error) {
	tables, names, err := lsm.loadTables(level)
	if err != nil {
		return 0, err
	}
	// delete query ref count.
	defer lsm.ref.Incr(-1, names...)

	if len(tables) <= 1 {
		return 0, nil
	}

	for i, t1 := range tables {
		if t1 == nil {
			continue
		}
		mergedNames := make([]string, 0)

		for j := i + 1; j < len(tables); j++ {
			t2 := tables[j]
			if t2 == nil {
				continue
			}

			if t1.IsOverlap(t2) {
				t1.Merge(t2)

				// if table is merged, remove it.
				tables[j] = nil
				mergedNum++
				mergedNames = append(mergedNames, filepath.Base(t2.fd.Name()))
			}
		}

		if len(mergedNames) > 0 {
			mergedNames = append(mergedNames, filepath.Base(t1.fd.Name()))

			// dump table.
			if err := lsm.dumpTable(level+1, t1.m); err != nil {
				panic(err)
			}

			// remmove old table.
			lsm.ref.Incr(-1, mergedNames...)
		}
	}
	return
}

func (lsm *LSM) log(msg string, args ...any) {
	lsm.logger.Info(fmt.Sprintf(msg, args...))
}
