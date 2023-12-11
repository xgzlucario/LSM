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
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sourcegraph/conc/pool"
	"github.com/xgzlucario/LSM/memdb"
	"github.com/xgzlucario/LSM/option"
	"github.com/xgzlucario/LSM/refmap"
	"github.com/xgzlucario/LSM/table"
)

// LSM-Tree defination.
type LSM struct {
	*option.Option

	seq uint64
	dir string

	// RefMap have two parts:
	// 1. Storage System (indicating what is valid data)
	// 2. Query (referenced when querying or compaction)
	// sstable is removed from the file system when the ref count is 0.
	ref *refmap.Map

	ctx    context.Context
	cancel context.CancelFunc

	// guards db and dbList.
	mu     sync.RWMutex
	db     *memdb.DB
	dbList []*memdb.DB

	// index of levels.
	index *LevelController

	tableWriter *table.Writer

	compactC chan struct{}

	logger *slog.Logger
}

// NewLSM
func NewLSM(dir string, opt *option.Option) (*LSM, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	lsm := &LSM{
		Option:      opt,
		dir:         dir,
		ref:         refmap.New(),
		ctx:         ctx,
		cancel:      cancel,
		db:          memdb.New(opt.MemDBSize),
		dbList:      make([]*memdb.DB, 0, 16),
		index:       NewLevelController(dir, opt),
		tableWriter: table.NewWriter(opt),
		compactC:    make(chan struct{}, 1),
		logger:      slog.Default(),
	}

	// build index.
	if err := lsm.index.buildFromDisk(); err != nil {
		panic(err)
	}

	// start minor compaction.
	go func() {
		for {
			select {
			case <-time.After(lsm.MinorCompactInterval):
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
			case <-time.After(lsm.MajorCompactInterval):
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
	full, err := lsm.db.PutIsFull(key, value, 1)
	// memdb is full.
	if full {
		newdb := memdb.New(lsm.MemDBSize)
		lsm.mu.Lock()
		lsm.dbList = append(lsm.dbList, lsm.db)
		lsm.db = newdb
		lsm.mu.Unlock()

		return lsm.db.Put(key, value, 1)
	}
	return err
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

// dumpTable dump immutable memtable to sstable.
func (lsm *LSM) dumpTable(level int, m *memdb.DB) error {
	id := atomic.AddUint64(&lsm.seq, 1)
	src := lsm.tableWriter.Marshal(level, id, m)

	// add storage ref count.
	name := fmt.Sprintf("%06d.sst", id)
	lsm.ref.Incr(1, name)

	lsm.log("dump table %s", name)

	return os.WriteFile(path.Join(lsm.dir, name), src, 0644)
}

// splitTable
func (lsm *LSM) splitTable(m *memdb.DB) error {
	pool := pool.New().WithErrors()
	db := memdb.New(lsm.MemDBSize)

	m.Iter(func(key, value []byte, meta uint16) {
		if full, err := db.PutIsFull(key, value, meta); full {
			// dump table.
			pool.Go(func() error {
				return lsm.dumpTable(1, db)
			})

			// create new memdb.
			db = memdb.New(lsm.MemDBSize)
			if err := db.Put(key, value, meta); err != nil {
				panic(err)
			}

		} else if err != nil {
			panic(err)
		}
	})

	// dump last table.
	pool.Go(func() error {
		return lsm.dumpTable(1, db)
	})

	return pool.Wait()
}

// MinorCompact
func (lsm *LSM) MinorCompact() {
	lsm.compactC <- struct{}{}

	lsm.mu.Lock()
	// need dump list.
	list := slices.Clone(lsm.dbList)
	lsm.dbList = lsm.dbList[:0]
	lsm.mu.Unlock()

	for _, m := range list {
		if err := lsm.dumpTable(0, m); err != nil {
			panic(err)
		}
	}

	<-lsm.compactC
}

// loadAllTables
func (lsm *LSM) loadAllTables() ([]*table.Table, []string, error) {
	tables := make([]*table.Table, 0, 16)
	names := make([]string, 0, 16)

	filepath.WalkDir(lsm.dir, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			panic(err)
		}

		name := entry.Name()
		if !strings.HasSuffix(name, ".sst") {
			return nil
		}

		// add query ref count.
		lsm.ref.Incr(1, name)

		sst, err := table.NewReader(path, lsm.Option)
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
	lsm.compactC <- struct{}{}
	start := time.Now()

	if err := lsm.compactLevel(); err != nil {
		panic(err)
	}

	lsm.ref.DelZero(func(tableName string) {
		if err := os.Remove(path.Join(lsm.dir, tableName)); err != nil {
			panic(err)
		}
	})

	fmt.Println("major compact cost:", time.Since(start))
	<-lsm.compactC
}

// compactLevel
func (lsm *LSM) compactLevel() error {
	tables, names, err := lsm.loadAllTables()
	if err != nil {
		return err
	}
	// delete query ref count.
	defer lsm.ref.Incr(-1, names...)

	if len(tables) <= 1 {
		return nil
	}

	// merge all tables.
	t := tables[0]
	t.Merge(tables[1:]...)

	// split tables.
	if err := lsm.splitTable(t.GetMemDB()); err != nil {
		panic(err)
	}

	if err := lsm.index.buildFromDisk(); err != nil {
		panic(err)
	}
	lsm.index.Print()

	// delete storage ref count.
	lsm.ref.Incr(-1, names...)

	return nil
}

func (lsm *LSM) log(msg string, args ...any) {
	lsm.logger.Info(fmt.Sprintf(msg, args...))
}
