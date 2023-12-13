package lsm

import (
	"context"
	"fmt"
	"os"
	"slices"
	"sync"
	"time"

	"github.com/xgzlucario/LSM/level"
	"github.com/xgzlucario/LSM/memdb"
	"github.com/xgzlucario/LSM/option"
	"github.com/xgzlucario/LSM/table"
)

// LSM-Tree defination.
type LSM struct {
	*option.Option

	dir string

	ctx    context.Context
	cancel context.CancelFunc

	// guards db and dbList.
	mu     sync.RWMutex
	db     *memdb.DB
	dbList []*memdb.DB

	// index controller.
	index *level.Controller

	tableWriter *table.Writer

	compactC chan struct{}
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
		ctx:         ctx,
		cancel:      cancel,
		db:          memdb.New(opt.MemDBSize),
		dbList:      make([]*memdb.DB, 0, 16),
		index:       level.NewController(dir, opt),
		tableWriter: table.NewWriter(opt),
		compactC:    make(chan struct{}, 1),
	}

	// build index.
	if err := lsm.index.BuildFromDisk(); err != nil {
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

	fmt.Println("LSM-Tree started.")

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
func (lsm *LSM) MinorCompact() {
	lsm.compactC <- struct{}{}

	lsm.mu.Lock()
	// need dump list.
	list := slices.Clone(lsm.dbList)
	lsm.dbList = lsm.dbList[:0]
	lsm.mu.Unlock()

	for _, db := range list {
		if err := lsm.index.AddLevel0Table(db); err != nil {
			panic(err)
		}
	}
	lsm.index.Print()

	<-lsm.compactC
}

// MajorCompact
func (lsm *LSM) MajorCompact() {
	lsm.compactC <- struct{}{}
	start := time.Now()

	if err := lsm.index.Compact(); err != nil {
		panic(err)
	}

	fmt.Println("major compact cost:", time.Since(start))
	<-lsm.compactC
}
