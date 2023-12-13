package level

import (
	"fmt"
	"io/fs"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/xgzlucario/LSM/memdb"
	"github.com/xgzlucario/LSM/option"
	"github.com/xgzlucario/LSM/table"
)

const (
	maxLevel              = 7
	maxCompactTableLength = 10
)

// Controller is a levels controller in lsm-tree.
type Controller struct {
	mu          sync.RWMutex
	tid         atomic.Uint64
	dir         string
	opt         *option.Option
	handlers    [maxLevel]*handler
	tableWriter *table.Writer
}

// NewController
func NewController(dir string, opt *option.Option) *Controller {
	c := &Controller{
		dir:         dir,
		opt:         opt,
		tableWriter: table.NewWriter(opt),
	}
	for i := range c.handlers {
		c.handlers[i] = &handler{
			level:  i,
			tables: make([]*table.Table, 0, 8),
		}
	}
	return c
}

// BuildFromDisk
func (c *Controller) BuildFromDisk() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, handler := range c.handlers {
		handler.Lock()
		defer handler.Unlock()
		handler.tables = handler.tables[:0]
	}

	// walk dir.
	err := filepath.WalkDir(c.dir, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			panic(err)
		}
		if entry.IsDir() {
			return nil
		}
		// create reader.
		table, err := table.NewReader(path, c.opt)
		if err != nil {
			return err
		}
		c.handlers[table.Level()].addTables(table)

		return nil
	})
	if err != nil {
		return err
	}

	fmt.Println("controller: build from disk.")

	for _, handler := range c.handlers {
		handler.sortTables()
	}
	c.Print()

	return nil
}

// Print
func (c *Controller) Print() {
	for _, handler := range c.handlers {
		fmt.Println(handler.tables)
	}
}

// Compact
func (c *Controller) Compact() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, handler := range c.handlers {
		handler.Lock()

		if len(handler.tables) == 0 {
			handler.Unlock()
			continue
		}

		tables, truncateTables := handler.truncateOverlapTables()
		handler.tables = tables

		db := table.MergeTables(truncateTables...)

		// split merged memdb.
		toLevel := handler.level
		if handler.level == 0 {
			toLevel = 1
		}

		err := db.SplitFunc(c.opt.MemDBSize, func(db *memdb.DB) error {
			table, err := c.tableWriter.WriteTable(toLevel, c.tid.Add(1), db)
			if err != nil {
				return err
			}
			c.handlers[toLevel].addTables(table)
			return nil
		})
		if err != nil {
			panic(err)
		}

		// delete truncate tables.
		handler.delTables(truncateTables...)

		handler.Unlock()
	}

	return nil
}

// AddLevel0Table
func (c *Controller) AddLevel0Table(db *memdb.DB) error {
	table, err := c.tableWriter.WriteTable(0, c.tid.Add(1), db)
	if err != nil {
		return err
	}
	c.handlers[0].addTables(table)
	return nil
}
