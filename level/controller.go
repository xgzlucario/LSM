package level

import (
	"fmt"
	"io/fs"
	"os"
	"path"
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
	mu       sync.RWMutex
	tid      uint64
	dir      string
	opt      *option.Option
	handlers [maxLevel]*handler
}

// NewController
func NewController(dir string, opt *option.Option) *Controller {
	c := &Controller{
		dir: dir,
		opt: opt,
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

	for _, h := range c.handlers {
		h.Lock()
		defer h.Unlock()
		h.tables = h.tables[:0]
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

	for _, h := range c.handlers {
		h.sortTables()
	}
	c.Print()

	return nil
}

// Print
func (c *Controller) Print() {
	for _, h := range c.handlers {
		fmt.Println(h.tables)
	}
}

// AddTable
func (c *Controller) AddTable(level int, table *table.Table) {
	h := c.handlers[level]
	h.Lock()
	h.addTables(table)
	h.Unlock()
}

// Compact
func (c *Controller) Compact() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, h := range c.handlers {
		if len(h.tables) == 0 {
			continue
		}

		tables, truncateTables := h.truncateOverlapTables()
		fmt.Println(tables)
		fmt.Println(truncateTables)

		db := table.MergeTables(truncateTables...)

		// split dbs.
		db.SplitFunc(c.opt.MemDBSize, func(db *memdb.DB) error {
			return c.DumpTable(h.level, db)
		})
	}

	return c.BuildFromDisk()
}

// dumpTable
func (c *Controller) DumpTable(level int, db *memdb.DB) error {
	id := atomic.AddUint64(&c.tid, 1)

	name := fmt.Sprintf("%06d.sst", id)
	fileName := path.Join(c.dir, name)

	src := table.NewWriter(c.opt).WriteTable(level, id, db)
	return os.WriteFile(fileName, src, 0644)
}
