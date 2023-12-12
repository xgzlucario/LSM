package level

import (
	"fmt"
	"io/fs"
	"path/filepath"
	"strings"
	"sync"

	"github.com/xgzlucario/LSM/option"
	"github.com/xgzlucario/LSM/table"
)

const (
	maxLevel = 7

	tableExt = ".sst"
)

// Controller
type Controller struct {
	sync.RWMutex
	dir    string
	opt    *option.Option
	levels [maxLevel]*Handler
}

// NewController
func NewController(dir string, opt *option.Option) *Controller {
	ctl := &Controller{
		dir: dir,
		opt: opt,
	}
	for i := 0; i < maxLevel; i++ {
		ctl.levels[i] = &Handler{
			level:  i,
			tables: make([]*table.Table, 0, 8),
		}
	}

	return ctl
}

// BuildFromDisk
func (ctl *Controller) BuildFromDisk() error {
	ctl.Lock()
	defer ctl.Unlock()

	for _, lh := range ctl.levels {
		lh.Lock()
		defer lh.Unlock()
		lh.tables = lh.tables[:0]
	}

	// walk dir.
	err := filepath.WalkDir(ctl.dir, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			panic(err)
		}

		name := entry.Name()
		if !strings.HasSuffix(name, tableExt) {
			return nil
		}

		table, err := table.NewReader(path, ctl.opt)
		if err != nil {
			return err
		}
		ctl.levels[table.GetLevel()].addTable(table)

		return nil
	})
	if err != nil {
		return err
	}

	for _, lh := range ctl.levels {
		lh.sortTables()
	}

	ctl.Print()

	return nil
}

// Print
func (ctl *Controller) Print() {
	for _, level := range ctl.levels {
		if len(level.tables) > 0 {
			fmt.Println()
			fmt.Println("=====level", level.level)

			for _, t := range level.tables {
				fmt.Println(
					"id:", t.GetId(),
					"level:", t.GetLevel(),
					"first:", string(t.GetFirstKey()),
					"last:", string(t.GetLastKey()))
			}
		}
	}
}
