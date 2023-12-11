package lsm

import (
	"io/fs"
	"path/filepath"
	"strings"
	"sync"

	"github.com/xgzlucario/LSM/option"
	"github.com/xgzlucario/LSM/table"
)

const (
	maxLevel = 6

	tableExt = ".sst"
)

// LevelHandler
type LevelHandler struct {
	sync.RWMutex
	opt *option.Option

	tables []*table.Table
}

// LevelsController
type LevelsController struct {
	sync.RWMutex
	opt *option.Option

	level0 *LevelHandler
	levels [maxLevel]*LevelHandler
}

// NewLevelHandler
func NewLevelHandler(tables []*table.Table, opt *option.Option) *LevelHandler {
	return &LevelHandler{
		opt:    opt,
		tables: tables,
	}
}

// AddTable
func (lh *LevelHandler) AddTable(table *table.Table) {
	lh.tables = append(lh.tables, table)
	// level0 sorted by ID (created time), and level1+ sorted by lastKey.
}

// NewLevelsController
func NewLevelsController(dir string, opt *option.Option) (*LevelsController, error) {
	ctl := &LevelsController{
		opt: opt,
	}

	// walk dir.
	filepath.WalkDir(dir, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			panic(err)
		}

		name := entry.Name()
		if !strings.HasSuffix(name, tableExt) {
			return nil
		}

		table, err := table.NewTable(path, opt)
		if err != nil {
			panic(err)
		}

		if table.GetLevel() == 0 {
			ctl.level0.AddTable(table)
		} else {
			ctl.levels[table.GetLevel()].AddTable(table)
		}

		return nil
	})

	return ctl, nil
}
