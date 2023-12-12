package lsm

import (
	"bytes"
	"cmp"
	"fmt"
	"io/fs"
	"path/filepath"
	"slices"
	"strings"
	"sync"

	"github.com/xgzlucario/LSM/option"
	"github.com/xgzlucario/LSM/table"
)

const (
	maxLevel = 7

	tableExt = ".sst"
)

// LevelHandler
type LevelHandler struct {
	sync.RWMutex

	level  int
	tables []*table.Table
}

// LevelController
type LevelController struct {
	sync.RWMutex

	dir string
	opt *option.Option

	levels [maxLevel]*LevelHandler
}

// NewLevelHandler
func NewLevelHandler(level int) *LevelHandler {
	return &LevelHandler{
		level:  level,
		tables: make([]*table.Table, 0, 8),
	}
}

// AddTable
func (lh *LevelHandler) AddTable(t *table.Table) {
	lh.Lock()
	lh.tables = append(lh.tables, t)

	// level0 sorted by ID (created time), and level1+ sorted by lastKey.
	if lh.level == 0 {
		slices.SortFunc(lh.tables, func(a, b *table.Table) int {
			return cmp.Compare(a.GetId(), b.GetId())
		})

	} else {
		slices.SortFunc(lh.tables, func(a, b *table.Table) int {
			return bytes.Compare(a.GetLastKey(), b.GetLastKey())
		})
	}
	lh.Unlock()
}

// NewLevelController
func NewLevelController(dir string, opt *option.Option) *LevelController {
	ctl := &LevelController{
		dir: dir,
		opt: opt,
	}
	for i := 0; i < maxLevel; i++ {
		ctl.levels[i] = NewLevelHandler(i)
	}

	return ctl
}

// buildFromDisk
func (ctl *LevelController) buildFromDisk() error {
	for i := 0; i < maxLevel; i++ {
		ctl.levels[i].Lock()
		defer ctl.levels[i].Unlock()
		ctl.levels[i] = NewLevelHandler(i)
	}

	// walk dir.
	return filepath.WalkDir(ctl.dir, func(path string, entry fs.DirEntry, err error) error {
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
		ctl.levels[table.GetLevel()].AddTable(table)

		return nil
	})
}

// Print
func (ctl *LevelController) Print() {
	for i := 0; i < maxLevel; i++ {
		ctl.levels[i].RLock()

		fmt.Println("=====level", i)
		for _, t := range ctl.levels[i].tables {
			fmt.Println(
				"id:", t.GetId(),
				"level:", t.GetLevel(),
				"first:", string(t.GetFirstKey()),
				"last:", string(t.GetLastKey()))
		}

		ctl.levels[i].RUnlock()
	}
}
