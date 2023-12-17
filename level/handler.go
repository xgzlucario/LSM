package level

import (
	"bytes"
	"cmp"
	"slices"

	"github.com/xgzlucario/LSM/table"
)

// handler is a lsm-tree level handler.
type handler struct {
	level  int
	tables []*table.Table
}

// addTables
func (h *handler) addTables(tables ...*table.Table) {
	for _, t := range tables {
		t.AddRef()
	}
	h.tables = append(h.tables, tables...)
}

// delTables
func (h *handler) delTables(tables ...*table.Table) {
	for _, t := range tables {
		t.DelRef()
	}
}

// sortTables
func (h *handler) sortTables() {
	// level0 sorted by ID (created time), and level1+ sorted by maxKey.
	if h.level == 0 {
		slices.SortFunc(h.tables, func(a, b *table.Table) int {
			return cmp.Compare(a.ID(), b.ID())
		})
	} else {
		slices.SortFunc(h.tables, func(a, b *table.Table) int {
			return bytes.Compare(a.GetMinKey(), b.GetMinKey())
		})
	}
}

// findOverlapTables
func (h *handler) findOverlapTables() (newTables, overlapTables []*table.Table) {
	slices.SortFunc(h.tables, func(a, b *table.Table) int {
		return bytes.Compare(a.GetMinKey(), b.GetMinKey())
	})

	// find overlap tables.
	return nil, h.tables
}
