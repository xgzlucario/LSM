package level

import (
	"bytes"
	"cmp"
	"slices"

	"github.com/xgzlucario/LSM/bcmp"
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

// truncateOverlapTables
func (h *handler) truncateOverlapTables() (newTables, overlapTables []*table.Table) {
	newTables = make([]*table.Table, 0, maxCompactTableLength)
	overlapTables = make([]*table.Table, 0, maxCompactTableLength)

	// find overlap tables.
	var krange = [2][]byte{
		h.tables[0].GetMinKey(),
		h.tables[0].GetMaxKey(),
	}
	overlapTables = append(overlapTables, h.tables[0])

	for _, table := range h.tables[1:] {
		minKey, maxKey := table.GetMinKey(), table.GetMaxKey()

		// is overlap
		if bcmp.Between(minKey, krange[0], krange[1]) || bcmp.Between(maxKey, krange[0], krange[1]) {
			krange[0] = bcmp.Min(krange[0], minKey)
			krange[1] = bcmp.Max(krange[1], maxKey)
			overlapTables = append(overlapTables, table)

		} else {
			newTables = append(newTables, table)
		}
	}
	return
}