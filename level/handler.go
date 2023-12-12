package level

import (
	"bytes"
	"cmp"
	"slices"
	"sync"

	"github.com/xgzlucario/LSM/table"
)

// Handler is a lsm-tree level handler.
type Handler struct {
	sync.RWMutex
	level  int
	size   int64
	tables []*table.Table
}

// addTable
func (h *Handler) addTable(t *table.Table) {
	h.size += t.GetFileSize()
	t.AddRef()
	h.tables = append(h.tables, t)
}

// delTable
func (h *Handler) delTable(t *table.Table) {
	h.size -= t.GetFileSize()
	t.DelRef()
}

// sortTables
func (h *Handler) sortTables() {
	// level0 sorted by ID (created time), and level1+ sorted by lastKey.
	if h.level == 0 {
		slices.SortFunc(h.tables, func(a, b *table.Table) int {
			return cmp.Compare(a.GetId(), b.GetId())
		})

	} else {
		slices.SortFunc(h.tables, func(a, b *table.Table) int {
			return bytes.Compare(a.GetLastKey(), b.GetLastKey())
		})
	}
}

// findOverlapTables
func (h *Handler) findOverlapTables() (int, []*table.Table) {
	h.RLock()
	defer h.RUnlock()

	// merge all level0 tables.
	if h.level == 0 {
		return 0, h.tables
	}

	return -1, nil
}
