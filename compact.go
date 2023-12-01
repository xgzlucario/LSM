package lsm

import (
	"bytes"

	"github.com/andy-kimball/arenaskl"
)

// MetaTable
type MetaTable struct {
	Level             int
	Name              string
	FirstKey, LastKey []byte
}

// IsOverlap
func (t *MetaTable) IsOverlap(n *MetaTable) bool {
	if bytes.Compare(t.FirstKey, n.FirstKey) <= 0 &&
		bytes.Compare(n.FirstKey, t.LastKey) <= 0 {
		return true
	}

	return bytes.Compare(n.FirstKey, t.FirstKey) <= 0 &&
		bytes.Compare(t.FirstKey, n.LastKey) <= 0
}

// CompactFiles
func CompactFiles(paths ...string) (*arenaskl.Iterator, error) {
	skls := make([]*arenaskl.Iterator, 0, len(paths))

	// decode all.
	for _, path := range paths {
		decoder, err := NewTableDecoder(path)
		if err != nil {
			return nil, err
		}
		defer decoder.Close()

		if err = decoder.decodeIndexBlock(); err != nil {
			return nil, err
		}
		it, err := decoder.decodeAll()
		if err != nil {
			return nil, err
		}
		skls = append(skls, it)
	}

	return compact(skls...)
}

// CompactTables
func CompactTables(tables ...*MemTable) (*arenaskl.Iterator, error) {
	skls := make([]*arenaskl.Iterator, 0, len(tables))
	for _, table := range tables {
		skls = append(skls, table.it)
	}
	return compact(skls...)
}

// compact
func compact(skls ...*arenaskl.Iterator) (*arenaskl.Iterator, error) {
	skl0 := skls[0]
	// merge skiplists.
	for _, it := range skls[1:] {
		it.SeekToFirst()
		for it.Valid() {
			skl0.Add(it.Key(), it.Value(), uint16(it.Meta()))
			it.Next()
		}
	}
	return skl0, nil
}
