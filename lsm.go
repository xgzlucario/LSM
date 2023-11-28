package lsm

import "github.com/tidwall/wal"

type LSM struct {
	path string

	mt  *MemTable
	imt []*MemTable // immutable memtables.

	wal *wal.Log
}
