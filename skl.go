package lsm

import "github.com/andy-kimball/arenaskl"

// Skiplist based on Arena Skiplist.
type Skiplist struct {
	*arenaskl.Skiplist
}

// NewSkiplist creates a new Skiplist.
func NewSkiplist(arenaSize uint32) *Skiplist {
	return &Skiplist{
		arenaskl.NewSkiplist(arenaskl.NewArena(arenaSize)),
	}
}
