package lsm

import "github.com/andy-kimball/arenaskl"

type Skiplist struct {
	*arenaskl.Skiplist
}

func NewSkiplist() *Skiplist {
	return &Skiplist{
		arenaskl.NewSkiplist(arenaskl.NewArena(1 << 20)),
	}
}
