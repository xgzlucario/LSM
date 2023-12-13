package option

import "time"

const (
	KB = 1 << 10
	MB = 1 << 20
)

// Option for LSM-Tree.
type Option struct {
	Path string

	MemDBSize     uint32
	DataBlockSize uint32

	MinorCompactInterval time.Duration
	MajorCompactInterval time.Duration
}

// DefaultOption
var DefaultOption = &Option{
	Path:                 "data",
	MemDBSize:            4 * MB,
	DataBlockSize:        4 * KB,
	MinorCompactInterval: time.Second,
	MajorCompactInterval: 5 * time.Second,
}
