package lsm

import "github.com/xgzlucario/LSM/table"

const maxLevel = 7

// FdFile is file descriptor of each level.
type FdFile []*table.SSTable

// FdFiles is the file descriptor of all levels.
type FdFiles [maxLevel]*FdFile

func InitDir(dir string) (*FdFiles, error) {
	return nil, nil
}
