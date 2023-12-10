package table

import (
	"io/fs"
	"path/filepath"
	"strings"

	"github.com/xgzlucario/LSM/option"
)

const maxLevel = 7

// FdFile is file descriptor of each level.
type FdFile []*SSTable

// FdFiles is the file descriptor of all levels.
type FdFiles struct {
	*option.Option
	fd [maxLevel]FdFile
}

// New
func NewWithOption(dir string, option *option.Option) (*FdFiles, error) {
	index := &FdFiles{
		Option: option,
		fd:     [maxLevel]FdFile{},
	}

	// walk dir.
	filepath.WalkDir(dir, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			panic(err)
		}

		name := entry.Name()
		if !strings.HasSuffix(name, tableNameExt) {
			return nil
		}

		sst, err := NewSSTable(path, option.MemDBSize)
		if err != nil {
			panic(err)
		}

		index.fd[sst.GetLevel()] = append(index.fd[sst.GetLevel()], sst)

		return nil
	})

	return index, nil
}
