package table

import (
	"os"

	"github.com/xgzlucario/LSM/option"
)

type Reader struct{}

// NewReader
func NewReader(path string, opt *option.Option) (*Table, error) {
	fd, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	table := &Table{fd: fd, opt: opt}
	if err := table.loadIndex(); err != nil {
		return nil, err
	}

	return table, nil
}
