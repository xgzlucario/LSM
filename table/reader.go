package table

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/xgzlucario/LSM/option"
)

const (
	tableExt = ".sst"
)

var (
	ErrTableName = errors.New("table path is not *.sst format")
)

type Reader struct{}

// NewReader
func NewReader(path string, opt *option.Option) (*Table, error) {
	if !strings.HasSuffix(path, tableExt) {
		return nil, fmt.Errorf("%w: %s", ErrTableName, path)
	}

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
