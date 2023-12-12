package table

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"sync/atomic"
	"unsafe"

	"github.com/xgzlucario/LSM/memdb"
	"github.com/xgzlucario/LSM/option"
	"github.com/xgzlucario/LSM/pb"
	"google.golang.org/protobuf/proto"
)

const (
	magic = "\x4d\x89\x8c\xc4\x0a\x9c\x7a\xdb"
)

var (
	order       = binary.LittleEndian
	footerSize  = uint64(unsafe.Sizeof(Footer{}))
	magicNumber = order.Uint64([]byte(magic))
)

var (
	ErrKeyNotFound = errors.New("table: key not found")
	ErrChecksum    = errors.New("table: invalid crc checksum")
	ErrMagicNumber = errors.New("table: invalid magic number")
)

// Table
type Table struct {
	fd  *os.File
	opt *option.Option

	// ref is the reference count of the table.
	ref atomic.Int32

	// MemTable is the container for data in memory.
	// When lookup a table, the data from the corresponding dataBlock on disk is first
	// loaded into the memTable, and then find it.
	m *memdb.DB

	// indexBlock is the index of dataBlocks, loaded when the table is opened.
	indexBlock pb.IndexBlock

	// footer
	footer Footer
}

// Footer
type Footer struct {
	Level          uint32
	CRC            uint32
	IndexBlockSize uint64
	Id             uint64
	MagicNumber    uint64
}

// GetId
func (s *Table) GetId() uint64 {
	return s.footer.Id
}

// GetLevel
func (s *Table) GetLevel() int {
	return int(s.footer.Level)
}

// GetFirstKey
func (s *Table) GetFirstKey() []byte {
	return s.indexBlock.FirstKey
}

// GetLastKey
func (s *Table) GetLastKey() []byte {
	return s.indexBlock.LastKey
}

// GetMemDB
func (s *Table) GetMemDB() *memdb.DB {
	return s.m
}

// GetFileSize
func (s *Table) GetFileSize() int64 {
	stat, _ := s.fd.Stat()
	return stat.Size()
}

// Close
func (s *Table) Close() error {
	return s.fd.Close()
}

// AddRef
func (s *Table) AddRef() {
	s.ref.Add(1)
}

// DelRef
func (s *Table) DelRef() {
	if s.ref.Add(-1) == 0 {
		os.Remove(s.fd.Name())
		s.fd.Close()
	}
}

// loadIndex load index block.
func (s *Table) loadIndex() error {
	buf, err := seekRead(s.fd, -int64(footerSize), footerSize, io.SeekEnd)
	if err != nil {
		return err
	}

	// decode footer.
	if err := binary.Read(bytes.NewReader(buf), order, &s.footer); err != nil {
		return err
	}
	if s.footer.MagicNumber != magicNumber {
		return ErrMagicNumber
	}

	// decode index block.
	buf, err = seekRead(s.fd, -int64(s.footer.IndexBlockSize+footerSize), s.footer.IndexBlockSize, io.SeekEnd)
	if err != nil {
		return err
	}
	if crc32.ChecksumIEEE(buf) != s.footer.CRC {
		return ErrChecksum
	}

	return proto.Unmarshal(buf, &s.indexBlock)
}

// FindKey return value by find sstable.
// cached indicates whether the data hit the cache.
func (s *Table) FindKey(key []byte) (res []byte, cached bool, err error) {
	for _, entry := range s.indexBlock.Entries {
		if bytes.Compare(key, entry.LastKey) <= 0 {
			// load cache.
			if ok, err := s.loadDataBlock(entry); err != nil {
				return nil, false, err

			} else {
				cached = !ok
			}
			break
		}
	}

	// find in memtable.
	res, ok := s.m.Get(key)
	if !ok {
		return nil, false, ErrKeyNotFound
	}
	return
}

// loadDataBlock load data block to cache.
func (s *Table) loadDataBlock(entry *pb.IndexBlockEntry) (bool, error) {
	if entry.Cached {
		return false, nil
	}
	// load and decode from disk.
	src, err := seekRead(s.fd, int64(entry.Offset), uint64(entry.Size), io.SeekStart)
	if err != nil {
		return false, err
	}
	dst, err := decompress(src, nil)
	if err != nil {
		return false, err
	}

	var dataBlock pb.DataBlock
	if err = proto.Unmarshal(dst, &dataBlock); err != nil {
		return false, err
	}

	// put to memtable.
	if s.m == nil {
		s.m = memdb.New(uint32(float64(s.opt.MemDBSize) * 1.1))
	}
	for i, k := range dataBlock.Keys {
		if err := s.m.Put(k, dataBlock.Values[i], uint16(dataBlock.Types[i])); err != nil {
			panic(err)
		}
	}
	entry.Cached = true

	return true, nil
}

// loadAllDataBlock load all data blocks to cache.
func (s *Table) loadAllDataBlock() error {
	for _, entry := range s.indexBlock.Entries {
		if _, err := s.loadDataBlock(entry); err != nil {
			return err
		}
	}
	return nil
}

// seekRead first seek(offset, whence) and then read(size).
func seekRead(fs *os.File, offset int64, size uint64, whence int) ([]byte, error) {
	if _, err := fs.Seek(offset, whence); err != nil {
		return nil, err
	}
	// TODO use buffer pool.
	buf := make([]byte, size)
	if _, err := fs.Read(buf); err != nil {
		return nil, err
	}

	return buf, nil
}

// merge
func (s *Table) Merge(tables ...*Table) {
	if err := s.loadAllDataBlock(); err != nil {
		panic(err)
	}
	for _, t := range tables {
		if err := t.loadAllDataBlock(); err != nil {
			panic(err)
		}
	}

	db := make([]*memdb.DB, 0, len(tables))
	for _, t := range tables {
		db = append(db, t.m)
	}
	s.m.Merge(db...)
}

// IsOverlap
func (t *Table) IsOverlap(n *Table) bool {
	if bytes.Compare(t.indexBlock.FirstKey, n.indexBlock.FirstKey) <= 0 &&
		bytes.Compare(n.indexBlock.FirstKey, t.indexBlock.LastKey) <= 0 {
		return true
	}

	return bytes.Compare(n.indexBlock.FirstKey, t.indexBlock.FirstKey) <= 0 &&
		bytes.Compare(t.indexBlock.FirstKey, n.indexBlock.LastKey) <= 0
}
