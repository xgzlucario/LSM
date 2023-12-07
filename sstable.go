package lsm

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"io"
	"os"

	"github.com/xgzlucario/LSM/memdb"
	"github.com/xgzlucario/LSM/pb"
	"google.golang.org/protobuf/proto"
)

const (
	typeVal uint16 = 1
	typeDel uint16 = 2

	footerSize = 8 + 4
)

var (
	order = binary.LittleEndian
)

// +-----------------+
// |  data block[0]  | <--+
// +-----------------+    |
// |     ... ...     |    |
// +-----------------+    |2
// |  data block[n]  |    |
// +-----------------+    |
// |                 | ---+
// |   index block   |
// |                 | <--+
// +-----------------+    |1
// |     footer      | ---+
// +-----------------+
// SSTable Defination.
type SSTable struct {
	fd *os.File

	// MemTable is the container for data in memory.
	// When lookup a table, the data from the corresponding dataBlock on disk is first
	// loaded into the memTable, and then find it.
	m *memdb.DB

	// indexBlock is the index of dataBlocks, loaded when the table is opened.
	indexBlock pb.IndexBlock

	// dataBlock is the container for data on disk.
	dataBlock pb.DataBlock
}

// Footer
type Footer struct {
	IndexBlockSize uint64
	CRC            uint32
}

// EncodeTable encode a memtable to bytes.
func EncodeTable(m *memdb.DB) []byte {
	buf := bytes.NewBuffer(make([]byte, 0, MemTableSize))
	var size uint32

	// initial.
	dataBlock := new(pb.DataBlock)
	indexBlock := &pb.IndexBlock{
		FirstKey: m.FirstKey(),
		LastKey:  m.LastKey(),
	}

	// encode data block function.
	encodeDataBlock := func() {
		src, _ := proto.Marshal(dataBlock)
		dst := compress(src, nil)

		indexBlock.Entries = append(indexBlock.Entries, &pb.IndexBlockEntry{
			LastKey: dataBlock.Keys[len(dataBlock.Keys)-1],
			Offset:  uint32(buf.Len()),
			Size:    uint32(len(dst)),
		})
		buf.Write(dst)

		dataBlock.Reset()
		size = 0
	}

	m.Iter(func(key, value []byte, meta uint16) {
		dataBlock.Keys = append(dataBlock.Keys, key)
		dataBlock.Values = append(dataBlock.Values, value)
		dataBlock.Types = append(dataBlock.Types, byte(meta))
		size += uint32(len(key) + len(value) + 1)

		// when reach the threshold, generate a new data block.
		if size >= DataBlockSize {
			encodeDataBlock()
		}
	})

	// encode the last data block.
	if len(dataBlock.Keys) > 0 {
		encodeDataBlock()
	}

	// encode index block.
	data, _ := proto.Marshal(indexBlock)
	buf.Write(data)

	// encode footer.
	binary.Write(buf, order, Footer{
		IndexBlockSize: uint64(len(data)),
		CRC:            crc32.ChecksumIEEE(data),
	})

	return buf.Bytes()
}

// NewSSTable create a sstable with decode index.
func NewSSTable(path string) (*SSTable, error) {
	fd, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	table := &SSTable{
		fd: fd,
		m:  memdb.New(8 * 1024 * 1024),
	}
	if err := table.loadIndex(); err != nil {
		return nil, err
	}

	return table, nil
}

// Close
func (s *SSTable) Close() error {
	return s.fd.Close()
}

// loadIndex load index block.
func (s *SSTable) loadIndex() error {
	buf, err := seekRead(s.fd, -footerSize, footerSize, io.SeekEnd)
	if err != nil {
		return err
	}

	// decode footer.
	var footer Footer
	if err := binary.Read(bytes.NewReader(buf), order, &footer); err != nil {
		return err
	}

	// decode index block.
	buf, err = seekRead(s.fd, -int64(footer.IndexBlockSize+footerSize), footer.IndexBlockSize, io.SeekEnd)
	if err != nil {
		return err
	}
	if crc32.ChecksumIEEE(buf) != footer.CRC {
		return ErrCRCChecksum
	}

	return proto.Unmarshal(buf, &s.indexBlock)
}

// findKey return value by find sstable.
// cached indicates whether the data hit the cache.
func (s *SSTable) findKey(key []byte) (res []byte, cached bool, err error) {
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
func (s *SSTable) loadDataBlock(entry *pb.IndexBlockEntry) (bool, error) {
	if entry.Cached {
		return false, nil
	}
	// read and decode.
	src, err := seekRead(s.fd, int64(entry.Offset), uint64(entry.Size), io.SeekStart)
	if err != nil {
		return false, err
	}
	dst, err := decompress(src, nil)
	if err != nil {
		return false, err
	}
	if err = proto.Unmarshal(dst, &s.dataBlock); err != nil {
		return false, err
	}

	// put to memtable.
	for i, k := range s.dataBlock.Keys {
		if err := s.m.Put(k, s.dataBlock.Values[i], uint16(s.dataBlock.Types[i])); err != nil {
			panic(err)
		}
	}
	entry.Cached = true

	return true, nil
}

// loadAllDataBlock load all data blocks to cache.
func (s *SSTable) loadAllDataBlock() error {
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

	buf := make([]byte, size)
	if _, err := fs.Read(buf); err != nil {
		return nil, err
	}

	return buf, nil
}

// merge
func (s *SSTable) Merge(tables ...*SSTable) {
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
func (t *SSTable) IsOverlap(n *SSTable) bool {
	if bytes.Compare(t.indexBlock.FirstKey, n.indexBlock.FirstKey) <= 0 &&
		bytes.Compare(n.indexBlock.FirstKey, t.indexBlock.LastKey) <= 0 {
		return true
	}

	return bytes.Compare(n.indexBlock.FirstKey, t.indexBlock.FirstKey) <= 0 &&
		bytes.Compare(t.indexBlock.FirstKey, n.indexBlock.LastKey) <= 0
}
