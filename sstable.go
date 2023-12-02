package lsm

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"io"
	"os"
	"slices"

	"github.com/xgzlucario/LSM/pb"
	"google.golang.org/protobuf/proto"
)

const (
	// footer contains [index_block_size, crc].
	footerSize = 8 + 4
)

var (
	order = binary.LittleEndian
)

type vtype = uint16

const (
	vtypeVal vtype = iota + 1
	vtypeDel
)

// SSTable
type SSTable struct {
	fd         *os.File
	m          *MemTable
	indexBlock pb.IndexBlock
	dataBlock  pb.DataBlock
}

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
// EncodeTable dumps a memtable to a sstable.
func EncodeTable(m *MemTable) []byte {
	buf := bytes.NewBuffer(make([]byte, 0, DataBlockSize))
	it := m.it

	// initial.
	dataBlock := new(pb.DataBlock)
	indexBlock := &pb.IndexBlock{
		FirstKey: m.FirstKey(),
		LastKey:  m.LastKey(),
	}

	// encode data block.
	do := func() {
		src, _ := proto.Marshal(dataBlock)
		dst := Compress(src, nil)

		indexBlock.Entries = append(indexBlock.Entries, &pb.IndexBlockEntry{
			LastKey: dataBlock.Keys[len(dataBlock.Keys)-1],
			Offset:  uint32(buf.Len()),
			Size:    uint32(len(dst)),
		})
		buf.Write(dst)

		dataBlock.Reset()
	}

	m.Iter(func(key, value []byte, meta uint16) {
		dataBlock.Keys = append(dataBlock.Keys, it.Key())
		dataBlock.Values = append(dataBlock.Values, it.Value())
		dataBlock.Types = append(dataBlock.Types, byte(it.Meta()))
		dataBlock.Size += uint32(len(it.Key()) + len(it.Value()) + 1)

		// encode data blocks.
		if dataBlock.Size >= DataBlockSize {
			do()
		}
	})
	do()

	// encode index block.
	data, _ := proto.Marshal(indexBlock)
	buf.Write(data)

	// encode footer.
	binary.Write(buf, order, uint64(len(data)))
	binary.Write(buf, order, crc32.ChecksumIEEE(data))

	return buf.Bytes()
}

// NewSSTable
func NewSSTable(path string) (*SSTable, error) {
	fd, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	return &SSTable{fd: fd}, nil
}

// Close
func (s *SSTable) Close() error {
	return s.fd.Close()
}

// decodeIndex decode index block.
func (s *SSTable) decodeIndex() error {
	buf, err := seekRead(s.fd, -footerSize, footerSize, io.SeekEnd)
	if err != nil {
		return err
	}
	indexBlockSize := order.Uint64(buf)
	crc := order.Uint32(buf[8:])

	// decode index block.
	buf, err = seekRead(s.fd, -int64(indexBlockSize+footerSize), indexBlockSize, io.SeekEnd)
	if err != nil {
		return err
	}

	// check crc.
	if crc32.ChecksumIEEE(buf) != crc {
		return ErrCRCChecksum
	}

	return proto.Unmarshal(buf, &s.indexBlock)
}

// findKey return value by key.
func (s *SSTable) findKey(key []byte) ([]byte, error) {
	// decode index first.
	if err := s.decodeIndex(); err != nil {
		return nil, err
	}

	for _, entry := range s.indexBlock.Entries {
		if bytes.Compare(key, entry.LastKey) <= 0 {
			// read
			src, err := seekRead(s.fd, int64(entry.Offset), uint64(entry.Size), io.SeekStart)
			if err != nil {
				return nil, err
			}
			// decompress
			dst, err := Decompress(src, nil)
			if err != nil {
				return nil, err
			}
			if err = proto.Unmarshal(dst, &s.dataBlock); err != nil {
				return nil, err
			}
			break
		}
	}

	// binary search.
	i, ok := slices.BinarySearchFunc(s.dataBlock.Keys, key, func(a, b []byte) int {
		return bytes.Compare(a, b)
	})
	if ok {
		return s.dataBlock.Values[i], nil
	}

	return nil, nil
}

// decodeData decode all data blocks.
func (s *SSTable) decodeData() error {
	// decode index first.
	if err := s.decodeIndex(); err != nil {
		return err
	}
	s.m = NewMemTable()

	for _, entry := range s.indexBlock.Entries {
		// read
		src, err := seekRead(s.fd, int64(entry.Offset), uint64(entry.Size), io.SeekStart)
		if err != nil {
			return err
		}
		// decompress
		dst, err := Decompress(src, nil)
		if err != nil {
			return err
		}

		if err = proto.Unmarshal(dst, &s.dataBlock); err != nil {
			return err
		}

		// insert
		for i, key := range s.dataBlock.Keys {
			s.m.PutRaw(key, s.dataBlock.Values[i], uint16(s.dataBlock.Types[i]))
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
func (s *SSTable) merge(t *SSTable) {
	s.m.Merge(t.m)
}
