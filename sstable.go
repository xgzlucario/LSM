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

	vtypeVal uint16 = 1
	vtypeDel uint16 = 2
)

var (
	order = binary.LittleEndian
)

// SSTable
type SSTable struct {
	fd         *os.File
	m          *MemTable
	indexBlock pb.IndexBlock

	dataDecoded bool
	dataBlock   pb.DataBlock
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
// EncodeTable encode a memtable to bytes.
func EncodeTable(m *MemTable) []byte {
	buf := bytes.NewBuffer(make([]byte, 0, DataBlockSize))

	// initial.
	dataBlock := new(pb.DataBlock)
	indexBlock := &pb.IndexBlock{
		FirstKey: m.FirstKey(),
		LastKey:  m.LastKey(),
	}

	// encode data block.
	for m.it.SeekToFirst(); ; {
		dataBlock.Keys = append(dataBlock.Keys, m.it.Key())
		dataBlock.Values = append(dataBlock.Values, m.it.Value())
		dataBlock.Types = append(dataBlock.Types, byte(m.it.Meta()))
		dataBlock.Size += uint32(len(m.it.Key()) + len(m.it.Value()) + 1)

		m.it.Next()

		// encode data blocks.
		if dataBlock.Size >= DataBlockSize || !m.it.Valid() {
			src, _ := proto.Marshal(dataBlock)
			// compress.
			dst := Compress(src, nil)

			indexBlock.Entries = append(indexBlock.Entries, &pb.IndexBlockEntry{
				LastKey: dataBlock.Keys[len(dataBlock.Keys)-1],
				Offset:  uint32(buf.Len()),
				Size:    uint32(len(dst)),
			})
			buf.Write(dst)

			dataBlock.Reset()

			// break if end.
			if !m.it.Valid() {
				break
			}
		}
	}

	// encode index block.
	data, _ := proto.Marshal(indexBlock)
	buf.Write(data)

	// encode footer.
	binary.Write(buf, order, uint64(len(data)))
	binary.Write(buf, order, crc32.ChecksumIEEE(data))

	return buf.Bytes()
}

// NewSSTable create a sstable with decode index.
func NewSSTable(path string) (*SSTable, error) {
	fd, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	table := &SSTable{fd: fd}
	if err := table.decodeIndex(); err != nil {
		return nil, err
	}

	return table, nil
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

// findKey return value by find sstable on disk.
func (s *SSTable) findKey(key []byte) ([]byte, error) {
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
	if s.dataDecoded {
		return nil
	}
	s.dataDecoded = true
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
	if err := s.decodeData(); err != nil {
		panic(err)
	}
	if err := t.decodeData(); err != nil {
		panic(err)
	}

	s.m.Merge(t.m)
}
