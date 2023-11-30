package lsm

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"
	"slices"

	"github.com/xgzlucario/LSM/pb"
	"google.golang.org/protobuf/proto"
)

const (
	footerSize = 8
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
	*Config
	*MemTable
	// bloom *bloom.BloomFilter
}

// DumpTable
// +-----------------+
// |  data block[0]  | <--+
// +-----------------+    |
// |     ... ...     |    |
// +-----------------+    |(2)
// |  data block[n]  |    |
// +-----------------+    |
// |                 | ---+
// |   index block   |
// |                 | <--+
// +-----------------+    |(1)
// |     footer      | ---+
// +-----------------+
func (s *SSTable) DumpTable() []byte {
	buf := bytes.NewBuffer(make([]byte, 0, s.DataBlockSize))

	dataBlock := new(pb.DataBlock)
	indexBlocks := make([]*pb.IndexBlockEntry, 0)

	// iter memtable.
	s.it.SeekToFirst()
	for {
		dataBlock.Keys = append(dataBlock.Keys, s.it.Key())
		dataBlock.Values = append(dataBlock.Values, s.it.Value())
		dataBlock.Types = append(dataBlock.Types, byte(s.it.Meta()))
		dataBlock.Size += uint32(len(s.it.Key()) + len(s.it.Value()) + 1)

		s.it.Next()

		// encode data blocks.
		if dataBlock.Size >= s.DataBlockSize || !s.it.Valid() {
			src, _ := proto.Marshal(dataBlock)
			// TODO zstd
			indexBlocks = append(indexBlocks, &pb.IndexBlockEntry{
				LastKey: dataBlock.Keys[len(dataBlock.Keys)-1],
				Offset:  uint32(buf.Len()),
				Size:    uint32(len(src)),
			})
			buf.Write(src)

			// break if end.
			if !s.it.Valid() {
				break
			}
		}
	}

	// encode index block.
	data, _ := proto.Marshal(&pb.IndexBlock{Entries: indexBlocks})
	buf.Write(data)

	// encode footer.
	IndexBlockSize := uint64(len(data))
	binary.Write(buf, order, IndexBlockSize)

	return buf.Bytes()
}

// FindSSTable
func FindSSTable(key []byte, path string) ([]byte, error) {
	fd, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer fd.Close()

	// decode footer.
	buf, err := seekRead(fd, -footerSize, footerSize, io.SeekEnd)
	if err != nil {
		return nil, err
	}
	indexBlockSize := order.Uint64(buf)

	// decode index block.
	buf, err = seekRead(fd, -int64(indexBlockSize+footerSize), indexBlockSize, io.SeekEnd)
	if err != nil {
		return nil, err
	}
	var indexBlock pb.IndexBlock
	if err = proto.Unmarshal(buf, &indexBlock); err != nil {
		return nil, err
	}

	// decode data block.
	var dataBlock pb.DataBlock

	for _, entry := range indexBlock.Entries {
		if bytes.Compare(key, entry.LastKey) <= 0 {
			// read
			buf, err := seekRead(fd, int64(entry.Offset), uint64(entry.Size), io.SeekStart)
			if err != nil {
				return nil, err
			}
			if err = proto.Unmarshal(buf, &dataBlock); err != nil {
				return nil, err
			}
			break
		}
	}

	// binary search.
	i, ok := slices.BinarySearchFunc(dataBlock.Keys, key, func(b1, b2 []byte) int {
		return bytes.Compare(b1, b2)
	})
	if ok {
		return dataBlock.Values[i], nil
	}

	return nil, nil
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
