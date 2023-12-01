package lsm

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"io"
	"os"
	"slices"

	"github.com/andy-kimball/arenaskl"
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

// TableDecoder
type TableDecoder struct {
	fd         *os.File
	indexBlock pb.IndexBlock
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
// DumpTable dumps a memtable to a sstable.
func DumpTable(mt *MemTable) []byte {
	buf := bytes.NewBuffer(make([]byte, 0, DataBlockSize))

	dataBlock := new(pb.DataBlock)
	indexBlocks := make([]*pb.IndexBlockEntry, 0)

	// iter memtable.
	mt.it.SeekToFirst()
	for {
		dataBlock.Keys = append(dataBlock.Keys, mt.it.Key())
		dataBlock.Values = append(dataBlock.Values, mt.it.Value())
		dataBlock.Types = append(dataBlock.Types, byte(mt.it.Meta()))
		dataBlock.Size += uint32(len(mt.it.Key()) + len(mt.it.Value()) + 1)

		mt.it.Next()

		// encode data blocks.
		if dataBlock.Size >= DataBlockSize || !mt.it.Valid() {
			src, _ := proto.Marshal(dataBlock)
			// compress.
			dst := Compress(src, nil)

			indexBlocks = append(indexBlocks, &pb.IndexBlockEntry{
				LastKey: dataBlock.Keys[len(dataBlock.Keys)-1],
				Offset:  uint32(buf.Len()),
				Size:    uint32(len(dst)),
			})
			buf.Write(dst)

			// reset data block.
			dataBlock.Keys = dataBlock.Keys[:0]
			dataBlock.Values = dataBlock.Values[:0]
			dataBlock.Types = dataBlock.Types[:0]

			// break if end.
			if !mt.it.Valid() {
				break
			}
		}
	}

	// encode index block.
	data, _ := proto.Marshal(&pb.IndexBlock{Entries: indexBlocks})
	buf.Write(data)

	// encode footer.
	binary.Write(buf, order, uint64(len(data)))
	binary.Write(buf, order, crc32.ChecksumIEEE(data))

	return buf.Bytes()
}

// FindTable
func FindTable(key []byte, path string) ([]byte, error) {
	decoder, err := NewTableDecoder(path)
	if err != nil {
		return nil, err
	}
	defer decoder.Close()

	if err = decoder.decodeIndexBlock(); err != nil {
		return nil, err
	}
	return decoder.findKey(key)
}

// NewTableDecoder
func NewTableDecoder(path string) (*TableDecoder, error) {
	fd, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	return &TableDecoder{fd: fd}, nil
}

// Close
func (s *TableDecoder) Close() error {
	return s.fd.Close()
}

// decodeIndexBlock
func (s *TableDecoder) decodeIndexBlock() error {
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

// findKey
func (s *TableDecoder) findKey(key []byte) ([]byte, error) {
	var dataBlock pb.DataBlock

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
			if err = proto.Unmarshal(dst, &dataBlock); err != nil {
				return nil, err
			}
			break
		}
	}

	// binary search.
	i, ok := slices.BinarySearchFunc(dataBlock.Keys, key, func(a, b []byte) int {
		return bytes.Compare(a, b)
	})
	if ok {
		return dataBlock.Values[i], nil
	}

	return nil, nil
}

// decodeAll
func (s *TableDecoder) decodeAll() (*arenaskl.Iterator, error) {
	// create arena skiplist.
	skl := arenaskl.NewSkiplist(arenaskl.NewArena(MemTableSize))
	var it arenaskl.Iterator
	it.Init(skl)

	var dataBlock pb.DataBlock

	for _, entry := range s.indexBlock.Entries {
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

		if err = proto.Unmarshal(dst, &dataBlock); err != nil {
			return nil, err
		}

		// insert
		for i, key := range dataBlock.Keys {
			it.Add(key, dataBlock.Values[i], uint16(dataBlock.Types[i]))
		}
	}
	return &it, nil
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
