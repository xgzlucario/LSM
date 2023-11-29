package lsm

import (
	"bytes"
	"encoding/binary"

	"github.com/xgzlucario/LSM/pb"
	"google.golang.org/protobuf/proto"
)

/*
   LevelDB SSTable format:
                            +------------+
	+-----------------+     |  entry[0]  | --> (shared_bytes, unshared_bytes, value_len, key_delta, value)
	|  data_block[0]  | --> +------------+
	+-----------------+     |  entry[1]  |
	|  data_block[1]  |     +------------+
	+-----------------+     |  ... ...   |
	|     ... ...     |     +------------+
	+-----------------+
    |     (filter)    |
	+-----------------+
	|                 |     +-------------+-------------+-------------+----------------+
    |    meta_index   | --> |  restarts0  |  restarts1  |   ... ...   |  restarts_len  |
	|                 |     +-------------+-------------+-------------+----------------+
    +-----------------+     +------------+
    |                 |     |  entry[0]  | --one entry per block--> (last_key, offset, size)
	|   index_block   | --> +------------+
	|                 |     |  entry[1]  |
	+-----------------+     +------------+
    |     footer      |     |  ... ...   |
    +-----------------+	    +------------+

	Get(key) -> footer -> index_block -> BlockHandle(offset, size) -> data_block -> entry(key, value)
*/

type vtype byte

const (
	vtypeVal vtype = iota + 1
	vtypeDel
)

// SSTable
type SSTable struct {
	*Config
	*MemTable
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

		// encode data block.
		if dataBlock.Size >= s.DataBlockSize || !s.it.Valid() {
			src, _ := proto.Marshal(dataBlock)
			// TODO zstd compress here.
			indexBlocks = append(indexBlocks, &pb.IndexBlockEntry{
				LastKey: dataBlock.Keys[len(dataBlock.Keys)-1],
				Offset:  uint32(buf.Len()),
				Size:    uint32(len(src)),
			})
			buf.Write(src)

			// break if invalid.
			if !s.it.Valid() {
				break
			}
		}
	}

	// encode index blocks.
	indexSrc, _ := proto.Marshal(&pb.IndexBlock{Entries: indexBlocks})

	// encode footer.
	indexBlockOffset := uint64(buf.Len())
	IndexBlockSize := uint64(len(indexSrc))

	buf.Write(indexSrc)

	binary.Write(buf, binary.LittleEndian, indexBlockOffset)
	binary.Write(buf, binary.LittleEndian, IndexBlockSize)

	return buf.Bytes()
}

// FindSSTable
func FindSSTable(key []byte, path string) ([]byte, error) {
	return nil, nil
}
