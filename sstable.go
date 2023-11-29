package lsm

import (
	"github.com/xgzlucario/LSM/pb"
	"google.golang.org/protobuf/proto"
)

/*
      SSTable format:
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
	*MemTable
}

// DumpTable
func (s *SSTable) DumpTable() []byte {
	entry := &pb.DataBlockEntry{}

	s.it.SeekToFirst()
	for s.it.Valid() {
		entry.Keys = append(entry.Keys, s.it.Key())
		entry.Values = append(entry.Values, s.it.Value())
		entry.Types = append(entry.Types, byte(s.it.Meta()))
		s.it.Next()
	}

	// encode data block.
	dataSrc, _ := proto.Marshal(&pb.DataBlock{
		Entries: []*pb.DataBlockEntry{entry},
	})

	// encode index block.
	indexSrc, _ := proto.Marshal(&pb.IndexBlock{
		Entries: []*pb.IndexBlockEntry{
			{
				LastKey: s.it.Key(),
				Offset:  0,
				Size:    uint32(len(dataSrc)),
			},
		},
	})

	// encode footer.
	footerSrc, _ := proto.Marshal(&pb.Footer{
		DataBlockOffset:  0,
		DataBlockSize:    uint64(len(dataSrc)),
		IndexBlockOffset: uint64(len(dataSrc)),
		IndexBlockSize:   uint64(len(indexSrc)),
	})

	dataSrc = append(dataSrc, indexSrc...)
	dataSrc = append(dataSrc, footerSrc...)

	return dataSrc
}
