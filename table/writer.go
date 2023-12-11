package table

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"

	"github.com/xgzlucario/LSM/memdb"
	"github.com/xgzlucario/LSM/option"
	"github.com/xgzlucario/LSM/pb"
	"google.golang.org/protobuf/proto"
)

// Writer
type Writer struct {
	*option.Option
}

// NewWriter
func NewWriter(opt *option.Option) *Writer {
	return &Writer{opt}
}

// Marshal
func (w *Writer) Marshal(level int, id uint64, db *memdb.DB) []byte {
	buf := bytes.NewBuffer(make([]byte, 0, db.Capacity()))
	var size, length uint32

	// initial.
	dataBlock := new(pb.DataBlock)
	indexBlock := &pb.IndexBlock{
		FirstKey: db.FirstKey(),
		LastKey:  db.LastKey(),
	}

	// encode data block function.
	encodeDataBlock := func() {
		src, _ := proto.Marshal(dataBlock)
		dst := compress(src)

		indexBlock.Entries = append(indexBlock.Entries, &pb.IndexBlockEntry{
			LastKey: dataBlock.Keys[len(dataBlock.Keys)-1],
			Offset:  uint32(buf.Len()),
			Size:    uint32(len(dst)),
			Length:  length,
		})
		buf.Write(dst)

		dataBlock.Reset()
		size = 0
		length = 0
	}

	db.Iter(func(key, value []byte, meta uint16) {
		dataBlock.Keys = append(dataBlock.Keys, key)
		dataBlock.Values = append(dataBlock.Values, value)
		dataBlock.Types = append(dataBlock.Types, byte(meta))

		length++
		size += uint32(len(key) + len(value) + 2)

		// when reach the threshold, generate a new data block.
		if size >= w.DataBlockSize {
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
		Level:          byte(level),
		IndexBlockSize: uint32(len(data)),
		CRC:            crc32.ChecksumIEEE(data),
		Id:             id,
		MagicNumber:    magicNumber,
	})

	return buf.Bytes()
}
