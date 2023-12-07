package table

import "github.com/klauspost/compress/zstd"

var (
	encoder, _ = zstd.NewWriter(
		nil,
		zstd.WithEncoderLevel(zstd.SpeedFastest),
		zstd.WithEncoderCRC(true),
	)
	decoder, _ = zstd.NewReader(nil)
)

func compress(src, dst []byte) []byte {
	return encoder.EncodeAll(src, dst)
}

func decompress(src, dst []byte) ([]byte, error) {
	return decoder.DecodeAll(src, dst)
}
