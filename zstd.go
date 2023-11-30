package lsm

import "github.com/klauspost/compress/zstd"

var (
	encoder, _ = zstd.NewWriter(
		nil,
		zstd.WithEncoderLevel(zstd.SpeedFastest),
		zstd.WithEncoderCRC(true),
	)
	decoder, _ = zstd.NewReader(nil)
)

// Compress with zstd algorithm.
func Compress(src, dst []byte) []byte {
	return encoder.EncodeAll(src, dst)
}

// Decompress with zstd algorithm.
func Decompress(src, dst []byte) ([]byte, error) {
	return decoder.DecodeAll(src, dst)
}
