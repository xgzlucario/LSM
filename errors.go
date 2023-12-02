package lsm

import "errors"

var (
	ErrKeyNotFound = errors.New("key not found")
	ErrCRCChecksum = errors.New("crc checksum error")
)
