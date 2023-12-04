package lsm

import "errors"

var (
	ErrInputToLarge = errors.New("input is too large than MaxUint16")
	ErrKeyNotFound  = errors.New("key not found")
	ErrCRCChecksum  = errors.New("crc checksum error")
)
