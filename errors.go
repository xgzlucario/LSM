package lsm

import "errors"

var (
	ErrWriteStopped = errors.New("too many im tables im memory, stop writting")

	ErrInputToLarge = errors.New("input is too large than MaxUint16")

	ErrKeyNotFound = errors.New("key not found")

	ErrCRCChecksum = errors.New("crc checksum error")
)
