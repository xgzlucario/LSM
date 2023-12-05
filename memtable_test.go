package lsm

import (
	"bytes"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMerge(t *testing.T) {
	assert := assert.New(t)
	m1 := NewMemTable()

	for i := 1000; i < 5000; i++ {
		k := []byte(strconv.Itoa(i))
		m1.Put(k, k)
	}

	m2 := NewMemTable()
	for i := 2000; i < 6000; i++ {
		k := []byte(strconv.Itoa(i))
		m2.Put(k, k)
	}

	m1.Merge(m2)
	assert.Equal("1000", string(m1.FirstKey()))
	assert.Equal("5999", string(m1.LastKey()))

	m1.Iter(func(key, value []byte, vtype uint16) {
		assert.Equal(key, value)
	})

	// Update
	m3 := NewMemTable()
	for i := 5000; i < 7000; i++ {
		k := []byte(strconv.Itoa(i))
		v := []byte("value" + strconv.Itoa(i))
		m3.Put(k, v)
	}
	m1.Merge(m3)
	assert.Equal("1000", string(m1.FirstKey()))
	assert.Equal("6999", string(m1.LastKey()))

	m1.Iter(func(key, value []byte, vtype uint16) {
		if bytes.Compare(key, []byte("5000")) >= 0 && bytes.Compare(key, []byte("7000")) < 0 {
			assert.Equal("value"+string(key), string(value))
		} else {
			assert.Equal(string(key), string(value))
		}
	})
}
