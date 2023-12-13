package memdb

import (
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/andy-kimball/arenaskl"
	"github.com/stretchr/testify/assert"
)

const (
	testMemDBSize = 4 * 1 << 20 // 4 MB
)

var (
	nilBytes []byte
)

func getKey(i int) []byte {
	return []byte(fmt.Sprintf("%08d", i))
}

func getMemDB(start, end int) *DB {
	m := New(testMemDBSize)
	for i := start; i < end; i++ {
		k := getKey(i)
		m.Put(k, k, typeVal)
	}
	return m
}

func checkData(m *DB, start, end int, assert *assert.Assertions) {
	// check minKey and maxKey.
	minKey := fmt.Sprintf("%08d", start)
	maxKey := fmt.Sprintf("%08d", end-1)
	assert.Equal(minKey, string(m.MinKey()))
	assert.Equal(maxKey, string(m.MaxKey()))

	// check 0-start.
	for i := 0; i < start; i++ {
		k := getKey(i)
		value, ok := m.Get(k)
		assert.Equal(nilBytes, value)
		assert.False(ok)
	}

	// check start-end.
	for i := start; i < end; i++ {
		k := getKey(i)
		value, ok := m.Get(k)
		assert.Equal(k, value)
		assert.True(ok)
	}

	// check end-end*2.
	for i := end; i < end*2; i++ {
		k := getKey(i)
		value, ok := m.Get(k)
		assert.Equal(nilBytes, value)
		assert.False(ok)
	}
}

func TestGet(t *testing.T) {
	assert := assert.New(t)
	m := getMemDB(0, 10000)

	// check cap.
	assert.Equal(uint32(testMemDBSize), m.Capacity())

	for i := 0; i < 20000; i++ {
		k := getKey(i)
		value, ok := m.Get(k)

		if i < 10000 {
			assert.Equal(k, value)
			assert.True(ok)
		} else {
			assert.Equal(nilBytes, value)
			assert.False(ok)
		}
	}
}

func TestPutIfFull(t *testing.T) {
	assert := assert.New(t)
	m := New(1024)

	// check cap.
	assert.Equal(uint32(1024), m.Capacity())

	// ok.
	for i := 0; i < 10; i++ {
		k := []byte(strconv.Itoa(i))
		full, err := m.PutIsFull(k, k, typeVal)
		assert.False(full)
		assert.Nil(err)
	}

	// overflow.
	for i := 0; i < 100; i++ {
		k := []byte(strings.Repeat(strconv.Itoa(i), 1024))
		full, err := m.PutIsFull(k, k, typeVal)
		assert.True(full)
		assert.Equal(err, arenaskl.ErrArenaFull)
	}
}

func TestMerge(t *testing.T) {
	assert := assert.New(t)
	{
		m1 := getMemDB(0, 10000)
		m2 := getMemDB(10000, 20000)
		m1 = Merge(m1, m2)
		// check cap.
		assert.Equal(uint32(testMemDBSize*2), m1.Capacity())
		// check data.
		checkData(m1, 0, 20000, assert)
	}
	{
		m1 := getMemDB(0, 15000)
		m2 := getMemDB(10000, 20000)
		m1 = Merge(m1, m2)
		// check cap.
		assert.Equal(uint32(testMemDBSize*2), m1.Capacity())
		// check data.
		checkData(m1, 0, 20000, assert)
	}
}
