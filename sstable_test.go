package lsm

import (
	"math"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestSSTable(t *testing.T) {
	assert := assert.New(t)
	m := NewMemTable(math.MaxUint32)
	vmap := map[string]string{}

	// insert
	for i := 0; i < 10000; i++ {
		ts := time.Now().UnixNano()
		k := strconv.Itoa(int(ts))

		vmap[k] = k
		m.Put([]byte(k), []byte(k))
	}

	// dump
	err := os.WriteFile("test.sst", EncodeTable(m), 0644)
	assert.Nil(err)

	// decodeAll
	sst, _ := NewSSTable("test.sst")
	assert.Nil(sst.decodeData())

	for k, v := range vmap {
		res, err := sst.m.Get([]byte(k))
		assert.Nil(err)
		assert.Equal(v, string(res))
	}

	// find
	for k, v := range vmap {
		res, err := sst.findKey([]byte(k))
		assert.Nil(err)
		assert.Equal(v, string(res))
	}

	// error
	for i := 0; i < 10000; i++ {
		ts := time.Now().UnixNano()
		k := strconv.Itoa(int(ts))
		res, err := sst.findKey([]byte(k))
		assert.Nil(err)
		assert.Equal("", string(res))
	}
}

func TestCompact(t *testing.T) {
	assert := assert.New(t)

	m1 := NewMemTable(math.MaxUint32)
	for i := 1000; i < 2000; i++ {
		k := []byte(strconv.Itoa(i))
		m1.Put(k, k)
	}
	assert.Nil(os.WriteFile("m1.sst", EncodeTable(m1), 0644))

	m2 := NewMemTable(math.MaxUint32)
	for i := 3000; i < 4000; i++ {
		k := []byte(strconv.Itoa(i))
		m2.Put(k, k)
	}
	assert.Nil(os.WriteFile("m2.sst", EncodeTable(m2), 0644))

	// load.
	s1, _ := NewSSTable("m1.sst")
	s2, _ := NewSSTable("m2.sst")

	assert.Equal(string(s1.indexBlock.FirstKey), "1000")
	assert.Equal(string(s1.indexBlock.LastKey), "1999")

	assert.Equal(string(s2.indexBlock.FirstKey), "3000")
	assert.Equal(string(s2.indexBlock.LastKey), "3999")

	// merge.
	s1.Merge(s2)
	err := os.WriteFile("m3.sst", EncodeTable(s1.m), 0644)
	assert.Nil(err)

	s3, err := NewSSTable("m3.sst")
	assert.Nil(err)

	assert.Equal(string(s3.indexBlock.FirstKey), "1000")
	assert.Equal(string(s3.indexBlock.LastKey), "3999")

	// find.
	for i := 1000; i < 2000; i++ {
		k := []byte(strconv.Itoa(i))
		res, err := s3.findKey(k)
		assert.Nil(err)
		assert.Equal(string(k), string(res))
	}
	for i := 3000; i < 4000; i++ {
		k := []byte(strconv.Itoa(i))
		res, err := s3.findKey(k)
		assert.Nil(err)
		assert.Equal(string(k), string(res))
	}
}
