package bcmp

import (
	"bytes"
	"encoding/binary"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestCompare(t *testing.T) {
	assert := assert.New(t)

	const num = 10 * 10000
	testData := make([][]byte, 0, num)
	source := rand.NewSource(time.Now().UnixNano())

	// gen random data.
	for i := 0; i < num; i++ {
		rnum := uint64(source.Int63())
		bytes := make([]byte, 8)
		binary.BigEndian.PutUint64(bytes, rnum)
		testData = append(testData, bytes)
	}

	// Test
	for i := 1; i < len(testData); i++ {
		a := testData[i-1]
		b := testData[i]

		assert.Equal(bytes.Compare(a, b), Compare(a, b))
		assert.Equal(bytes.Compare(a, b) < 0, Less(a, b))
		assert.Equal(bytes.Compare(a, b) <= 0, LessEqual(a, b))
		assert.Equal(bytes.Equal(a, b), Equal(a, b))
		assert.Equal(bytes.Compare(a, b) >= 0, GreatEqual(a, b))
		assert.Equal(bytes.Compare(a, b) > 0, Great(a, b))

		_min := Min(a, b)
		assert.True(LessEqual(_min, a))
		assert.True(LessEqual(_min, b))

		_max := Max(a, b)
		assert.True(GreatEqual(_max, a))
		assert.True(GreatEqual(_max, b))

		target := []byte{100, 101, 102}
		assert.Equal(
			bytes.Compare(a, target) <= 0 && bytes.Compare(target, b) <= 0,
			Between(target, a, b),
		)
	}
}

func TestMergeInterval(t *testing.T) {
	assert := assert.New(t)

	// 1
	testData := []Interval{
		{Min: []byte{1}, Max: []byte{2}},
		{Min: []byte{3}, Max: []byte{4}},
		{Min: []byte{5}, Max: []byte{6}},
	}
	assert.Equal(testData, MergeInterval(testData))
	assert.Equal([][]int{{0}, {1}, {2}}, MergeIntervalIndex(testData))

	// 2
	testData = []Interval{
		{Min: []byte{1}, Max: []byte{3}},
		{Min: []byte{4}, Max: []byte{6}},
		{Min: []byte{5}, Max: []byte{8}},
	}
	assert.Equal([]Interval{
		{Min: []byte{1}, Max: []byte{3}},
		{Min: []byte{4}, Max: []byte{8}},
	}, MergeInterval(testData))
	assert.Equal([][]int{{0}, {1, 2}}, MergeIntervalIndex(testData))

	// 3
	testData = []Interval{
		{Min: []byte{1}, Max: []byte{3}},
		{Min: []byte{2}, Max: []byte{6}},
		{Min: []byte{7}, Max: []byte{8}},
	}
	assert.Equal([]Interval{
		{Min: []byte{1}, Max: []byte{6}},
		{Min: []byte{7}, Max: []byte{8}},
	}, MergeInterval(testData))
	assert.Equal([][]int{{0, 1}, {2}}, MergeIntervalIndex(testData))

	// 4
	testData = []Interval{
		{Min: []byte{1}, Max: []byte{5}},
		{Min: []byte{2}, Max: []byte{7}},
		{Min: []byte{6}, Max: []byte{8}},
	}
	assert.Equal([]Interval{
		{Min: []byte{1}, Max: []byte{8}},
	}, MergeInterval(testData))
	assert.Equal([][]int{{0, 1, 2}}, MergeIntervalIndex(testData))
}
