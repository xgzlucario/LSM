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
