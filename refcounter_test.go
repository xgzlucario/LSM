package lsm

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRefCounter(t *testing.T) {
	assert := assert.New(t)
	l := NewRefCounter()

	for i := 0; i < 100; i++ {
		l.AddRef("a")
		n, ok := l.GetRef("a")
		assert.True(ok)
		assert.Equal(uint32(i+1), n)
	}

	for i := 0; i < 100; i++ {
		var done bool
		if i < 99 {
			l.DelRef("a", func() { done = true })
			n, ok := l.GetRef("a")

			assert.True(ok)
			assert.Equal(uint32(99-i), n)
			assert.False(done)

		} else {
			l.DelRef("a", func() { done = true })
			n, ok := l.GetRef("a")

			assert.False(ok)
			assert.Equal(uint32(0), n)
			assert.True(done)
		}
	}
}
