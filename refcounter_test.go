package lsm

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRefCounter(t *testing.T) {
	assert := assert.New(t)
	l := NewRefCounter()

	for i := 0; i < 100; i++ {
		l.Incr(1, "a")
		n, ok := l.Get("a")
		assert.True(ok)
		assert.Equal(i+1, n)
	}

	for i := 0; i < 100; i++ {
		if i < 99 {
			l.Incr(-1, "a")
			n, ok := l.Get("a")

			assert.True(ok)
			assert.Equal(99-i, n)

		} else {
			l.Incr(-1, "a")
			n, ok := l.Get("a")

			assert.True(ok)
			assert.Equal(0, n)
		}
	}

	l = NewRefCounter()
	l.Incr(1, "a")
	l.Incr(-1, "a")
	l.Incr(1, "b")
	l.Incr(1, "c")
	l.Incr(-1, "c")

	l.DelZero(func(s string) {
		if s == "a" || s == "c" {
		} else {
			assert.Fail("should not happen")
		}
	})
}
