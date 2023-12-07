package refmap

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRefCounter(t *testing.T) {
	assert := assert.New(t)
	ref := New()

	for i := 0; i < 100; i++ {
		ref.Incr(1, "a")
		n, ok := ref.Get("a")
		assert.True(ok)
		assert.Equal(i+1, n)
	}

	for i := 0; i < 100; i++ {
		if i < 99 {
			ref.Incr(-1, "a")
			n, ok := ref.Get("a")

			assert.True(ok)
			assert.Equal(99-i, n)

		} else {
			ref.Incr(-1, "a")
			n, ok := ref.Get("a")

			assert.True(ok)
			assert.Equal(0, n)
		}
	}

	ref = New()
	ref.Incr(1, "a")
	ref.Incr(-1, "a")
	ref.Incr(1, "b")
	ref.Incr(1, "c")
	ref.Incr(-1, "c")

	ref.DelZero(func(s string) {
		if s == "a" || s == "c" {
		} else {
			assert.Fail("should not happen")
		}
	})
}
