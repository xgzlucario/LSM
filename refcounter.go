package lsm

import (
	"sync"
)

// RefCounter
type RefCounter struct {
	mu sync.RWMutex
	m  map[string]int
}

// NewRefCounter
func NewRefCounter() *RefCounter {
	return &RefCounter{m: make(map[string]int)}
}

// Incr
func (l *RefCounter) Incr(delta int, keys ...string) {
	l.mu.Lock()
	defer l.mu.Unlock()

	for _, key := range keys {
		n, ok := l.m[key]
		if ok {
			l.m[key] = n + delta
		} else {
			l.m[key] = delta
		}
	}
}

// GetRef
func (l *RefCounter) Get(key string) (int, bool) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	ref, ok := l.m[key]

	return ref, ok
}

// DelZero
func (l *RefCounter) DelZero(cb func(string)) {
	l.mu.Lock()
	defer l.mu.Unlock()

	for k, v := range l.m {
		if v == 0 {
			cb(k)
			delete(l.m, k)
		}
	}
}
