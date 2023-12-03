package lsm

import "sync"

// RefCounter
type RefCounter struct {
	mu sync.RWMutex
	m  map[string]uint32
}

// NewRefCounter
func NewRefCounter() *RefCounter {
	return &RefCounter{m: make(map[string]uint32)}
}

// AddRef
func (l *RefCounter) AddRef(key string) {
	l.mu.Lock()
	defer l.mu.Unlock()

	n, ok := l.m[key]
	if !ok {
		l.m[key] = 1

	} else {
		l.m[key] = n + 1
	}
}

// DelRef
func (l *RefCounter) DelRef(key string, onZeroRef func()) {
	l.mu.Lock()
	defer l.mu.Unlock()

	n, ok := l.m[key]
	if !ok {
		return
	}

	if n == 1 {
		delete(l.m, key)
		onZeroRef()

	} else {
		l.m[key] = n - 1
	}
}

// GetRef
func (l *RefCounter) GetRef(key string) (uint32, bool) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	ref, ok := l.m[key]

	return ref, ok
}
