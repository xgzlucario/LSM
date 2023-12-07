package refmap

import (
	"sync"
)

// Map
type Map struct {
	sync.RWMutex
	m map[string]int
}

// New
func New() *Map {
	return &Map{m: make(map[string]int)}
}

// Incr
func (m *Map) Incr(delta int, keys ...string) {
	m.Lock()
	defer m.Unlock()

	for _, key := range keys {
		n, ok := m.m[key]
		if ok {
			m.m[key] = n + delta
		} else {
			m.m[key] = delta
		}
	}
}

// Get
func (m *Map) Get(key string) (int, bool) {
	m.RLock()
	defer m.RUnlock()

	ref, ok := m.m[key]

	return ref, ok
}

// DelZero
func (m *Map) DelZero(cb func(string)) {
	m.Lock()
	defer m.Unlock()

	for k, v := range m.m {
		if v == 0 {
			cb(k)
			delete(m.m, k)
		}
	}
}
