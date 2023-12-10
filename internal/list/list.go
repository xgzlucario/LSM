package list

// SortedList is a sorted list.
type SortedList[T any] struct {
	data []T
	cmp  func(T, T) int
}

// New
func New[T any](cmp func(T, T) int) *SortedList[T] {
	return &SortedList[T]{
		data: make([]T, 0),
	}
}

// Insert
func (l *SortedList[T]) Insert(v T) {
	for i, d := range l.data {
		if l.cmp(v, d) < 0 {
			l.data = append(l.data, v)
			copy(l.data[i+1:], l.data[i:])
			l.data[i] = v
			return
		}
	}
	l.data = append(l.data, v)
}

func (l *SortedList[T]) Len() int {
	return len(l.data)
}
