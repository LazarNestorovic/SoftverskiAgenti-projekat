package crdt

import (
	"encoding/json"
	"sync"
	"time"
)

type LWWRegister[T any] struct {
	mu        sync.RWMutex
	value     T
	timestamp int64
}

type lwwState[T any] struct {
	Value     T     `json:"value"`
	Timestamp int64 `json:"timestamp"`
}

func NewLWWRegister[T any]() *LWWRegister[T] {
	return &LWWRegister[T]{
		timestamp: 0,
	}
}

func (l *LWWRegister[T]) Set(value T) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.value = value
	l.timestamp = time.Now().UnixNano()
}

func (l *LWWRegister[T]) Get() T {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.value
}

func (l *LWWRegister[T]) Timestamp() int64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.timestamp
}

func (l *LWWRegister[T]) Merge(other *LWWRegister[T]) {
	other.mu.RLock()
	otherVal := other.value
	otherTime := other.timestamp
	other.mu.RUnlock()

	l.mu.Lock()
	defer l.mu.Unlock()
	if otherTime > l.timestamp {
		l.value = otherVal
		l.timestamp = otherTime
	}
}

func (l *LWWRegister[T]) MarshalJSON() ([]byte, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	state := lwwState[T]{l.value, l.timestamp}
	return json.Marshal(state)
}

func (l *LWWRegister[T]) UnmarshalAndMerge(data []byte) error {
	var other lwwState[T]
	if err := json.Unmarshal(data, &other); err != nil {
		return err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.timestamp < other.Timestamp {
		l.value = other.Value
		l.timestamp = other.Timestamp
	}
	return nil
}

func (l *LWWRegister[T]) Type() string { return "LWWRegister" }
