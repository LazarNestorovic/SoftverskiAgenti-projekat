package crdt

import (
	"encoding/json"
	"sync"
)

type GSet[T comparable] struct {
	mu  sync.RWMutex
	set map[T]struct{}
}

func NewGSet[T comparable]() *GSet[T] {
	return &GSet[T]{
		set: make(map[T]struct{}),
	}
}

func (g *GSet[T]) Add(el T) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.set[el] = struct{}{}
}

func (g *GSet[T]) Contains(el T) bool {
	g.mu.RLock()
	defer g.mu.RUnlock()
	_, ok := g.set[el]
	return ok
}

func (g *GSet[T]) Members() []T {
	g.mu.RLock()
	defer g.mu.RUnlock()
	temp := make([]T, 0, len(g.set))
	for key := range g.set {
		temp = append(temp, key)
	}
	return temp
}

func (g *GSet[T]) Size() int {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return len(g.set)
}

func (g *GSet[T]) Merge(other *GSet[T]) {
	other.mu.RLock()
	snap := make([]T, 0, len(other.set))
	for key := range other.set {
		snap = append(snap, key)
	}
	other.mu.RUnlock()

	g.mu.Lock()
	defer g.mu.Unlock()
	for _, key := range snap {
		g.set[key] = struct{}{}
	}
}

func (g *GSet[T]) MarshalJSON() ([]byte, error) {
	g.mu.RLock()
	defer g.mu.RUnlock()
	elems := make([]T, 0, len(g.set))
	for elem := range g.set {
		elems = append(elems, elem)
	}
	return json.Marshal(elems)
}

func (g *GSet[T]) UnmarshalAndMerge(data []byte) error {
	var elems []T
	if err := json.Unmarshal(data, &elems); err != nil {
		return err
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	for _, elem := range elems {
		g.set[elem] = struct{}{}
	}
	return nil
}

func (g *GSet[T]) Type() string { return "GSet" }
