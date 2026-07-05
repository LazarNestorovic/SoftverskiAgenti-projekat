package crdt

import (
	"encoding/json"
	"sync"

	"github.com/google/uuid"
)

type ORSet[T comparable] struct {
	mu      sync.RWMutex
	added   map[T]map[string]struct{}
	removed map[string]struct{}
}

func NewORSet[T comparable]() *ORSet[T] {
	return &ORSet[T]{
		added:   make(map[T]map[string]struct{}),
		removed: make(map[string]struct{}),
	}
}

func (o *ORSet[T]) Add(el T) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.added[el] == nil {
		o.added[el] = make(map[string]struct{})
	}
	o.added[el][uuid.New().String()] = struct{}{}
}

func (s *ORSet[T]) Remove(elem T) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for tag := range s.added[elem] {
		s.removed[tag] = struct{}{}
	}
}

func (o *ORSet[T]) Contains(el T) bool {
	o.mu.RLock()
	defer o.mu.RUnlock()
	ids, _ := o.added[el]
	for key := range ids {
		if _, exists := o.removed[key]; !exists {
			return true
		}
	}
	return false
}

func (o *ORSet[T]) Size() int {
	o.mu.RLock()
	defer o.mu.RUnlock()
	size := 0
	for element := range o.added {
		if o.containsLocked(element) {
			size++
		}
	}
	return size
}

func (o *ORSet[T]) containsLocked(el T) bool {
	for tag := range o.added[el] {
		if _, removed := o.removed[tag]; !removed {
			return true
		}
	}
	return false
}

func (o *ORSet[T]) Merge(other *ORSet[T]) {
	other.mu.RLock()
	snapAdded := make(map[T]map[string]struct{})
	for elem, tags := range other.added {
		cp := make(map[string]struct{})
		for tag := range tags {
			cp[tag] = struct{}{}
		}
		snapAdded[elem] = cp
	}
	snapRemoved := make(map[string]struct{})
	for tag := range other.removed {
		snapRemoved[tag] = struct{}{}
	}
	other.mu.RUnlock()

	o.mu.Lock()
	defer o.mu.Unlock()
	for elem, tags := range snapAdded {
		if o.added[elem] == nil {
			o.added[elem] = make(map[string]struct{})
		}
		for tag := range tags {
			o.added[elem][tag] = struct{}{}
		}
	}
	for tag := range snapRemoved {
		o.removed[tag] = struct{}{}
	}
}

type orTaggedElem[T any] struct {
	Elem T        `json:"elem"`
	Tags []string `json:"tags"`
}

type orSetState[T any] struct {
	Added   []orTaggedElem[T] `json:"added"`
	Removed []string          `json:"removed"`
}

func (o *ORSet[T]) MarshalJSON() ([]byte, error) {
	o.mu.RLock()
	defer o.mu.RUnlock()
	added := make([]orTaggedElem[T], 0, len(o.added))
	removed := make([]string, 0, len(o.removed))
	for key, value := range o.added {
		tags := []string{}
		for keyTag, _ := range value {
			tags = append(tags, keyTag)
		}
		tagElem := orTaggedElem[T]{Elem: key, Tags: tags}
		added = append(added, tagElem)
	}
	for key, _ := range o.removed {
		removed = append(removed, key)
	}
	state := orSetState[T]{added, removed}
	return json.Marshal(state)
}

func (o *ORSet[T]) UnmarshalAndMerge(data []byte) error {
	var other orSetState[T]
	if err := json.Unmarshal(data, &other); err != nil {
		return err
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	for _, te := range other.Added {
		if o.added[te.Elem] == nil {
			o.added[te.Elem] = make(map[string]struct{})
		}
		for _, tag := range te.Tags {
			o.added[te.Elem][tag] = struct{}{}
		}
	}
	for _, tag := range other.Removed {
		o.removed[tag] = struct{}{}
	}
	return nil
}

func (o *ORSet[T]) Type() string { return "ORSet" }
