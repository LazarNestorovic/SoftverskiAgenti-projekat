package crdt

import (
	"encoding/json"
	"sync"
)

type GCounter struct {
	mu      sync.RWMutex
	counts  map[string]uint64
	localID string
}

type CRDTMerge struct {
	CRDTType string `json:"crdt_type"`
	Payload  []byte `json:"payload"`
}

func NewGCounter(nodeID string) *GCounter {
	return &GCounter{
		counts:  make(map[string]uint64),
		localID: nodeID,
	}
}

func (g *GCounter) Increment() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.counts[g.localID]++
}

func (g *GCounter) IncrementBy(n uint64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.counts[g.localID] += n
}

func (g *GCounter) Value() uint64 {
	g.mu.RLock()
	defer g.mu.RUnlock()
	globalSum := uint64(0)
	for _, value := range g.counts {
		globalSum += value
	}
	return globalSum
}

func (g *GCounter) LocalValue() uint64 {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.counts[g.localID]
}

func (g *GCounter) Merge(other *GCounter) {
	other.mu.RLock()
	temp := make(map[string]uint64, len(other.counts))
	for k, v := range other.counts {
		temp[k] = v
	}
	other.mu.RUnlock()

	g.mu.Lock()
	defer g.mu.Unlock()
	for id, value := range temp { // ← iteriraj kroz temp, ne g.counts
		if value > g.counts[id] {
			g.counts[id] = value
		}
	}
}

func (c *GCounter) MarshalJSON() ([]byte, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return json.Marshal(c.counts)
}

func (c *GCounter) UnmarshalAndMerge(data []byte) error {
	var other map[string]uint64
	if err := json.Unmarshal(data, &other); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for nodeID, val := range other {
		if val > c.counts[nodeID] {
			c.counts[nodeID] = val
		}
	}
	return nil
}

func (c *GCounter) Type() string { return "GCounter" }
