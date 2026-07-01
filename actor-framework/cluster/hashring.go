package cluster

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"sort"
	"sync"
)

type HashRing struct {
	mu           sync.RWMutex
	ring         map[uint32]NodeID
	sortedHashes []uint32
	replicas     int
}

func NewHashRing(replicas int) *HashRing {
	return &HashRing{
		ring:     make(map[uint32]NodeID),
		replicas: replicas,
	}
}

func (r *HashRing) hash(key string) uint32 {
	h := sha256.Sum256([]byte(key))
	return binary.BigEndian.Uint32(h[:4])
}

func (r *HashRing) Add(nodeID NodeID) {
	defer r.mu.Unlock()
	r.mu.Lock()

	if _, exists := r.ring[r.hash(fmt.Sprintf("%s#0", nodeID))]; exists {
		return // već dodat
	}

	for i := 0; i < r.replicas; i++ {
		virtualKey := fmt.Sprintf("%s#%d", nodeID, i)
		hashKey := r.hash(virtualKey)
		r.ring[hashKey] = nodeID
		r.sortedHashes = append(r.sortedHashes, hashKey)
	}
	sort.Slice(r.sortedHashes, func(i, j int) bool {
		return r.sortedHashes[i] < r.sortedHashes[j]
	})
}

func (r *HashRing) Remove(nodeID NodeID) {
	defer r.mu.Unlock()
	r.mu.Lock()
	for i := 0; i < r.replicas; i++ {
		virtualKey := fmt.Sprintf("%s#%d", nodeID, i)
		hashKey := r.hash(virtualKey)
		delete(r.ring, hashKey)
	}

	filtered := r.sortedHashes[:0]
	for _, hashKey := range r.sortedHashes {
		if _, exists := r.ring[hashKey]; exists {
			filtered = append(filtered, hashKey)
		}
	}
	r.sortedHashes = filtered
}

func (r *HashRing) Get(key string) NodeID {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.sortedHashes) == 0 {
		return ""
	}

	hashKey := r.hash(key)
	idx := sort.Search(len(r.sortedHashes), func(i int) bool {
		return r.sortedHashes[i] >= hashKey
	})

	if idx == len(r.sortedHashes) {
		return r.ring[r.sortedHashes[0]]
	} else {
		return r.ring[r.sortedHashes[idx]]
	}
}
