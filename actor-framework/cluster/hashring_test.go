package cluster

import (
	"fmt"
	"testing"
)

func TestHashRing_AddAndGetNodes(t *testing.T) {
	hashRing := NewHashRing(10)

	hashRing.Add("node1")

	result := hashRing.Get("some-actor-id")
	if result != "node1" {
		t.Errorf("očekivano node1, dobijeno %s", result)
	}
}

func TestHashRing_RemoveNode(t *testing.T) {
	hashRing := NewHashRing(10)

	hashRing.Add("node1")

	hashRing.Remove("node1")

	if hashRing.Get("node1") != "" {
		t.Errorf("Node nije pravilno uklonjen iz hash ring-a")
	}
}

func TestHashRing_Distribution(t *testing.T) {
	hashRing := NewHashRing(100)
	hashRing.Add("node1")
	hashRing.Add("node2")

	// Sa dva čvora i 100 replika, različiti ključevi treba da idu na oba čvora
	results := make(map[NodeID]int)
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("actor-%d", i)
		node := hashRing.Get(key)
		results[node]++
	}

	if results["node1"] == 0 {
		t.Errorf("node1 nije dobio nijedan ključ")
	}
	if results["node2"] == 0 {
		t.Errorf("node2 nije dobio nijedan ključ")
	}
}
