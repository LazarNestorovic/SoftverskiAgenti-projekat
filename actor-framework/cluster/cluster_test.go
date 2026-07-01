package cluster

import (
	"fmt"
	"testing"
	"time"
)

type mockTransport struct {
	address string
	inbox   chan GossipMessage
	network map[string]*mockTransport // adresa → transport tog čvora
}

func (t *mockTransport) SendGossip(address string, msg GossipMessage) error {
	target, ok := t.network[address]
	if !ok {
		return fmt.Errorf("nepoznata adresa: %s", address)
	}
	target.inbox <- msg
	return nil
}

func (t *mockTransport) ReceiveGossip() <-chan GossipMessage {
	return t.inbox
}

func newMockNetwork(addresses ...string) map[string]*mockTransport {
	network := make(map[string]*mockTransport)
	for _, addr := range addresses {
		network[addr] = &mockTransport{
			address: addr,
			inbox:   make(chan GossipMessage, 10), // bafer da SendGossip ne blokira u testu
			network: network,                      // svi dele istu mapu
		}
	}
	return network
}

func TestGossipProtocol_TwoNodesDiscoverEachOther(t *testing.T) {
	network := newMockNetwork("node1:8080", "node2:8081")

	node1 := &NodeInfo{ID: "node1", Address: "node1:8080", Status: NodeAlive, Version: 1}
	node2 := &NodeInfo{ID: "node2", Address: "node2:8081", Status: NodeAlive, Version: 1}

	g1 := NewGossipProtocol(node1, network["node1:8080"])
	g2 := NewGossipProtocol(node2, network["node2:8081"])

	// Kratak interval da test ne mora da čeka 2+ sekunde (podrazumevani interval).
	g1.interval = 20 * time.Millisecond
	g2.interval = 20 * time.Millisecond

	// Asimetrično stanje: samo g1 zna za node2. g2 NE zna za node1.
	// Ako gossip radi ispravno, g2 će saznati za node1 kroz primljenu poruku.
	g1.members.Upsert(node2)

	g1.Start()
	g2.Start()
	defer g1.Stop()
	defer g2.Stop()

	// Čekamo da prođe makar par gossip ciklusa.
	time.Sleep(100 * time.Millisecond)

	all2 := g2.members.All()
	if len(all2) != 2 {
		t.Errorf("node2 očekivano da sazna za 2 čvora kroz gossip, zna za %d", len(all2))
	}

	var foundNode1 *NodeInfo
	for _, n := range all2 {
		if n.ID == "node1" {
			foundNode1 = n
		}
	}
	if foundNode1 == nil {
		t.Errorf("node2 nije saznao za node1 kroz gossip")
	}
}

func TestGossipProtocol_FailureDetection(t *testing.T) {
	network := newMockNetwork("node1:8080", "node2:8081")

	node1 := &NodeInfo{ID: "node1", Address: "node1:8080", Status: NodeAlive, Version: 1}
	node2 := &NodeInfo{ID: "node2", Address: "node2:8081", Status: NodeAlive, Version: 1}

	g1 := NewGossipProtocol(node1, network["node1:8080"])
	g1.members.Upsert(node2)

	// Kratki intervali da test ne traje predugo.
	g1.interval = 20 * time.Millisecond
	g1.suspectInterval = 40 * time.Millisecond
	g1.deadInterval = 80 * time.Millisecond

	// Simuliramo da je node2 viđen "sada", pa ćemo čekati da istekne.
	g1.lsMU.Lock()
	g1.lastSeen["node2"] = time.Now()
	g1.lsMU.Unlock()

	g1.Start()
	defer g1.Stop()

	time.Sleep(60 * time.Millisecond)

	all := g1.members.All()
	var found *NodeInfo
	for _, n := range all {
		if n.ID == "node2" {
			found = n
		}
	}
	if found.Status != NodeSuspected {
		t.Errorf("očekivan status NodeSuspected, dobijeno %v", found.Status)
	}

	time.Sleep(100 * time.Millisecond)

	all = g1.members.All()
	for _, n := range all {
		if n.ID == "node2" {
			found = n
		}
	}
	if found.Status != NodeDead {
		t.Errorf("očekivan status NodeDead, dobijeno %v", found.Status)
	}
}

func TestMemberList_UpsertNew(t *testing.T) {
	ml := NewMemberList()

	info := &NodeInfo{ID: "node1", Address: "localhost:8080", Status: NodeAlive, Version: 1}
	ml.Upsert(info)

	all := ml.All()
	if len(all) != 1 {
		t.Errorf("očekivano 1 čvor, dobijeno %d", len(all))
	}
}

func TestMemberList_UpsertUpdate(t *testing.T) {
	ml := NewMemberList()

	info := &NodeInfo{ID: "node1", Address: "localhost:8080", Status: NodeAlive, Version: 1}
	ml.Upsert(info)

	new := &NodeInfo{ID: "node1", Address: "localhost:8081", Status: NodeAlive, Version: 2}
	ml.Upsert(new)

	all := ml.All()

	if all[0].Address != new.Address {
		t.Errorf("očekivano Address = localhost:8081 čvor, dobijeno %v", all[0].Address)
	}

	if len(all) != 1 {
		t.Errorf("očekivano 1 čvor, dobijeno %d", len(all))
	}
}

func TestMemberList_MergeOlderVersionIgnored(t *testing.T) {
	ml := NewMemberList()

	info := &NodeInfo{ID: "node1", Address: "localhost:8080", Status: NodeAlive, Version: 5}
	ml.Upsert(info)

	new := &NodeInfo{ID: "node1", Address: "localhost:8080", Status: NodeAlive, Version: 2}

	newList := []*NodeInfo{new}

	ml.Merge(newList)

	all := ml.All()

	var found *NodeInfo
	for _, n := range all {
		if n.ID == "node1" {
			found = n
		}
	}

	if found.Version != 5 {
		t.Errorf("očekivano Version = 5, dobijeno %d", found.Version)
	}
}

func TestMemberList_MergeNewerVersionApplied(t *testing.T) {
	ml := NewMemberList()

	info := &NodeInfo{ID: "node1", Address: "localhost:8080", Status: NodeAlive, Version: 5}
	ml.Upsert(info)

	new := &NodeInfo{ID: "node1", Address: "localhost:8080", Status: NodeAlive, Version: 7}

	newList := []*NodeInfo{new}

	changed := ml.Merge(newList)

	if len(changed) != 1 {
		t.Errorf("očekivano broj elemenata 1, dobijeno %d", len(changed))
	} else if changed[0].Version != 7 {
		t.Errorf("očekivano Version = 7, dobijeno %d", changed[0].Version)
	}
}
