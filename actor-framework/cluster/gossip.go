package cluster

import (
	"math/rand"
	"sync"
	"time"
)

type NodeID string

type NodeStatus int

const (
	NodeAlive NodeStatus = iota
	NodeSuspected
	NodeDead
)

type NodeInfo struct {
	ID      NodeID
	Address string
	Status  NodeStatus
	Version int
}

type GossipMessage struct {
	Sender  NodeID
	Members []*NodeInfo
}

type GossipTransport interface {
	SendGossip(address string, msg GossipMessage) error
	ReceiveGossip() <-chan GossipMessage
}

type MemberList struct {
	mu      sync.RWMutex
	members map[NodeID]*NodeInfo
}

type GossipProtocol struct {
	localNode *NodeInfo
	members   *MemberList
	transport GossipTransport
	onChange  func(*NodeInfo)

	interval        time.Duration //how often gossips
	suspectInterval time.Duration //after that interval -> suspect
	deadInterval    time.Duration // after that interval -> dead

	stopCh chan struct{}

	lastSeen map[NodeID]time.Time
	lsMU     sync.Mutex
}

func NewGossipProtocol(local *NodeInfo, transport GossipTransport) *GossipProtocol {
	gossipProtocol := &GossipProtocol{
		localNode:       local,
		members:         NewMemberList(),
		transport:       transport,
		interval:        2 * time.Second,
		suspectInterval: 6 * time.Second,
		deadInterval:    12 * time.Second,
		stopCh:          make(chan struct{}),
		lastSeen:        make(map[NodeID]time.Time),
	}
	gossipProtocol.members.Upsert(local)
	return gossipProtocol
}

func NewMemberList() *MemberList {
	return &MemberList{
		members: make(map[NodeID]*NodeInfo),
	}
}

func (g *GossipProtocol) Start() {
	go g.gossipLoop()
	go g.receiveLoop()
	go g.failureDetectionLoop()
}

func (g *GossipProtocol) Stop() {
	close(g.stopCh)
}

func (g *GossipProtocol) gossipLoop() {
	ticker := time.NewTicker(g.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			g.gossipOnce()
		case <-g.stopCh:
			return
		}
	}
}

func (g *GossipProtocol) gossipOnce() {
	var peers []*NodeInfo
	for _, n := range g.members.Alive() {
		if n.ID != g.localNode.ID {
			peers = append(peers, n)
		}
	}
	if len(peers) == 0 {
		return
	}

	target := peers[rand.Intn(len(peers))]
	msg := GossipMessage{
		Sender:  g.localNode.ID,
		Members: g.members.All(),
	}
	g.transport.SendGossip(target.Address, msg)
}

func (g *GossipProtocol) receiveLoop() {
	for {
		select {
		case msg := <-g.transport.ReceiveGossip():
			g.handleGossip(msg)
		case <-g.stopCh:
			return
		}
	}
}

func (g *GossipProtocol) handleGossip(msg GossipMessage) {
	g.lsMU.Lock()
	g.lastSeen[msg.Sender] = time.Now()
	g.lsMU.Unlock()
	changedMemebers := g.members.Merge(msg.Members)
	if g.onChange != nil {
		for _, member := range changedMemebers {
			g.onChange(member)
		}
	}
}

func (g *GossipProtocol) failureDetectionLoop() {
	ticker := time.NewTicker(g.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			g.detectFailures()
		case <-g.stopCh:
			return
		}
	}
}

func (g *GossipProtocol) detectFailures() {
	for _, member := range g.members.All() {
		if member.ID == g.localNode.ID {
			continue
		}
		g.lsMU.Lock()
		ls, ok := g.lastSeen[member.ID]
		g.lsMU.Unlock()
		if !ok {
			continue
		} else if time.Now().Sub(ls) >= g.deadInterval {
			updated := *member
			updated.Status = NodeDead
			updated.Version++
			g.members.Upsert(&updated)
			if g.onChange != nil {
				g.onChange(&updated)
			}
			continue
		} else if time.Now().Sub(ls) >= g.suspectInterval {
			updated := *member
			updated.Status = NodeSuspected
			updated.Version++
			g.members.Upsert(&updated)
			if g.onChange != nil {
				g.onChange(&updated)
			}
			continue
		}
	}
}

func (m *MemberList) Upsert(info *NodeInfo) {
	defer m.mu.Unlock()
	m.mu.Lock()
	m.members[info.ID] = info
}

func (m *MemberList) All() []*NodeInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()
	ret := make([]*NodeInfo, 0, len(m.members))
	for _, info := range m.members {
		ret = append(ret, info)
	}
	return ret
}

func (m *MemberList) Alive() []*NodeInfo {
	defer m.mu.RUnlock()
	ret := []*NodeInfo{}
	m.mu.RLock()
	for _, member := range m.members {
		if member.Status == NodeAlive {
			ret = append(ret, member)
		}
	}
	return ret
}

func (m *MemberList) Merge(incoming []*NodeInfo) []*NodeInfo {
	m.mu.Lock()
	defer m.mu.Unlock()

	var changed []*NodeInfo
	for _, info := range incoming {
		existing, ok := m.members[info.ID]
		if !ok || info.Version > existing.Version {
			m.members[info.ID] = info
			changed = append(changed, info)
		}
	}
	return changed
}
