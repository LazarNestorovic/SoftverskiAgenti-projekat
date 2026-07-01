package cluster

type Cluster struct {
	localID NodeID
	gossip  *GossipProtocol
	ring    *HashRing
}

func NewCluster(replicas int, localID NodeID, localAddress string, transport GossipTransport) *Cluster {
	if replicas <= 0 {
		panic("Broj replika u Hash prstenu mora biti veci od nule")
	}

	ring := NewHashRing(replicas)

	localNodeInfo := NodeInfo{localID, localAddress, NodeAlive, 0}
	gossipProtocol := NewGossipProtocol(&localNodeInfo, transport)
	ring.Add(localNodeInfo.ID)
	gossipProtocol.onChange = func(node *NodeInfo) {
		if node.Status == NodeAlive {
			ring.Add(node.ID)
		} else if node.Status == NodeDead {
			ring.Remove(node.ID)
		}
	}
	return &Cluster{
		localID: localID,
		gossip:  gossipProtocol,
		ring:    ring,
	}
}

func (c *Cluster) Start() { c.gossip.Start() }
func (c *Cluster) Stop()  { c.gossip.Stop() }

func (c *Cluster) ResponsibleNode(actorID string) NodeID {
	return c.ring.Get(actorID)
}

func (c *Cluster) IsLocal(actorID string) bool {
	return c.ring.Get(actorID) == c.localID
}
