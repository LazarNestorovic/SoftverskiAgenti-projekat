package actors

import (
	"agentskiSistemi/actor-framework"
	"math"
)

type clientEntry struct {
	ref         actor.ActorRef
	featureMean []float64
}

type ClusterManagerActor struct {
	numClusters int
	clients     []clientEntry
}

func NewClusterManagerActor(numClusters int) *ClusterManagerActor {
	return &ClusterManagerActor{numClusters: numClusters}
}

func (c *ClusterManagerActor) OnStart(ctx actor.ActorContext) {}
func (c *ClusterManagerActor) OnStop()                        {}

func (c *ClusterManagerActor) Receive(ctx actor.ActorContext, msg actor.Message) {
	switch m := msg.(type) {
	case RegisterClient:
		c.clients = append(c.clients, clientEntry{ref: m.Ref, featureMean: m.FeatureMean})
		if len(c.clients) >= m.ExpectedTotal {
			c.clusterAndAssign(ctx)
		}
	}
}

func (c *ClusterManagerActor) clusterAndAssign(ctx actor.ActorContext) {
	assignments := kMeans(c.clients, c.numClusters)

	clusterMembers := make(map[int][]string)
	for i, cid := range assignments {
		clusterMembers[cid] = append(clusterMembers[cid], string(c.clients[i].ref.ID()))
	}

	for i, entry := range c.clients {
		cid := assignments[i]
		members := clusterMembers[cid]
		ctx.Send(entry.ref, ClusterAssign{
			ClusterID:            cid,
			ClusterMembers:       members,
			ClusterCoordinatorID: members[0],
		})
	}
}

func kMeans(clients []clientEntry, k int) []int {
	if k > len(clients) {
		k = len(clients)
	}
	centers := make([][]float64, k)
	for i := range k {
		centers[i] = clients[i].featureMean
	}

	assignments := make([]int, len(clients))
	for range 100 {
		changed := false
		for i, c := range clients {
			best, minD := 0, math.MaxFloat64
			for j, center := range centers {
				if d := euclidean(c.featureMean, center); d < minD {
					minD, best = d, j
				}
			}
			if assignments[i] != best {
				assignments[i] = best
				changed = true
			}
		}
		if !changed {
			break
		}
		dims := len(clients[0].featureMean)
		next := make([][]float64, k)
		counts := make([]int, k)
		for j := range k {
			next[j] = make([]float64, dims)
		}
		for i, c := range clients {
			cl := assignments[i]
			counts[cl]++
			for d, f := range c.featureMean {
				next[cl][d] += f
			}
		}
		for j := range k {
			if counts[j] > 0 {
				for d := range dims {
					next[j][d] /= float64(counts[j])
				}
			}
		}
		centers = next
	}
	return assignments
}

func euclidean(a, b []float64) float64 {
	var sum float64
	for i := range a {
		d := a[i] - b[i]
		sum += d * d
	}
	return math.Sqrt(sum)
}
