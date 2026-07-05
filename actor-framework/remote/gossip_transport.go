package remote

import "agentskiSistemi/actor-framework/cluster"

type GrpcGossipTransport struct {
	client *RemoteClient
	server *RemoteServer
}

func NewGrpcGossipTransport(cl *RemoteClient, serv *RemoteServer) *GrpcGossipTransport {
	return &GrpcGossipTransport{
		client: cl,
		server: serv,
	}
}

func (g *GrpcGossipTransport) SendGossip(address string, msg cluster.GossipMessage) error {
	err := g.client.SendGossip(address, msg)
	if err != nil {
		return err
	}
	return nil
}
func (g *GrpcGossipTransport) ReceiveGossip() <-chan cluster.GossipMessage {
	return g.server.GossipReceive()
}
