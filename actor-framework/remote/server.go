package remote

import (
	"agentskiSistemi/actor-framework"
	"agentskiSistemi/actor-framework/cluster"
	"context"
	"fmt"
	"net"
	"time"

	"google.golang.org/grpc"
)

type RemoteServer struct {
	UnimplementedActorServiceServer // generisano od protobuf-a
	system                          *actor.ActorSystem
	registry                        *MessageRegistry
	gossipCh                        chan cluster.GossipMessage
	grpcSrv                         *grpc.Server
}

func NewRemoteServer(sys *actor.ActorSystem) *RemoteServer {
	return &RemoteServer{
		system:   sys,
		registry: DefaultRegistry,
		gossipCh: make(chan cluster.GossipMessage, 32),
		grpcSrv:  grpc.NewServer(),
	}
}

func (r *RemoteServer) Start(port string) error {
	lis, err := net.Listen("tcp", port)
	if err != nil {
		return fmt.Errorf("remote server: %w", err)
	}

	RegisterActorServiceServer(r.grpcSrv, r) // ← registruj server!
	go r.grpcSrv.Serve(lis)
	return nil
}

func (r *RemoteServer) Stop() {
	r.grpcSrv.GracefulStop()
}

func (r *RemoteServer) Tell(_ context.Context, req *RemoteMessage) (*Ack, error) {
	ref := r.system.Lookup(actor.ActorID(req.ActorId))
	if ref == nil {
		return &Ack{Success: false, Error: "aktor nije pronađen: " + req.ActorId}, nil
	}
	msg, err := r.registry.Decode(req.TypeName, req.Payload)
	if err != nil {
		ack := Ack{Success: false, Error: err.Error()}
		return &ack, err
	}
	ref.Tell(msg)
	ack := Ack{Success: true, Error: ""}
	return &ack, nil
}

func (r *RemoteServer) Ask(ctx context.Context, req *RemoteMessage) (*RemoteMessage, error) {
	ref := r.system.Lookup(actor.ActorID(req.ActorId))
	if ref == nil {
		return nil, fmt.Errorf("Nije promadjen aktor: %v", actor.ActorID(req.ActorId))
	}
	msg, err := r.registry.Decode(req.TypeName, req.Payload)
	if err != nil {
		return nil, err
	}
	response, err := ref.Ask(ctx, msg, 30*time.Second)
	if err != nil {
		return nil, err
	}
	typeName, payload, err := r.registry.Encode(response)
	if err != nil {
		return nil, err
	}
	msgRemote := RemoteMessage{
		TypeName: typeName,
		Payload:  payload,
	}
	return &msgRemote, nil
}

func (r *RemoteServer) Gossip(_ context.Context, req *GossipProto) (*Ack, error) {
	members := make([]*cluster.NodeInfo, 0, len(req.Members))
	for _, m := range req.Members {
		members = append(members, &cluster.NodeInfo{
			ID:      cluster.NodeID(m.NodeId),
			Address: m.Address,
			Status:  cluster.NodeStatus(m.Status),
			Version: int(m.Version),
		})
	}
	msg := cluster.GossipMessage{
		Sender:  cluster.NodeID(req.SenderId),
		Members: members,
	}
	select {
	case r.gossipCh <- msg:
	default:
	}
	return &Ack{Success: true}, nil
}

func (r *RemoteServer) GossipReceive() <-chan cluster.GossipMessage {
	return r.gossipCh
}
