package remote

import (
	"agentskiSistemi/actor-framework"
	"agentskiSistemi/actor-framework/cluster"
	context "context"
	"fmt"
	"sync"
	"time"

	grpc "google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type ConnectionPool struct {
	mu    sync.RWMutex
	conns map[string]ActorServiceClient
}

type RemoteClient struct {
	pool     *ConnectionPool
	registry *MessageRegistry
}

func NewRemoteClient(registry *MessageRegistry) *RemoteClient {
	if registry == nil {
		registry = DefaultRegistry
	}
	return &RemoteClient{
		pool:     NewConnectionPool(),
		registry: registry,
	}
}

func (r *RemoteClient) Tell(address string, id actor.ActorID, msg actor.Message) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	typeName, payload, err := r.registry.Encode(msg)
	if err != nil {
		return err
	}
	actorServiceClient, err := r.pool.get(address)
	if err != nil {
		return err
	}
	ack, err := actorServiceClient.Tell(ctx, &RemoteMessage{ActorId: string(id), TypeName: typeName, Payload: payload})
	if err != nil {
		return err
	}
	if !ack.Success {
		return fmt.Errorf("Metoda Tell nije uspesno poslata")
	}
	return nil
}

func (r *RemoteClient) Ask(ctx context.Context, address string, id actor.ActorID, msg actor.Message) (actor.Message, error) {
	typeName, payload, err := r.registry.Encode(msg)
	if err != nil {
		return nil, err
	}
	actorServiceClient, err := r.pool.get(address)
	if err != nil {
		return nil, err
	}

	remoteMsg, err := actorServiceClient.Ask(ctx, &RemoteMessage{ActorId: string(id), TypeName: typeName, Payload: payload})

	if err != nil {
		return nil, err
	}

	actorMessage, err := r.registry.Decode(remoteMsg.TypeName, remoteMsg.Payload)
	if err != nil {
		return nil, err
	}
	return actorMessage, nil
}

func (r *RemoteClient) SendGossip(address string, msg cluster.GossipMessage) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	actorServiceClient, err := r.pool.get(address)
	if err != nil {
		return err
	}

	protoMembers := make([]*NodeInfo, 0, len(msg.Members))
	for _, msg := range msg.Members {
		protoMembers = append(protoMembers, &NodeInfo{
			NodeId:  string(msg.ID),
			Address: msg.Address,
			Status:  int32(msg.Status),
			Version: int64(msg.Version),
		})
	}

	gossipProto := &GossipProto{SenderId: string(msg.Sender), Members: protoMembers}

	ack, err := actorServiceClient.Gossip(ctx, gossipProto)
	if err != nil {
		return err
	}

	if !ack.Success {
		return fmt.Errorf("Metoda Tell nije uspesno poslata")
	}
	return nil
}

func NewConnectionPool() *ConnectionPool {
	return &ConnectionPool{
		conns: make(map[string]ActorServiceClient),
	}
}

func (c *ConnectionPool) get(address string) (ActorServiceClient, error) {
	c.mu.RLock()
	client, exists := c.conns[address]
	c.mu.RUnlock()
	if exists {
		return client, nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	// Double-check — možda je neko drugi dodao konekciju dok smo čekali na Lock
	if client, exists = c.conns[address]; exists {
		return client, nil
	}
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("connect %s: %w", address, err)
	}
	client = NewActorServiceClient(conn)
	c.conns[address] = client
	return client, nil
}
