package remote

import (
	"agentskiSistemi/actor-framework"
	context "context"
	"time"
)

type RemoteActorRef struct {
	id      actor.ActorID
	address string
	client  *RemoteClient
}

func (r *RemoteActorRef) ID() actor.ActorID {
	return r.id
}

func (r *RemoteActorRef) Tell(msg actor.Message) {
	go func() {
		_ = r.client.Tell(r.address, r.id, msg)
	}()
}

func (r *RemoteActorRef) Ask(ctx context.Context, msg actor.Message, timeout time.Duration) (actor.Message, error) {
	relpyMsg, err := r.client.Ask(ctx, r.address, r.id, msg)
	if err != nil {
		return nil, err
	}
	return relpyMsg, nil
}

func (r *RemoteActorRef) Stop() {
	//Prazna implementacija, jer ne mozemo udaljenog actor-a ugasiti
}
