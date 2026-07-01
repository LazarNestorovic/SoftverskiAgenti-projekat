package actor

import (
	"agentskiSistemi/actor-framework/supervision"
	"context"
	"time"

	"github.com/google/uuid"
)

type ActorID string

func NewActorId() ActorID {
	return ActorID(uuid.New().String())
}

type Message any

type Actor interface {
	OnStart(ctx ActorContext)
	OnStop()
	Receive(ctx ActorContext, msg Message)
}

type BehaviorFunc func(ctx ActorContext, msg Message)

type ActorRef interface {
	ID() ActorID
	Tell(msg Message)
	Ask(ctx context.Context, msg Message, timeout time.Duration) (Message, error)
	Stop()
}

type ActorContext interface {
	Self() ActorRef
	Parent() ActorRef
	Send(to ActorRef, msg Message)
	Spawn(actor Actor, name string) ActorRef
	SpawnWithSupervisor(actor Actor, name string, supervisor *supervision.Supervisor) ActorRef
	Become(behavior BehaviorFunc)
	Unbecome()
	Reply(msg Message)
}
