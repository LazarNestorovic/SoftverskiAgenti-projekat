package actor

import "agentskiSistemi/actor-framework/supervision"

type actorContext struct {
	self      ActorRef
	parent    ActorRef
	system    *ActorSystem
	behaviors []BehaviorFunc
	replyFn   func(Message)
}

func newActorContext(self ActorRef, parent ActorRef, system *ActorSystem) *actorContext {
	return &actorContext{
		self:   self,
		parent: parent,
		system: system,
	}
}

func (a *actorContext) Self() ActorRef {
	return a.self
}

func (a *actorContext) Parent() ActorRef {
	return a.parent
}

func (a *actorContext) Send(to ActorRef, msg Message) {
	to.Tell(msg)
}

func (a *actorContext) Spawn(actor Actor, name string) ActorRef {
	return a.system.spawnChild(actor, name, a.self)
}

func (a *actorContext) SpawnWithSupervisor(actor Actor, name string, supervisor *supervision.Supervisor) ActorRef {
	return a.system.spawnChildWithSupervisor(actor, name, a.self, supervisor)
}

func (a *actorContext) Become(behavior BehaviorFunc) {
	a.behaviors = append(a.behaviors, behavior)
}

func (a *actorContext) Unbecome() {
	n := len(a.behaviors)
	if n > 0 {
		a.behaviors = a.behaviors[:n-1]
	}
}

func (a *actorContext) activeBehavior() BehaviorFunc {
	n := len(a.behaviors)
	if n == 0 {
		return nil
	}

	return a.behaviors[n-1]
}

func (a *actorContext) Reply(msg Message) {
	if a.replyFn != nil {
		a.replyFn(msg)
	}
}
