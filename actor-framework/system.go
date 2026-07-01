package actor

import (
	"agentskiSistemi/actor-framework/supervision"
	"fmt"
	"sync"
	"time"
)

type Started struct{}
type Stopping struct{}
type Restarting struct {
	reason error
}
type Stopped struct{}

type forceRestart struct{}

type ActorFailed struct {
	ActorID ActorID
	ErrMsg  string
}

type actorCell struct {
	id         ActorID
	actor      Actor
	mailbox    Mailbox
	ctx        *actorContext
	stopCh     chan struct{}
	stopOnce   sync.Once
	supervisor *supervision.Supervisor
	mu         sync.Mutex
	children   []ActorRef
}

type localActorRef struct {
	id      ActorID
	mailbox Mailbox
	system  *ActorSystem
}

type PreStarter interface {
	OnPreStart(ctx ActorContext)
}

type PostStopper interface {
	OnPostStop()
}

type PreRestarter interface {
	OnPreRestart(ctx ActorContext, reason error)
}

func (c *actorCell) stop() {
	c.stopOnce.Do(func() {
		close(c.stopCh)
	})
}

type ActorSystem struct {
	name  string
	cells map[ActorID]*actorCell
	mu    sync.RWMutex
	wg    sync.WaitGroup
}

type askEnvelope struct {
	Payload Message
	ReplyTo chan<- Message
}

func NewActorSystem(name string) *ActorSystem {
	return &ActorSystem{
		name:  name,
		cells: make(map[ActorID]*actorCell),
	}
}

func (s *ActorSystem) Lookup(id ActorID) ActorRef {
	s.mu.RLock()
	defer s.mu.RUnlock()
	cell, ok := s.cells[id]
	if !ok {
		return nil
	}
	return cell.ctx.Self()
}

func (s *ActorSystem) SpawnWithSupervisor(a Actor, name string, supervisor *supervision.Supervisor) ActorRef {
	return s.spawnChildWithSupervisor(a, name, nil, supervisor)
}

func (s *ActorSystem) spawnChildWithSupervisor(a Actor, name string, parent ActorRef, supervisor *supervision.Supervisor) ActorRef {
	id := NewActorId()
	mailbox := NewMailbox(MailboxConfig{Capacity: 100, Priority: nil})
	ref := &localActorRef{id, mailbox, s}
	ctx := newActorContext(ref, parent, s)
	cell := actorCell{
		id:         id,
		actor:      a,
		mailbox:    mailbox,
		ctx:        ctx,
		stopCh:     make(chan struct{}),
		supervisor: supervisor,
	}

	if parent != nil {
		s.mu.RLock()
		parentCell := s.cells[parent.ID()]
		s.mu.RUnlock()
		addChildToParentList(parentCell, ref)
	}

	s.mu.Lock()
	s.cells[id] = &cell
	s.mu.Unlock()
	s.wg.Add(1)
	go s.runActor(&cell)
	return ref
}

func addChildToParentList(parentCell *actorCell, ref ActorRef) {
	if parentCell != nil {
		parentCell.mu.Lock()
		parentCell.children = append(parentCell.children, ref)
		parentCell.mu.Unlock()
	}
}

func (s *ActorSystem) spawnChild(a Actor, name string, parent ActorRef) ActorRef {
	id := NewActorId()
	mailbox := NewMailbox(MailboxConfig{Capacity: 100, Priority: nil})
	ref := &localActorRef{id, mailbox, s}
	ctx := newActorContext(ref, parent, s)
	cell := actorCell{
		id:      id,
		actor:   a,
		mailbox: mailbox,
		ctx:     ctx,
		stopCh:  make(chan struct{}),
	}

	if parent != nil {
		s.mu.RLock()
		parentCell := s.cells[parent.ID()]
		s.mu.RUnlock()
		addChildToParentList(parentCell, ref)
	}

	s.mu.Lock()
	s.cells[id] = &cell
	s.mu.Unlock()
	s.wg.Add(1)
	go s.runActor(&cell)
	return ref
}

func (s *ActorSystem) Spawn(a Actor, name string) ActorRef {
	return s.spawnChild(a, name, nil)
}

func (s *ActorSystem) safeInvoke(cell *actorCell, msg Message) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%v", r)
		}
	}()
	s.dispatch(cell, msg)
	return nil
}

func (s *ActorSystem) escalateFailure(cell *actorCell, err error) {
	parent := cell.ctx.Parent()
	if parent == nil {
		return
	}
	fail := ActorFailed{ActorID: cell.id, ErrMsg: err.Error()}
	parent.Tell(fail)
}

func (s *ActorSystem) restartChildren(parentCell *actorCell) {
	parentCell.mu.Lock()
	children := make([]ActorRef, len(parentCell.children))
	copy(children, parentCell.children)
	parentCell.mu.Unlock()

	for _, child := range children {
		child.Tell(forceRestart{})
	}
}

func (s *ActorSystem) runActor(cell *actorCell) {
	defer s.wg.Done()
	defer func() {
		s.mu.Lock()
		delete(s.cells, cell.id)
		s.mu.Unlock()
	}()

	for {

		cell.ctx.behaviors = nil

		//----Start phase ----
		if h, ok := cell.actor.(PreStarter); ok {
			h.OnPreStart(cell.ctx)
		}
		cell.actor.OnStart(cell.ctx)
		cell.mailbox.Enqueue(Started{})

		restarting := false

		// ---- Loop phase ----

		msgs := cell.mailbox.Messages()

	messageLoop:
		for {

			select {
			case msg, ok := <-msgs:
				if !ok {
					return
				}
				if _, ok := msg.(forceRestart); ok {
					restarting = true
					break messageLoop
				}
				err := s.safeInvoke(cell, msg)
				if err == nil {
					continue
				} else {
					if cell.supervisor == nil {
						return
					} else {
						direktive, delay := cell.supervisor.Handle(string(cell.id), err)
						switch direktive {
						case supervision.Resume:
							continue
						case supervision.Restart:
							if h, ok := cell.actor.(PreRestarter); ok {
								h.OnPreRestart(cell.ctx, err)
							}
							cell.mailbox.Enqueue(Restarting{reason: err})
							if delay > 0 {
								time.Sleep(delay)
							}
							restarting = true

							if cell.supervisor.IsOneForAll() {
								parent := cell.ctx.Parent()
								if parent != nil {
									s.mu.RLock()
									parentCell := s.cells[parent.ID()]
									s.mu.RUnlock()
									if parentCell != nil {
										s.restartChildren(parentCell)
									}
								}
							}
							break messageLoop
						case supervision.Stop:
							return
						case supervision.Escalate:
							s.escalateFailure(cell, err)
							return
						}
					}
				}
			case <-cell.stopCh:
				cell.mailbox.Enqueue(Stopping{})
				cell.mailbox.Close()
				for msg := range msgs {
					s.dispatch(cell, msg)
				}

				cell.actor.OnStop()
				if h, ok := cell.actor.(PostStopper); ok {
					h.OnPostStop()
				}
				return
			}
		}

		if !restarting {
			break
		}
	}
}

func (s *ActorSystem) invoke(cell *actorCell, msg Message) {
	behavior := cell.ctx.activeBehavior()
	if behavior != nil {
		behavior(cell.ctx, msg)
	} else {
		cell.actor.Receive(cell.ctx, msg)
	}
}

func (s *ActorSystem) dispatch(cell *actorCell, msg Message) {
	if ask, ok := msg.(*askEnvelope); ok {
		prev := cell.ctx.replyFn
		cell.ctx.replyFn = func(response Message) {
			ask.ReplyTo <- response
		}
		s.invoke(cell, ask.Payload)
		cell.ctx.replyFn = prev
		return
	}
	s.invoke(cell, msg)
}

func (s *ActorSystem) Shutdown() {
	s.mu.RLock()
	cells := make([]*actorCell, 0, len(s.cells))
	for _, cell := range s.cells {
		cells = append(cells, cell)
	}
	s.mu.RUnlock()
	for _, cell := range cells {
		cell.stop()
	}
	s.wg.Wait()
}

func (l *localActorRef) ID() ActorID {
	return l.id
}

func (l *localActorRef) Tell(msg Message) {
	l.mailbox.Enqueue(msg)
}

func (l *localActorRef) Stop() {
	l.system.mu.RLock()
	actor, exists := l.system.cells[l.id]
	l.system.mu.RUnlock()
	if exists {
		actor.stop()
	}
}

func (l *localActorRef) Ask(context ActorContext, msg Message, timeout time.Duration) (Message, error) {
	replyCh := make(chan Message, 1)
	l.mailbox.Enqueue(&askEnvelope{Payload: msg, ReplyTo: replyCh})
	select {
	case response := <-replyCh:
		return response, nil
	case <-time.After(timeout):
		return nil, fmt.Errorf("ask timeout")
	}
}
