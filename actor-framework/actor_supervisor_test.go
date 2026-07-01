package actor

import (
	"agentskiSistemi/actor-framework/supervision"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

type mockActor struct {
	panicOnMsg    bool
	received      []Message
	startCount    atomic.Int32
	escalateCount atomic.Int32
}

func (m *mockActor) OnStart(ctx ActorContext) {
	fmt.Println("Actor Startovan")
	m.startCount.Add(1)
}
func (m *mockActor) OnStop() {}
func (m *mockActor) Receive(ctx ActorContext, msg Message) {
	if m.panicOnMsg {
		panic("simulirani pad")
	}
	switch msg.(type) {
	case ActorFailed: //ne znam kako da proverim da li je Message tipa ActorFailed?
		fmt.Printf("Stigla je poruka o Eskalaciji, ka ovo roditeljskom aktoru")
		m.escalateCount.Add(1)
	}
	m.received = append(m.received, msg)
}

func TestActorRestart(t *testing.T) {
	sys := NewActorSystem("sistem")

	strategy := supervision.NewOneForOne(3, 10*time.Second, nil)
	supervisor := supervision.NewSupervisor(strategy)
	mock := &mockActor{panicOnMsg: true}
	ref := sys.SpawnWithSupervisor(mock, "root", supervisor)
	ref.Tell("panic trigger")

	time.Sleep(50 * time.Millisecond)

	if mock.startCount.Load() < 2 {
		t.Errorf("očekivan restart, startCount = %d", mock.startCount.Load())
	}

	sys.Shutdown()
}

func TestActorEscalate(t *testing.T) {
	sys := NewActorSystem("sistem")

	strategy := supervision.NewOneForOne(3, 10*time.Second, nil)
	supervisor := supervision.NewSupervisor(strategy)
	mockParent := &mockActor{panicOnMsg: false}
	mock := &mockActor{panicOnMsg: true}

	parent := sys.Spawn(mockParent, "parent")
	child := sys.spawnChildWithSupervisor(mock, "child", parent, supervisor)
	child.Tell("trigger panic")

	time.Sleep(50 * time.Millisecond)

	if mockParent.escalateCount.Load() != 1 {
		t.Errorf("Roditeljski aktor nije dobio tacno jednu poruku za escalate kada je broj restarta bio veci od maksimalnog broja. EscalateCount = %d", mockParent.escalateCount.Load())
	}
}

func TestOneForAllRestart(t *testing.T) {
	sys := NewActorSystem("sistem")

	strategy := supervision.NewOneForAll(3, 10*time.Second, nil)
	supervisor := supervision.NewSupervisor(strategy)
	mockParent := &mockActor{panicOnMsg: false}
	mockChild1 := &mockActor{panicOnMsg: true}
	mockChild2 := &mockActor{panicOnMsg: false}

	parent := sys.Spawn(mockParent, "parent")
	child := sys.spawnChildWithSupervisor(mockChild1, "child1", parent, supervisor)
	sys.spawnChildWithSupervisor(mockChild2, "child2", parent, supervisor)
	child.Tell("trigger panic")

	time.Sleep(1 * time.Second)

	if mockChild1.startCount.Load() < 3 {
		t.Errorf("Dete1 koje ima PanicOnMsg TRUE nije imalo tri restarta koliki je maks, vec StartCount =  %d", mockChild1.startCount.Load())
	}

	if mockChild2.startCount.Load() < 3 {
		t.Errorf("Dete2 koje ima PanicOnMsg FALSE nije imalo tri restarta koliki je maks, vec StartCount =  %d", mockChild2.startCount.Load())
	}
}
