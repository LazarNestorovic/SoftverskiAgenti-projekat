package actor

import (
	"fmt"
	"testing"
	"time"
)

type EchoActor struct{}

func (e *EchoActor) OnStart(ctx ActorContext) {}
func (e *EchoActor) OnStop()                  {}
func (e *EchoActor) Receive(ctx ActorContext, msg Message) {
	// ako je string, odgovori istom porukom
	switch m := msg.(type) {
	case string:
		fmt.Println("Aktor primio:", m)
		ctx.Reply(m)
	case Started:
		fmt.Println("Aktor startovan")
	}
}

// Test 1 — Tell: pošalji poruku, provjeri da ne crashuje
func TestTell(t *testing.T) {
	sys := NewActorSystem("Proba")
	rootActor := EchoActor{}
	root := sys.Spawn(&rootActor, "something")
	root.Tell("Cao")
	time.Sleep(50 * time.Millisecond) // daj goroutini vremena
	sys.Shutdown()
}

// Test 2 — Ask: pošalji i čekaj odgovor
func TestAsk(t *testing.T) {
	sys := NewActorSystem("Proba")
	root := sys.Spawn(&EchoActor{}, "something")

	response, err := root.Ask(nil, "Zdravo", 1*time.Second)

	if err != nil {
		t.Fatal("Neocekivana greska: ", err)
	}

	switch response.(type) {
	case string:
		fmt.Println("Response je tipa string")
	default:
		t.Error("Response nije string!")
	}

	if response != "Zdravo" {
		t.Error("Response nije zdravo")
	}

	sys.Shutdown()
}

// Test 3 — Shutdown: sistem se uredno gasi
func TestShutdown(t *testing.T) {
	sys := NewActorSystem("test")

	// spawna nekoliko aktora
	sys.Spawn(&EchoActor{}, "echo1")
	sys.Spawn(&EchoActor{}, "echo2")
	sys.Spawn(&EchoActor{}, "echo3")

	// provjeri da Shutdown završi u razumnom vremenu
	done := make(chan struct{})
	go func() {
		sys.Shutdown()
		close(done)
	}()

	select {
	case <-done:
		// Shutdown završio ✅
	case <-time.After(2 * time.Second):
		t.Fatal("Shutdown se nije završio na vrijeme — moguć deadlock!")
	}
}
