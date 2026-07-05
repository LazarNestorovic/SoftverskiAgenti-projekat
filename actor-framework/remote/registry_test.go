package remote

import (
	"agentskiSistemi/actor-framework"
	"fmt"
	reflect "reflect"
	"sync/atomic"
	"testing"
	"time"
)

type EchoActor struct {
	received atomic.Int32
}

func (e *EchoActor) OnStart(ctx actor.ActorContext) {}
func (e *EchoActor) OnStop()                        {}
func (e *EchoActor) Receive(ctx actor.ActorContext, msg actor.Message) {
	// ako je string, odgovori istom porukom
	switch m := msg.(type) {
	case string:
		fmt.Println("Aktor primio:", m)
		e.received.Add(1)
	}
}

func TestEncodingDecoding(t *testing.T) {
	msgRegistry := &MessageRegistry{
		types: make(map[string]reflect.Type),
	}
	msg := actor.Message("Ovo je nova poruka")
	msgRegistry.Register(msg)
	typeName, payload, err := msgRegistry.Encode(msg)
	if err != nil {
		t.Errorf("Nije uspesno enkodovano, %v", err)
	}
	msgDecoded, err := msgRegistry.Decode(typeName, payload)
	if err != nil {
		t.Errorf("Nije uspesno dekodovano, %v", err)
	}

	if msg != msgDecoded {
		t.Errorf("msg i msgDecoded nisu isti, msg = %v, msgDecoded = %v", msg, msgDecoded)
	}
}

func TestUnregisteredMessage(t *testing.T) {
	msgRegistry := &MessageRegistry{
		types: make(map[string]reflect.Type),
	}

	msg := actor.Message("Ovo je nova poruka")
	typeName, payload, err := msgRegistry.Encode(msg)
	if err != nil {
		t.Errorf("Nije uspesno enkodovano, %v", err)
	}
	_, err = msgRegistry.Decode(typeName, payload)
	if err == nil {
		t.Errorf("Greska nije ocitana, prosledjena je ne registrovana poruka, %v", err)
	}
}

func TestRemoteTell(t *testing.T) {
	// 1. Napravi ActorSystem i spawnovaj mock aktora
	sys := actor.NewActorSystem("test")
	mock := &EchoActor{}
	ref := sys.Spawn(mock, "mock")

	// 2. Pokreni RemoteServer
	server := NewRemoteServer(sys)
	err := server.Start(":50051")
	if err != nil {
		t.Fatalf("server nije startovan: %v", err)
	}
	// ...
	defer server.Stop()

	// 3. Registruj tip poruke
	DefaultRegistry.Register("") // string poruka

	// 4. Pošalji poruku kroz RemoteClient
	client := NewRemoteClient(DefaultRegistry)
	err = client.Tell(":50051", ref.ID(), "test poruka")
	if err != nil {
		t.Errorf("Tell nije uspeo: %v", err)
	}

	// 5. Sačekaj i provjeri da je mock primio poruku
	time.Sleep(100 * time.Millisecond)
	if mock.received.Load() != 1 {
		t.Errorf("aktor nije primio poruku")
	}
}
