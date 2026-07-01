package remote

import (
	"agentskiSistemi/actor-framework"
	"encoding/json"
	"fmt"
	"reflect"
	"sync"
)

type MessageRegistry struct {
	mu    sync.RWMutex
	types map[string]reflect.Type
}

var DefaultRegistry = &MessageRegistry{
	types: make(map[string]reflect.Type),
}

func (r *MessageRegistry) Register(msg actor.Message) {
	t := reflect.TypeOf(msg)
	if t.Kind() == reflect.Ptr {
		t = t.Elem() // *ChatMessage → ChatMessage
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.types[t.Name()] = t
}

func (r *MessageRegistry) Encode(msg actor.Message) (string, []byte, error) {
	t := reflect.TypeOf(msg)
	if t.Kind() == reflect.Ptr {
		t = t.Elem() // *ChatMessage → ChatMessage
	}
	payload, err := json.Marshal(msg)
	if err != nil {
		return "", nil, err
	}
	return t.Name(), payload, nil
}

func (r *MessageRegistry) Decode(typeName string, payload []byte) (actor.Message, error) {
	r.mu.RLock()
	reflectType, ok := r.types[typeName]
	r.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("neregistrovan tip poruke: %q", typeName)
	}
	ptr := reflect.New(reflectType) // *T
	if err := json.Unmarshal(payload, ptr.Interface()); err != nil {
		return nil, fmt.Errorf("decode %s: %w", typeName, err)
	}
	return ptr.Elem().Interface(), nil // T (vrednost, ne pointer)
}
