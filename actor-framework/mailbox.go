package actor

import (
	"container/heap"
	"errors"
	"sync"
)

var (
	ErrMailboxClosed = errors.New("mailbox is closed")
	ErrMailboxFull   = errors.New("mailbox is full")
)

type PriorityFunc func(a, b Message) bool

type Mailbox interface {
	Enqueue(msg Message) error
	Messages() <-chan Message
	Close()
	Len() int
}

type MailboxConfig struct {
	Capacity int
	Priority PriorityFunc
}

func NewMailbox(cfg MailboxConfig) Mailbox {
	if cfg.Priority != nil {
		return newPriorityMailbox(cfg.Priority)
	} else if cfg.Capacity > 0 {
		return newBoundedMailbox(cfg.Capacity)
	} else {
		return newUnboundedMailbox()
	}
}

//----Bounded Mailbox Implementation -----

type boundedMailbox struct {
	ch     chan Message
	mtx    sync.Mutex
	closed bool
}

func newBoundedMailbox(capacity int) *boundedMailbox {
	return &boundedMailbox{
		ch:     make(chan Message, capacity),
		closed: false,
	}
}

func (m *boundedMailbox) Enqueue(msg Message) error {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	if m.closed {
		return ErrMailboxClosed
	}
	select {
	case m.ch <- msg:
	default:
		return ErrMailboxFull
	}
	return nil
}

func (m *boundedMailbox) Close() {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	if m.closed {
		return
	}
	m.closed = true
	close(m.ch)
}

func (m *boundedMailbox) Messages() <-chan Message {
	return m.ch
}

func (m *boundedMailbox) Len() int {
	return len(m.ch)
}

// ----- Unbounded Mailbox Implementation -----

type unboundedMailbox struct {
	in     chan Message
	out    chan Message
	mtx    sync.Mutex
	closed bool
	size   int
}

func newUnboundedMailbox() *unboundedMailbox {
	return &unboundedMailbox{
		in:     make(chan Message, 16), // Mali buffer kako ne bi doslo do blokiranja dok process() ne skladisti poruku
		out:    make(chan Message),
		closed: false,
		size:   0,
	}
}

func (m *unboundedMailbox) Enqueue(msg Message) error {
	m.mtx.Lock()
	if m.closed {
		m.mtx.Unlock()
		return ErrMailboxClosed
	}
	m.size++
	m.mtx.Unlock()
	m.in <- msg
	return nil
}

func (m *unboundedMailbox) Close() {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	if m.closed {
		return
	}
	m.closed = true
	close(m.in)
}

func (m *unboundedMailbox) Messages() <-chan Message {
	return m.out
}

func (m *unboundedMailbox) Len() int {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	return m.size
}

func (m *unboundedMailbox) process() {
	defer close(m.out)
	var queue []Message

	for {
		if len(queue) == 0 {
			msg, ok := <-m.in
			if !ok {
				return
			}
			queue = append(queue, msg)
			continue
		}
		select {
		case m.out <- queue[0]:
			queue = queue[1:]
			m.mtx.Lock()
			m.size--
			m.mtx.Unlock()
		case msg, ok := <-m.in:
			if !ok {
				for _, remaining := range queue {
					m.out <- remaining
				}
				return
			}
			queue = append(queue, msg)
		}
	}
}

// ----- Priority Mailbox Implementation -----

type priorityMailbox struct {
	mtx    sync.Mutex
	pq     *msgHeap
	signal chan struct{}
	out    chan Message
	closed bool
}

func newPriorityMailbox(fn PriorityFunc) *priorityMailbox {
	m := &priorityMailbox{
		pq:     &msgHeap{priority: fn},
		signal: make(chan struct{}, 1),
		out:    make(chan Message, 16),
		closed: false,
	}
	heap.Init(m.pq)
	return m
}

func (m *priorityMailbox) Enqueue(msg Message) error {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	if m.closed {
		return ErrMailboxClosed
	}
	heap.Push(m.pq, msg)
	select {
	case m.signal <- struct{}{}:
	default:
	}
	return nil
}

func (m *priorityMailbox) Close() {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	if m.closed {
		return
	}
	m.closed = true
	close(m.signal)
}

func (m *priorityMailbox) Messages() <-chan Message {
	return m.out
}

func (m *priorityMailbox) Len() int {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	return m.pq.Len()
}

func (m *priorityMailbox) process() {
	defer close(m.out)
	for {
		select {
		case _, ok := <-m.signal:
			m.mtx.Lock()
			for m.pq.Len() > 0 {
				msg := heap.Pop(m.pq).(Message)
				m.mtx.Unlock()
				m.out <- msg
				m.mtx.Lock()
			}
			m.mtx.Unlock()

			if !ok {
				return
			}
		}
	}
}

//----- Heap -----

type msgHeap struct {
	items    []Message
	priority PriorityFunc // func(a, b Message) bool
}

func (h *msgHeap) Len() int {
	return len(h.items)
}

func (h *msgHeap) Less(i, j int) bool {
	return h.priority(h.items[i], h.items[j])
}

func (h *msgHeap) Swap(i, j int) {
	h.items[i], h.items[j] = h.items[j], h.items[i]
}

func (h *msgHeap) Push(x any) {
	h.items = append(h.items, x.(Message))
}

func (h *msgHeap) Pop() any {
	old := h.items
	n := len(old)
	item := h.items[n-1]
	h.items = old[:n-1]
	return item
}
