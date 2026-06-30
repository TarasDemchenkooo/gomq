package gomq

import (
	"sync"
	"sync/atomic"
)

const defaultQueueBufferSize = 64
var QueueBufferSize = defaultQueueBufferSize

type Queue interface {
	// get queue name
	Name() string

	// create new consumer for the queue
	Subscribe() Consumer
	
	// current count of consumers
	ConsumerCount() int
}

type queue struct {
	name string
	buffer chan Message
	nextMessageID atomic.Uint64

	cmu sync.Mutex
	consumers map[*consumer]struct{}

	mu sync.RWMutex
	closed bool
}

func (q *queue) Name() string {
	return q.name
}

func (q *queue) Subscribe() Consumer {
	c := newConsumer(q)

	q.mu.RLock()
	defer q.mu.RUnlock()

	if q.closed {
		return c
	}

	q.cmu.Lock()
	defer q.cmu.Unlock()

	q.consumers[c] = struct{}{}
	return c
}

func (q *queue) ConsumerCount() int {
	q.cmu.Lock()
	defer q.cmu.Unlock()
	
	return len(q.consumers)
}

func newQueue(name string) *queue {
	q := &queue{
		name: name,
		buffer: make(chan Message, QueueBufferSize),
		consumers: make(map[*consumer]struct{}),
	}

	go q.deliver()
	return q
}

func (q *queue) unsubscribe(c *consumer) {
	q.cmu.Lock()
	defer q.cmu.Unlock()

	delete(q.consumers, c)
	close(c.buffer)
}

func (q *queue) closeQueue() {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		return
	}

	q.closed = true
	close(q.buffer)
}

func (q *queue) deliver() {
	for message := range q.buffer {
		q.cmu.Lock()
		for c := range q.consumers {
			select {
			case c.buffer <- message:
			default:
			}
		}
		q.cmu.Unlock()
	}

	q.cmu.Lock()
	for c := range q.consumers {
		close(c.buffer)
	}
	clear(q.consumers)
	q.cmu.Unlock()
}