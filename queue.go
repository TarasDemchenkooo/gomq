package gomq

import (
	"sync"
	"sync/atomic"
)

type Queue interface {
	// get queue name
	Name() string

	// create new consumer for the queue
	Subscribe(opts ...ConsumerOption) Consumer
	
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

func (q *queue) Subscribe(opts ...ConsumerOption) Consumer {
	q.mu.RLock()
	defer q.mu.RUnlock()

	c := newConsumer(q, opts...)

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

func newQueue(name string, opts ...QueueOption) *queue {
	q := &queue{
		name: name,
		buffer: make(chan Message, defaultQueueBufferSize),
		consumers: make(map[*consumer]struct{}),
	}

	for _, opt := range opts {
		opt(q)
	}

	go q.deliver()

	return q
}

func (q *queue) unsubscribe(c *consumer) {
	q.cmu.Lock()
	defer q.cmu.Unlock()

	delete(q.consumers, c)
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
				c.bmu.Lock()
				c.backlog = append(c.backlog, message)
				c.bmu.Unlock()
				c.cond.Signal()
			}
		}
		q.cmu.Unlock()
	}

	q.cmu.Lock()
	for c := range q.consumers {
		c.bmu.Lock()
		c.backlogClosed = true
		c.bmu.Unlock()
		c.cond.Signal()
	}

	clear(q.consumers)
	q.cmu.Unlock()
}