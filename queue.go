package gomq

import (
	"sync"
	"sync/atomic"
	"time"
	"context"
	"fmt"
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
	backlog map[*consumer][]Message

	mu sync.RWMutex
	closed bool
	timeout time.Duration
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
		backlog: make(map[*consumer][]Message),
		timeout: defaultQueueClosingTimeout,
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

	if _, ok := q.consumers[c]; ok {
		delete(q.consumers, c)
		delete(q.backlog, c)
		close(c.buffer)
	}
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
			// try to send all consumer's backlog within a tick
			if backlog, ok := q.backlog[c]; ok {
				i := 0

				loop:
				for i < len(backlog) {
					select {
					case c.buffer <- backlog[i]:
						i++
					default:
						break loop
					}
				}

				if i == len(backlog) {
					delete(q.backlog, c)
				} else {
					q.backlog[c] = backlog[i:]
				}
			}

			select {
			case c.buffer <- message:
			default:
				q.backlog[c] = append(q.backlog[c], message)
			}
		}
		q.cmu.Unlock()
	}

	// after queue closing, trying to send backlog within timeout
	// on timeout just drop backlog for too slow consumers
	q.cmu.Lock()
	var wg sync.WaitGroup
	ctx, cancel := context.WithTimeout(context.Background(), q.timeout)
	defer cancel()

	for c := range q.consumers {
		backlog, ok := q.backlog[c]

		if !ok {
			close(c.buffer)
			continue
		}

		wg.Add(1)

		go func() {
			defer wg.Done()
			defer close(c.buffer)

			i := 0

			for i < len(backlog) {
				select {
				case c.buffer <- backlog[i]:
					i++
				case <-ctx.Done():
					return
				}
			}
		}()
	}

	wg.Wait()

	clear(q.consumers)
	clear(q.backlog)
	q.cmu.Unlock()
}