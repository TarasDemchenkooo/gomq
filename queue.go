package gomq

import (
	"sync"
	"sync/atomic"
)

// тут тоже самое что и в consumer
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
	name          string
	buffer        chan Message
	nextMessageID atomic.Uint64

	cmu       sync.Mutex
	consumers map[*consumer]struct{}

	mu     sync.RWMutex
	closed bool
}

func (q *queue) Name() string {
	return q.name
}

func (q *queue) Subscribe() Consumer {
	c := newConsumer(q) // уже запустил go c.consume()

	q.mu.RLock()
	defer q.mu.RUnlock()

	// при подписке на закрытую очередь consumer не добавляется в cinsumers. c.buffer не закроется и consume виснет
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
		name:      name,
		buffer:    make(chan Message, QueueBufferSize),
		consumers: make(map[*consumer]struct{}),
	}

	go q.deliver()
	return q
}

// unsubscribe закрывает c.buffer. Но deliver() при закрытии очереди уже закрывает
//
//	нужно закрывать только если consumer реально был в мапе (ok из delete)
//
// panic: close of closed channel
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
	// при полном буффере просто дропаются сообщения, медленный и быстрый получат разный набор сообщений
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
