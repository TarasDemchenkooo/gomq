package gomq

import (
	"fmt"
	"sync"
)

// not thread safe
// for parallel reading create new consumer
type Consumer interface {
	// get channel for reading messages
	Messages() <-chan Message

	// approve that message was processed
	// returns error if there's no message with the provided id in the buffer
	Ack(id uint64) error

	// get all unprocessed messages
	Pending() []Message

	// unsubscribe from the queue
	Close()
}

type consumer struct {
	q *queue

	buffer chan Message
	userChan chan Message

	closeOnce sync.Once
	done chan struct{}

	mu sync.Mutex
	pending map[uint64]Message
}

func (c *consumer) Messages() <-chan Message {
	return c.userChan
}

func (c *consumer) Ack(id uint64) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.pending[id]; !ok {
		return fmt.Errorf("there's no message with id %d", id)
	}

	delete(c.pending, id)
	return nil
}

func (c *consumer) Pending() []Message {
	c.mu.Lock()
	defer c.mu.Unlock()
	
	messages := make([]Message, len(c.pending))
	i := 0

	for _, msg := range c.pending {
		messages[i] = msg
		i++
	}

	return messages
}

func (c *consumer) Close() {
	c.closeOnce.Do(func() {
		c.q.unsubscribe(c)
		close(c.done)
	})
}

func newConsumer(q *queue, opts ...ConsumerOption) *consumer {
	c := &consumer{
		q: q,
		buffer: make(chan Message, defaultConsumerBufferSize),
		userChan: make(chan Message),
		pending: make(map[uint64]Message),
		done: make(chan struct{}),
	}

	for _, opt := range opts {
		opt(c)
	}

	if !q.closed {
		go c.consume()
	}

	return c
}

func (c *consumer) consume() {
	defer close(c.userChan)

	for msg := range c.buffer {
		c.mu.Lock()
        c.pending[msg.ID] = msg
        c.mu.Unlock()

        select {
        case c.userChan <- msg:
        case <-c.done:
            return
        }
	}
}