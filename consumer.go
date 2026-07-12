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

	userChan chan Message

	bmu sync.Mutex
	cond *sync.Cond
	buffer []Message
	bufferClosed bool

	mu sync.Mutex
	pending map[uint64]Message

	closeOnce sync.Once
	done chan struct{}
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
		c.bmu.Lock()
		c.bufferClosed = true
		c.bmu.Unlock()
		close(c.done)
		c.cond.Signal()
	})
}

const initialConsumerBufferSize = 16

func newConsumer(q *queue) *consumer {
	c := &consumer{
		q: q,
		userChan: make(chan Message),
		buffer: make([]Message, 0, initialConsumerBufferSize),
		pending: make(map[uint64]Message),
		done: make(chan struct{}),
	}

	c.cond = sync.NewCond(&c.bmu)

	if q.closed {
		close(c.userChan)
	} else {
		go c.consume()
	}

	return c
}

func (c *consumer) consume() {
	defer close(c.userChan)

	for {
		c.bmu.Lock()
		for len(c.buffer) == 0 && !c.bufferClosed {
			c.cond.Wait()
		}

		if len(c.buffer) == 0 && c.bufferClosed {
			c.bmu.Unlock()
			return
		}

		snapshot := c.buffer
		c.buffer = make([]Message, 0, initialConsumerBufferSize)
		c.bmu.Unlock()

		for i := 0; i < len(snapshot); i++ {
			message := snapshot[i]

			c.mu.Lock()
        	c.pending[message.ID] = message
        	c.mu.Unlock()

			select {
			case c.userChan <- message:
			case <-c.done:
				return
			}
		}
	}
}