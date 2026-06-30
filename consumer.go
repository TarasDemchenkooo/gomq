package gomq

import "fmt"

const defaultConsumerBufferSize = 64
var ConsumerBufferSize = defaultConsumerBufferSize

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

	pending map[uint64]Message
}

func (c *consumer) Messages() <-chan Message {
	return c.userChan
}

func (c *consumer) Ack(id uint64) error {
	if _, ok := c.pending[id]; !ok {
		return fmt.Errorf("there's no message with id %d", id)
	}

	delete(c.pending, id)
	return nil
}

func (c *consumer) Pending() []Message {
	messages := make([]Message, len(c.pending))
	i := 0

	for _, msg := range c.pending {
		messages[i] = msg
		i++
	}

	return messages
}

func (c *consumer) Close() {
	c.q.unsubscribe(c)
}

func newConsumer(q *queue) *consumer {
	c := &consumer{
		q: q,
		buffer: make(chan Message, ConsumerBufferSize),
		userChan: make(chan Message),
		pending: make(map[uint64]Message),
	}

	go c.consume()

	return c
}

func (c *consumer) consume() {
	for msg := range c.buffer {
		c.pending[msg.ID] = msg
		c.userChan <- msg
	}

	close(c.userChan)
}