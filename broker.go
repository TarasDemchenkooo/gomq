package gomq

import "sync"

type Broker interface {
	// get or create new queue
	Queue(name string, opts ...QueueOption) Queue

	// create new producer for a queue with name queueName
	NewProducer(queueName string) Producer
	
	// close all queues
	Close()
}

type broker struct {
	mu sync.Mutex
	queues map[string]*queue
}

func (b *broker) Queue(name string, opts ...QueueOption) Queue {
	return b.getOrCreateQueue(name, opts...)
}

func (b *broker) NewProducer(queueName string) Producer {
	q := b.getOrCreateQueue(queueName)
	return newProducer(q)
}

func (b *broker) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()

	for _, q := range b.queues {
		q.closeQueue()
	}
}

func NewBroker() Broker {
	return &broker{
		queues: make(map[string]*queue),
	}
}

func (b *broker) getOrCreateQueue(name string, opts ...QueueOption) *queue {
	b.mu.Lock()
	defer b.mu.Unlock()
	
	if q, ok := b.queues[name]; ok {
		return q
	}

	q := newQueue(name, opts...)
	b.queues[name] = q
	return q
}