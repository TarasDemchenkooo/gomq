package gomq

import "testing"

func TestQueueCreation(t *testing.T) {
	b := NewBroker()

	q1 := b.Queue("a").(*queue)
	q2 := b.Queue("a").(*queue)

	if q1 != q2 {
		t.Error("same name returns a new queue")
	}
}

func TestProducerCreation(t *testing.T) {
	b := NewBroker()

	p := b.NewProducer("a").(*producer)
	q := b.Queue("a").(*queue)

	if p.q != q {
		t.Error("producer is not attached to appropriate queue")
	}
}

func TestQueuesClosing(t *testing.T) {
	b := NewBroker()

	q1 := b.Queue("a").(*queue)
	q2 := b.Queue("b").(*queue)
	q3 := b.Queue("c").(*queue)

	b.Close()

	if !q1.closed || !q2.closed || !q3.closed {
		t.Error("not all queues are closed")
	}
}