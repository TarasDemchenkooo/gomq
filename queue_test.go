package gomq

import "testing"

func TestGetQueueName(t *testing.T) {
	b := NewBroker()

	q := b.Queue("name")

	if q.Name() != "name" {
		t.Errorf("for queue with name %q returned %q", "name", q.Name())
	}
}

func TestSubscribeToQueue(t *testing.T) {
	b := NewBroker()

	q := b.Queue("name")

	c := q.Subscribe()
	q.Subscribe()
	q.Subscribe()
	c.Close()

	if q.ConsumerCount() != 2 {
		t.Errorf("expected %d consumers, got %d", 2, q.ConsumerCount())
	}
}