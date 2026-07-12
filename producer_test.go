package gomq

import (
	"testing"
	"time"
)

func TestProducerPublishing(t *testing.T) {
	b := NewBroker()

	p := b.NewProducer("a")

	for i := 0; i < defaultQueueBufferSize; i++ {
		err := p.Publish(i)

		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestProducerFilters(t *testing.T) {
	b := NewBroker()

	var filter Filter = func(m Message) bool {
		return m.Value >= 10 || m.Value == 0
	}

	p := b.NewProducer("a").
		WithZeroFilter().
		WithFilter(filter)

	q := b.Queue("a")
	c := q.Subscribe()

	cases := []struct{
		value int
		pass bool
	}{
		{0, false},
		{5, false},
		{11, true},
	}

	for _, c := range cases {
		p.Publish(c.value)
	}

	select {
	case msg := <-c.Messages():
		if msg.Value != 11 {
			t.Errorf("expected message with Value %d, got %d", 11, msg.Value)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout: message was not delivered")
	}
}

func TestProducerClosing(t *testing.T) {
	b := NewBroker()
	p := b.NewProducer("a")
	
	p.Close()
	err := p.Publish(0)

	if err == nil {
		t.Error("successful publish to the closed queue")
	}
}