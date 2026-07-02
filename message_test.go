package gomq

import (
	"testing"
	"time"
)

func TestUniqueMessageID(t *testing.T) {
	messagesCount := 200_000

	b := NewBroker()
	q := b.Queue("a", WithQueueBufferSize(messagesCount))
	p := b.NewProducer("a")
	c := q.Subscribe()
	ch := c.Messages()

	for i := 0; i < messagesCount; i++ {
		p.Publish(i)
	}

	set := make(map[uint64]struct{}, messagesCount)
	deadline := time.After(time.Second)

	for i := 0; i < messagesCount; i++ {
		select {
		case msg := <-ch:
			if _, ok := set[msg.ID]; ok {
				t.Errorf("found message with duplicate id")
			}

			set[msg.ID] = struct{}{}
		case <-deadline:
			t.Errorf("timeout: consumer read %d/%d messages", i, messagesCount)
		}
	}
}