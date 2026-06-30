package gomq

import (
	"testing"
	"sync"
	"time"
	"fmt"
)

func TestConsumerAck(t *testing.T) {
	b := NewBroker()
	p := b.NewProducer("a")
	c := b.Queue("a").Subscribe()
	ch := c.Messages()

	p.Publish(0)
	p.Publish(1)
	p.Publish(2)

	<-ch
	one := <-ch
	<-ch

	pending := c.Pending()

	if len(pending) != 3 {
		t.Errorf("expected 3 pending messages, got %d", len(pending))
	}

	c.Ack(one.ID)

	pending = c.Pending()

	for _, msg := range pending {
		if msg.ID == one.ID {
			t.Errorf("message is pending after ack")
		}
	}
}

func TestUniqueMessageID(t *testing.T) {
	const messagesCount = 200000
	const producersCount = 10
	QueueBufferSize = messagesCount
	ConsumerBufferSize = messagesCount

	b := NewBroker()
	producers := [producersCount]Producer{}
	for i := 0; i < producersCount; i++ {
		producers[i] = b.NewProducer("a")
	}
	q := b.Queue("a")
	c := q.Subscribe()
	ch := c.Messages()

	errCh := make(chan error, 1)
	done := make(chan struct{})

	go func() {
		set := make(map[uint64]struct{}, messagesCount)
		deadline := time.After(5 * time.Second)

		for i := 0; i < messagesCount; i++ {
			select {
			case msg := <-ch:
				if _, ok := set[msg.ID]; ok {
					errCh <- fmt.Errorf("found message with duplicate ID")
					return
				}
				set[msg.ID] = struct{}{}
			case <-deadline:
				errCh <- fmt.Errorf("consumer timeout: received %d/%d messages", i, messagesCount)
				return
			}
		}

		close(done)
	}()

	var wg sync.WaitGroup

	publish := func(p Producer) {
		for i := 0; i < messagesCount / producersCount; i++ {
			p.Publish(i)
		}
		wg.Done()
	}

	wg.Add(producersCount)

	for i := 0; i < producersCount; i++ {
		go publish(producers[i])
	}

	wg.Wait()

	select {
	case err := <-errCh:
		t.Fatal(err)
	case <-done:
	}

	QueueBufferSize = defaultQueueBufferSize
	ConsumerBufferSize = defaultConsumerBufferSize
}