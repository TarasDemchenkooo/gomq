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

func TestSlowConsumer(t *testing.T) {
	const messagesCount = 1_000_000
	const producersCount = 10

	b := NewBroker()
	q := b.Queue("a", WithQueueBufferSize(messagesCount))
	c := q.Subscribe(WithConsumerBufferSize(10))
	ch := c.Messages()

	producers := [producersCount]Producer{}
	for i := 0; i < producersCount; i++ {
		producers[i] = b.NewProducer("a")
	}

	errCh := make(chan error, 1)
	done := make(chan struct{})

	go func() {
		deadline := time.After(time.Second * messagesCount / 100_000)

		for i := 0; i < messagesCount; i++ {
			select {
			case <-ch:
			case <-deadline:
				errCh <- fmt.Errorf("consumer timeout: received %d/%d messages", i, messagesCount)
				return
			}
		}

		close(done)
	}()

	var wg sync.WaitGroup

	publish := func(p Producer) {
		defer wg.Done()

		for i := 0; i < messagesCount / producersCount; i++ {
			p.Publish(i)
		}
	}

	for i := 0; i < producersCount; i++ {
		wg.Add(1)
		go publish(producers[i])
	}

	wg.Wait()

	select {
	case err := <-errCh:
		t.Fatal(err)
	case <-done:
	}
}