package gomq

import "fmt"

type Producer interface {
	// push message to the queue
	// if the queue's buffer is full, just drops message and returns an error
	Publish(value int) error
	
	// applies your custom filter before publishing messages
	WithFilter(f Filter) Producer

	// built-in filters
	WithZeroFilter() Producer
	WithEvenFilter() Producer
	WithOddFilter() Producer
	WithMinMaxFilter(min, max int) Producer

	// closes queue
	Close()
}

type producer struct {
	q *queue
	filters []Filter
}

func (p *producer) Publish(value int) error {
	message := createMessage(p.q, value)

	for _, filter := range p.filters {
		if !filter(message) {
			return nil
		}
	}

	p.q.mu.RLock()
	defer p.q.mu.RUnlock()

	if p.q.closed {
		return fmt.Errorf("queue with name %q was already closed", p.q.name)
	}

	select {
	case p.q.buffer <- message:
	default:
		return fmt.Errorf("queue with name %q is full", p.q.name)
	}

	return nil
}

func (p *producer) WithFilter(f Filter) Producer {
	p.filters = append(p.filters, f)
	return p
}

func (p *producer) WithZeroFilter() Producer {
	p.filters = append(p.filters, zeroFilter)
	return p
}

func (p *producer) WithEvenFilter() Producer {
	p.filters = append(p.filters, evenFilter)
	return p
}

func (p *producer) WithOddFilter() Producer {
	p.filters = append(p.filters, oddFilter)
	return p
}

func (p *producer) WithMinMaxFilter(min, max int) Producer {
	var minMaxFilter Filter = func(m Message) bool {
		return m.Value >= min && m.Value <= max
	}

	p.filters = append(p.filters, minMaxFilter)
	return p
}

func (p *producer) Close() {
	p.q.closeQueue()
}

func newProducer(q *queue) *producer {
	return &producer{
		q: q,
		filters: make([]Filter, 0),
	}
}