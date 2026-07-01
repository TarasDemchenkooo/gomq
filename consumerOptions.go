package gomq

type ConsumerOption func(*consumer)

const (
	defaultConsumerBufferSize = 64
)

func WithConsumerBufferSize(size int) ConsumerOption {
	if size < 0 {
		size = defaultConsumerBufferSize
	}

	return func(c *consumer) {
		c.buffer = make(chan Message, size)
	}
}