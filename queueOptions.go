package gomq

type QueueOption func(*queue)

const (
	defaultQueueBufferSize = 64
)

func WithQueueBufferSize(size int) QueueOption {
	if size < 0 {
		size = defaultQueueBufferSize
	}

	return func(q *queue) {
		q.buffer = make(chan Message, size)
	}
}