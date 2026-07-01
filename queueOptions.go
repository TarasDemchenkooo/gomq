package gomq

import "time"

type QueueOption func(*queue)

const (
	defaultQueueBufferSize = 64
	defaultQueueClosingTimeout = 5 * time.Second
)

func WithQueueBufferSize(size int) QueueOption {
	if size < 0 {
		size = defaultQueueBufferSize
	}

	return func(q *queue) {
		q.buffer = make(chan Message, size)
	}
}

func WithQueueClosingTimeout(t time.Duration) QueueOption {
	return func(q *queue) {
		q.timeout = t
	}
}