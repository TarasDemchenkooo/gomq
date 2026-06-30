package gomq

type Message struct {
	ID uint64
	Value int
}

func createMessage(q *queue, value int) Message {
	return Message{
		ID: q.nextMessageID.Add(1),
		Value: value,
	}
}