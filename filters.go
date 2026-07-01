package gomq

type Filter func(m Message) bool

var zeroFilter Filter = func(m Message) bool {
	return m.Value != 0
}

var evenFilter Filter = func(m Message) bool {
	return m.Value % 2 == 0
}

var oddFilter Filter = func(m Message) bool {
	return m.Value % 2 != 0
}