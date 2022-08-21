package idgenerator

import "sync"

type generator struct {
	id uint32
	mu sync.Mutex
}

var gen = &generator{
	id: 0,
}

func GetId() uint32 {
	gen.mu.Lock()
	defer gen.mu.Unlock()
	gen.id++

	return gen.id
}
