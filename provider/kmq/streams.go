package kmq

import (
	"context"
	"sync"

	"github.com/botchris/go-pubsub"
)

type streams struct {
	streams map[pubsub.Topic]map[string]*stream
	mu      sync.RWMutex
}

type stream struct {
	ctx    context.Context
	cancel context.CancelFunc
	queue  string
}

func newStreams() *streams {
	return &streams{
		streams: make(map[pubsub.Topic]map[string]*stream),
	}
}

func (s *streams) add(ctx context.Context, topic pubsub.Topic, queue string) context.Context {
	s.mu.Lock()
	defer s.mu.Unlock()

	if queues, ok := s.streams[topic]; ok {
		if st, ok := queues[queue]; ok {
			return st.ctx
		}
	}

	if _, ok := s.streams[topic]; !ok {
		s.streams[topic] = make(map[string]*stream, 0)
	}

	ctx, cancel := context.WithCancel(ctx)

	s.streams[topic][queue] = &stream{
		ctx:    ctx,
		cancel: cancel,
		queue:  queue,
	}

	return ctx
}

func (s *streams) remove(topic pubsub.Topic, queue string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	queues, ok := s.streams[topic]
	if !ok {
		return
	}

	if st, ok := queues[queue]; ok {
		st.cancel()
		delete(queues, queue)
	}
}

func (s *streams) has(topic pubsub.Topic, queue string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if queues, ok := s.streams[topic]; ok {
		if _, ok := queues[queue]; ok {
			return true
		}
	}

	return false
}
