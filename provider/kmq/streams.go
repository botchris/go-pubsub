package kmq

import (
	"context"
	"sync"

	"github.com/botchris/go-pubsub"
)

type streams struct {
	streams map[pubsub.Topic]*stream
	mu      sync.RWMutex
}

type stream struct {
	ctx    context.Context
	cancel context.CancelFunc
}

func newStreams() *streams {
	return &streams{
		streams: make(map[pubsub.Topic]*stream),
	}
}

func (s *streams) add(ctx context.Context, topic pubsub.Topic) context.Context {
	s.mu.Lock()
	defer s.mu.Unlock()

	if st, ok := s.streams[topic]; ok {
		return st.ctx
	}

	ctx, cancel := context.WithCancel(ctx)

	s.streams[topic] = &stream{
		ctx:    ctx,
		cancel: cancel,
	}

	return ctx
}

func (s *streams) remove(topic pubsub.Topic) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if st, ok := s.streams[topic]; ok {
		st.cancel()
		delete(s.streams, topic)
	}
}

func (s *streams) has(topic pubsub.Topic) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	_, ok := s.streams[topic]

	return ok
}
