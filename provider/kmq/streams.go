package kmq

import (
	"context"
	"sync"

	"github.com/botchris/go-pubsub"
)

type streams struct {
	streams map[pubsub.Topic]struct {
		ctx    context.Context
		cancel context.CancelFunc
	}
	mu sync.RWMutex
}

func newStreams() *streams {
	return &streams{
		streams: make(map[pubsub.Topic]struct {
			ctx    context.Context
			cancel context.CancelFunc
		}),
	}
}

func (s *streams) add(ctx context.Context, topic pubsub.Topic) context.Context {
	s.mu.Lock()
	defer s.mu.Unlock()

	if hit, ok := s.streams[topic]; ok {
		return hit.ctx
	}

	ctx, cancel := context.WithCancel(ctx)

	s.streams[topic] = struct {
		ctx    context.Context
		cancel context.CancelFunc
	}{
		ctx:    ctx,
		cancel: cancel,
	}

	return ctx
}

func (s *streams) remove(topic pubsub.Topic) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.streams[topic]; !ok {
		return
	}

	s.streams[topic].cancel()
	delete(s.streams, topic)
}

func (s *streams) has(topic pubsub.Topic) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	_, ok := s.streams[topic]

	return ok
}
