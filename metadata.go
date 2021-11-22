package pubsub

import (
	"sync"
)

// Metadata is the interface that wraps the basic Get and Set methods.
type Metadata interface {
	// Get returns the value associated with this metadata for key, or nil if no
	// value is associated with key. Successive calls to Value with the same key
	// returns the same result.
	Get(key interface{}) interface{}

	// Set stores the given value under the specified key, overwrites if
	// already exists.
	Set(key, val interface{}) Metadata
}

// NewMetadata returns a new thread-safe Metadata instance which can be safely
// used by multiple goroutines.
func NewMetadata() Metadata {
	return &metadata{
		items: make(map[interface{}]interface{}),
	}
}

// Metadata is a map of arbitrary string key/value pairs.
type metadata struct {
	items map[interface{}]interface{}
	mu    sync.RWMutex
}

func (m *metadata) Get(key interface{}) interface{} {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if item, ok := m.items[key]; ok {
		return item
	}

	return nil
}

func (m *metadata) Set(key, val interface{}) Metadata {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.items[key] = val

	return m
}
