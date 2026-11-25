package redis

import (
	"time"
)

func (r *broker) runJanitor() {
	done := r.ctx.Done()

	// Some times it's possible that a consumer group has old consumers that
	// have failed to be deleted. Janitor will clean up any consumers that
	// haven't been seen for X duration
	go func() {
		for {
			select {
			case <-done:
				return
			case <-time.After(r.options.janitorFrequency):
				if err := r.cleanupConsumers(); err != nil {
					r.options.logger.Errorf("Error cleaning up consumers: %s", err)
				}
			}
		}
	}()

}

// cleanupConsumers reads groups and consumers with XINFO GROUPS and XINFO
// CONSUMERS, not XINFO STREAM FULL: go-redis rejects any field it does not
// know, and STREAM FULL is the reply Redis keeps extending (8.12 added the
// idmp-* and iids-* fields).
func (r *broker) cleanupConsumers() error {
	keys, err := r.redisClient.Keys(r.ctx, streamName("*")).Result()
	if err != nil {
		return err
	}

	for _, stream := range keys {
		groups, err := r.redisClient.XInfoGroups(r.ctx, stream).Result()
		if err != nil {
			r.options.logger.Errorf("Error listing groups of %s: %s", stream, err)
			continue
		}

		for _, g := range groups {
			consumers, err := r.redisClient.XInfoConsumers(r.ctx, stream, g.Name).Result()
			if err != nil {
				r.options.logger.Errorf("Error listing consumers of %s %s: %s", stream, g.Name, err)
				continue
			}

			for _, c := range consumers {
				// Idle is the time since this consumer last read from the stream.
				// This means if the stream is low volume you could delete currently connected consumers
				// This isn't a massive problem because the clients should reconnect with a new consumer
				if c.Idle < r.options.janitorConsumerTimeout {
					continue
				}

				if err := r.redisClient.XGroupDelConsumer(r.ctx, stream, g.Name, c.Name).Err(); err != nil {
					r.options.logger.Errorf("Error deleting consumer %s %s %s: %s", stream, g.Name, c.Name, err)
					continue
				}
			}
		}
	}

	return nil
}
