package redis

import (
	"strconv"
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
					r.options.logger.Errorf("Error cleaning up consumers")
				}
			}
		}
	}()

}

func (r *broker) cleanupConsumers() error {
	now := time.Now()
	keys, err := r.redisClient.Keys(r.ctx, "stream-*").Result()
	if err != nil {
		return err
	}

	for _, streamName := range keys {
		s, err := r.redisClient.XInfoStreamFull(r.ctx, streamName, 1).Result()
		if err != nil {
			continue
		}

		for _, g := range s.Groups {
			for _, c := range g.Consumers {
				// Seen time is the last time this consumer read a message successfully.
				// This means if the stream is low volume you could delete currently connected consumers
				// This isn't a massive problem because the clients should reconnect with a new consumer
				if c.SeenTime.Add(r.options.janitorConsumerTimeout).After(now) {
					continue
				}

				//logger.Infof("Cleaning up consumer %s, it is %s old", c.Name, time.Since(c.SeenTime))
				if err := r.redisClient.XGroupDelConsumer(r.ctx, streamName, g.Name, c.Name).Err(); err != nil {
					r.options.logger.Errorf("Error deleting consumer %s %s %s: %s", streamName, g.Name, c.Name, err)
					continue
				}
			}

		}

		d := r.options.trimDuration
		if err := r.redisClient.XTrimMinID(r.ctx, streamName, strconv.FormatInt(time.Now().Add(-d).Unix()*1000, 10)); err != nil {
			r.options.logger.Errorf("Error trimming %s", err)
		}
	}

	return nil
}
