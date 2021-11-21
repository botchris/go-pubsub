package redis

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/botchris/go-pubsub"
	"github.com/go-redis/redis/v8"
	"github.com/kubemq-io/kubemq-go/pkg/uuid"
)

type broker struct {
	ctx         context.Context
	cancel      context.CancelFunc
	options     *options
	redisClient *redis.Client
	attempts    map[string]int
	subs        map[pubsub.Topic]map[string]*subscription
	mu          sync.RWMutex
}

type subscription struct {
	ctx        context.Context
	cancel     context.CancelFunc
	subscriber pubsub.Subscriber
}

// NewBroker builds a new broker that uses redis streams as exchange mechanism.
//
// This implementation is based on go-micro implementation:
// https://github.com/asim/go-micro
func NewBroker(ctx context.Context, option ...Option) (pubsub.Broker, error) {
	opts := &options{
		groupID:                uuid.New(),
		logger:                 noopLogger{},
		deliverTimeout:         10 * time.Second,
		readGroupTimeout:       10 * time.Second,
		pendingIdleTime:        60 * time.Second,
		janitorConsumerTimeout: 24 * time.Hour,
		janitorFrequency:       4 * time.Hour,
		trimDuration:           5 * 24 * time.Hour,
	}

	for _, o := range option {
		o(opts)
	}

	client := redis.NewClient(&redis.Options{Addr: opts.address})
	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("%w: redis connection error", err)
	}

	ctx, cancel := context.WithCancel(ctx)

	b := &broker{
		ctx:         ctx,
		cancel:      cancel,
		options:     opts,
		redisClient: client,
		attempts:    make(map[string]int),
		subs:        make(map[pubsub.Topic]map[string]*subscription),
	}

	b.runJanitor()

	return b, nil
}

func (r *broker) Publish(ctx context.Context, topic pubsub.Topic, msg interface{}) error {
	body, isBinary := msg.([]byte)
	if !isBinary {
		return fmt.Errorf("expecting message to be of type []byte, but got `%T`", msg)
	}

	return r.redisClient.XAdd(ctx, &redis.XAddArgs{
		Stream: fmt.Sprintf("stream-%s", topic),
		Values: map[string]interface{}{"event": string(body), "attempt": 1},
	}).Err()
}

func (r *broker) Subscribe(_ context.Context, topic pubsub.Topic, subscriber pubsub.Subscriber) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	ctx, cancel := context.WithCancel(r.ctx)
	if _, ok := r.subs[topic]; !ok {
		r.subs[topic] = make(map[string]*subscription)
	}

	r.subs[topic][subscriber.ID()] = &subscription{
		ctx:        ctx,
		cancel:     cancel,
		subscriber: subscriber,
	}

	err := r.consume(topic, r.subs[topic][subscriber.ID()])
	if err != nil {
		return err
	}

	return nil
}

func (r *broker) Unsubscribe(_ context.Context, topic pubsub.Topic, subscriber pubsub.Subscriber) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	subs, ok := r.subs[topic]
	if !ok {
		return nil
	}

	sub, ok := subs[subscriber.ID()]
	if !ok {
		return nil
	}

	sub.cancel()
	delete(r.subs[topic], subscriber.ID())

	return nil
}

func (r *broker) Subscriptions(_ context.Context) (map[pubsub.Topic][]pubsub.Subscriber, error) {
	out := make(map[pubsub.Topic][]pubsub.Subscriber)

	r.mu.RLock()
	defer r.mu.RUnlock()

	for topic, subs := range r.subs {
		for _, sub := range subs {
			out[topic] = append(out[topic], sub.subscriber)
		}
	}

	return out, nil
}

func (r *broker) Shutdown(_ context.Context) error {
	r.cancel()

	return nil
}

func (r *broker) consume(t pubsub.Topic, sub *subscription) error {
	topic := fmt.Sprintf("stream-%s", t)
	lastRead := "$"

	cErr := callWithRetry(func() error {
		return r.redisClient.XGroupCreateMkStream(sub.ctx, topic, r.options.groupID, lastRead).Err()
	}, 2)

	if cErr != nil {
		if !strings.HasPrefix(cErr.Error(), "BUSYGROUP") {
			return cErr
		}
	}

	consumerName := uuid.New()

	go func() {
		defer func() {
			r.options.logger.Infof("Deleting consumer %s %s %s", topic, r.options.groupID, consumerName)

			// try to clean up the consumer
			err := callWithRetry(func() error {
				return r.redisClient.XGroupDelConsumer(sub.ctx, topic, r.options.groupID, consumerName).Err()
			}, 2)

			if err != nil {
				r.options.logger.Errorf("Error deleting consumer %s", err)
			}
		}()

		start := "-"
		done := sub.ctx.Done()

		for {
			select {
			case <-done:
				return
			default:
			}

			// sweep up any old pending messages
			var pendingCmd *redis.XPendingExtCmd

			err := callWithRetry(func() error {
				pendingCmd = r.redisClient.XPendingExt(sub.ctx, &redis.XPendingExtArgs{
					Stream: topic,
					Group:  r.options.groupID,
					Start:  start,
					End:    "+",
					Count:  50,
				})

				return pendingCmd.Err()
			}, 2)

			if err != nil && err != redis.Nil {
				r.options.logger.Errorf("Error finding pending messages %s", err)

				return
			}

			pend := pendingCmd.Val()
			if len(pend) == 0 {
				break
			}

			pendingIDs := make([]string, len(pend))
			for i, p := range pend {
				pendingIDs[i] = p.ID
			}

			var claimCmd *redis.XMessageSliceCmd
			err = callWithRetry(func() error {
				claimCmd = r.redisClient.XClaim(sub.ctx, &redis.XClaimArgs{
					Stream:   topic,
					Group:    r.options.groupID,
					Consumer: consumerName,
					MinIdle:  r.options.pendingIdleTime,
					Messages: pendingIDs,
				})

				return claimCmd.Err()
			}, 2)

			if err != nil {
				r.options.logger.Errorf("Error claiming message %s", err)

				return
			}

			msgs := claimCmd.Val()
			if err := r.processMessages(msgs, sub, t, 2); err != nil {
				r.options.logger.Errorf("Error reprocessing message %s", err)

				return
			}

			if len(pendingIDs) < 50 {
				break
			}

			start = incrementID(pendingIDs[49])
		}

		for {
			select {
			case <-done:
				return
			default:
			}

			res := r.redisClient.XReadGroup(sub.ctx, &redis.XReadGroupArgs{
				Group:    r.options.groupID,
				Consumer: consumerName,
				Streams:  []string{topic, ">"},
				Block:    r.options.readGroupTimeout,
			})

			sl, err := res.Result()

			if err != nil && !errors.Is(err, redis.Nil) {
				r.options.logger.Errorf("Error reading from stream %s", err)

				if !isTimeoutError(err) {
					return
				}

				sleepWithJitter(2 * time.Second)

				continue
			}

			if len(sl) == 0 || len(sl[0].Messages) == 0 {
				continue
			}

			if err := r.processMessages(sl[0].Messages, sub, t, 2); err != nil {
				r.options.logger.Errorf("Error processing message %s", err)
				//return
				continue
			}
		}
	}()

	return nil
}

func (r *broker) processMessages(msgs []redis.XMessage, sub *subscription, t pubsub.Topic, retryLimit int) error {
	topic := streamName(t)

	for _, v := range msgs {
		vid := v.ID
		evBytes := v.Values["event"]

		bStr, ok := evBytes.(string)
		if !ok {
			r.options.logger.Warnf("Failed to convert to bytes, discarding %s", vid)
			r.redisClient.XAck(sub.ctx, topic, r.options.groupID, vid)
			continue
		}

		ev := []byte(bStr)

		attemptsKey := fmt.Sprintf("%s:%s:%s", topic, r.options.groupID, vid)
		r.mu.Lock()
		r.attempts[attemptsKey], _ = strconv.Atoi(v.Values["attempt"].(string))
		r.mu.Unlock()

		dCtx, cancel := context.WithTimeout(sub.ctx, r.options.deliverTimeout)
		dErr := sub.subscriber.Deliver(dCtx, t, ev)
		cancel()

		ack := func() error {
			r.mu.Lock()
			delete(r.attempts, attemptsKey)
			r.mu.Unlock()

			return r.redisClient.XAck(sub.ctx, topic, r.options.groupID, vid).Err()
		}

		nack := func() error {
			// no way to nack a message. Best you can do is to ack and readd
			if err := r.redisClient.XAck(sub.ctx, topic, r.options.groupID, vid).Err(); err != nil {
				return err
			}

			r.mu.RLock()
			attempt := r.attempts[attemptsKey]
			r.mu.RUnlock()

			if retryLimit > 0 && attempt > retryLimit {
				// don't read
				r.mu.Lock()
				delete(r.attempts, attemptsKey)
				r.mu.Unlock()

				return nil
			}

			return r.redisClient.XAdd(sub.ctx,
				&redis.XAddArgs{
					Stream: topic,
					Values: map[string]interface{}{
						"event":   string(ev),
						"attempt": attempt + 1,
					},
				},
			).Err()
		}

		if dErr == nil {
			if err := ack(); err != nil {
				r.options.logger.Warnf("ack failed")
			}

			continue
		}

		if err := nack(); err != nil {
			r.options.logger.Warnf("nack failed")
		}
	}

	return nil
}
