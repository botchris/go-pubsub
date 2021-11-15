package printer

import (
	"context"
	"encoding/json"
	"fmt"
	"io"

	"github.com/botchris/go-pubsub"
)

// PublishInterceptor intercepts each message and prints its content on the given writer in JSON format
func PublishInterceptor(writer io.Writer) pubsub.PublishInterceptor {
	return func(ctx context.Context, next pubsub.PublishHandler) pubsub.PublishHandler {
		return func(ctx context.Context, topic pubsub.Topic, m interface{}) error {
			j, err := json.Marshal(m)
			if err != nil {
				return err
			}

			log := fmt.Sprintf("[middleware] publishing @ %v: %s\n", topic, string(j))
			if _, err := writer.Write([]byte(log)); err != nil {
				return err
			}

			return next(ctx, topic, m)
		}
	}
}

// SubscriberInterceptor intercepts each message that is delivered to a subscribers and prints out its content on
// the given writer in JSON format
func SubscriberInterceptor(writer io.Writer) pubsub.SubscriberInterceptor {
	return func(ctx context.Context, next pubsub.SubscriberMessageHandler) pubsub.SubscriberMessageHandler {
		return func(ctx context.Context, s *pubsub.Subscriber, t pubsub.Topic, m interface{}) error {
			j, err := json.Marshal(m)
			if err != nil {
				return err
			}

			log := fmt.Sprintf("[middleware] received: %s\n", string(j))
			if _, err := writer.Write([]byte(log)); err != nil {
				return err
			}

			return next(ctx, s, t, m)
		}
	}
}
