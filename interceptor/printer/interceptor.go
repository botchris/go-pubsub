package printer

import (
	"context"
	"encoding/json"
	"fmt"
	"io"

	"github.com/ChristopherCastro/go-pubsub"
	"github.com/ChristopherCastro/go-pubsub/interceptor"
)

// PublishInterceptor intercepts each message and prints its content on the given writer in JSON format
func PublishInterceptor(writer io.Writer) interceptor.PublishInterceptor {
	return func(ctx context.Context, next interceptor.PublishHandler) interceptor.PublishHandler {
		return func(ctx context.Context, m interface{}, topic pubsub.Topic) error {
			j, err := json.Marshal(m)
			if err != nil {
				return err
			}

			log := fmt.Sprintf("[broker] publishing @ %v: %s\n", topic, string(j))
			if _, err := writer.Write([]byte(log)); err != nil {
				return err
			}

			return next(ctx, m, topic)
		}
	}
}

// SubscribeInterceptor intercepts each message that is delivered to a subscribers and prints out its content on the
// given writer in JSON format
func SubscribeInterceptor(writer io.Writer) interceptor.SubscriberInterceptor {
	return func(ctx context.Context, next interceptor.SubscribeMessageHandler) interceptor.SubscribeMessageHandler {
		return func(ctx context.Context, s *pubsub.Subscriber, m interface{}) error {
			j, err := json.Marshal(m)
			if err != nil {
				return err
			}

			log := fmt.Sprintf("[broker] received: %s\n", string(j))
			if _, err := writer.Write([]byte(log)); err != nil {
				return err
			}

			return next(ctx, s, m)
		}
	}
}
