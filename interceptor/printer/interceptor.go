package printer

import (
	"context"
	"encoding/json"
	"fmt"
	"io"

	"github.com/ChristopherCastro/go-pubsub"
	"github.com/ChristopherCastro/go-pubsub/interceptor"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// PublishInterceptor intercepts each message and prints its content on the given writer
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

// SubscribeInterceptor intercepts each message that is delivered to a subscriber and prints out its content on the given writer
func SubscribeInterceptor(writer io.Writer) interceptor.SubscribeInterceptor {
	return func(ctx context.Context, next interceptor.SubscribeMessageHandler) interceptor.SubscribeMessageHandler {
		return func(ctx context.Context, s *pubsub.Subscriber, m proto.Message) error {
			log := fmt.Sprintf("[broker] received: %s\n", protojson.Format(m))
			if _, err := writer.Write([]byte(log)); err != nil {
				return err
			}

			return next(ctx, s, m)
		}
	}
}
