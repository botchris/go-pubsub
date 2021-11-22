package rr_test

import (
	"context"
	"testing"
	"time"

	"github.com/botchris/go-pubsub"
	"github.com/botchris/go-pubsub/provider/kmq/rr"
	"github.com/kubemq-io/kubemq-go/pkg/uuid"
	"github.com/stretchr/testify/require"
)

func TestSubscriptionsCollection(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	t.Run("GIVEN an empty collection", func(t *testing.T) {
		topic := pubsub.Topic("dummy")
		collection := rr.NewSubscriptionsCollection()

		t.Run("WHEN asking for a topic THEN collection returns topics does not exists", func(t *testing.T) {
			require.False(t, collection.HasTopic(topic))
		})

		t.Run("WHEN adding a subscription to topic dummy THEN collection eturns topic exists", func(t *testing.T) {
			handler := pubsub.NewHandler(func(ctx context.Context, topic pubsub.Topic, msg interface{}) error { return nil })
			s, err := dummySubscription(ctx, topic, handler)
			require.NoError(t, err)

			collection.Add(s)
			require.True(t, collection.HasTopic(topic))
		})
	})

	t.Run("GIVEN a collection with one topic and one subscription", func(t *testing.T) {
		topic := pubsub.Topic("dummy")
		collection := rr.NewSubscriptionsCollection()
		handler := pubsub.NewHandler(func(ctx context.Context, topic pubsub.Topic, msg interface{}) error { return nil })

		s, err := dummySubscription(ctx, topic, handler)
		require.NoError(t, err)

		collection.Add(s)
		require.True(t, collection.HasTopic(topic))

		t.Run("WHEN removing the last subscription for a topic THEN collection is empty", func(t *testing.T) {
			collection.RemoveFromTopic(topic, s.ID())
			require.False(t, collection.HasTopic(topic))
		})
	})

	t.Run("GIVEN a collection with one topic and one subscription", func(t *testing.T) {
		topic := pubsub.Topic("dummy")
		collection := rr.NewSubscriptionsCollection()
		handler := pubsub.NewHandler(func(ctx context.Context, topic pubsub.Topic, msg interface{}) error { return nil })

		s, err := dummySubscription(ctx, topic, handler)
		require.NoError(t, err)

		collection.Add(s)
		require.True(t, collection.HasTopic(topic))

		t.Run("WHEN gracefully stopping THEN collection is empty and subscription is stopped", func(t *testing.T) {
			collection.GracefulStop()

			require.False(t, collection.HasTopic(topic))
		})
	})

	t.Run("GIVEN a collection with one topic and two subscription sharing the same queue", func(t *testing.T) {
		topic := pubsub.Topic("dummy")
		collection := rr.NewSubscriptionsCollection()

		h1 := pubsub.NewHandler(func(ctx context.Context, topic pubsub.Topic, msg interface{}) error { return nil })
		h2 := pubsub.NewHandler(func(ctx context.Context, topic pubsub.Topic, msg interface{}) error { return nil })

		s1, err := dummySubscription(ctx, topic, h1, pubsub.WithGroup("q1"))
		require.NoError(t, err)

		s2, err := dummySubscription(ctx, topic, h2, pubsub.WithGroup("q1"))
		require.NoError(t, err)

		collection.Add(s1)
		collection.Add(s2)
		require.True(t, collection.HasTopic(topic))

		t.Run("WHEN asking for a candidate twice THEN both subscription are returned", func(t *testing.T) {
			r1 := collection.Receptors(topic)
			require.Len(t, r1, 1)

			r2 := collection.Receptors(topic)
			require.Len(t, r2, 1)

			require.NotEqualValues(t, r1[0].ID(), r2[0].ID())
		})
	})
}

func dummySubscription(ctx context.Context, topic pubsub.Topic, handler pubsub.Handler, option ...pubsub.SubscribeOption) (pubsub.StoppableSubscription, error) {
	opts := pubsub.NewSubscribeOptions(option...)

	return pubsub.NewStoppableSubscription(ctx, uuid.New(), topic, handler, func() error { return nil }, opts)
}
