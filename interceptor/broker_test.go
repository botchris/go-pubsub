package interceptor_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/botchris/go-pubsub"
	"github.com/botchris/go-pubsub/interceptor"
	"github.com/botchris/go-pubsub/provider/nop"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
)

func Test_NewInterceptor(t *testing.T) {
	t.Run("GIVEN an interceptor instance WHEN creating a broker with no options THEN a new broker is created", func(t *testing.T) {
		broker := nop.NewBroker()
		instance := interceptor.New(broker)

		require.NotNil(t, instance)
	})

	t.Run("GIVEN an interceptor instance WHEN creating a broker with one option THEN a new broker is created", func(t *testing.T) {
		broker := nop.NewBroker()
		spy := newPublishInterceptorSpy()
		instance := interceptor.New(broker, interceptor.WithPublishInterceptor(spy.fn))

		require.NotNil(t, instance)
	})
}

func Test_Publish(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	t.Run("GIVEN an interceptor instance with zero interceptors configured WHEN publishing a message THEN no error is raised", func(t *testing.T) {
		broker := nop.NewBroker()
		instance := interceptor.New(broker)

		require.NotNil(t, instance)
		require.NoError(t, instance.Publish(ctx, "dummyTopic", &CustomMessage{}))
	})

	t.Run("GIVEN an interceptor instance with one publish interceptor WHEN publishing a message THEN message is intercepted", func(t *testing.T) {
		broker := nop.NewBroker()
		spy := newPublishInterceptorSpy()
		instance := interceptor.New(broker, interceptor.WithPublishInterceptor(spy.fn))

		require.NotNil(t, instance)
		require.NoError(t, instance.Publish(ctx, "dummyTopic", &CustomMessage{}))

		assert.Len(t, spy.m, 1)
	})

	t.Run("GIVEN an interceptor instance with two chained publish interceptors WHEN publishing a message THEN message is captured by each interceptor in correct order", func(t *testing.T) {
		broker := nop.NewBroker()
		interceptor1 := newPublishInterceptorSpy()
		interceptor2 := newPublishInterceptorSpy()
		logger := &lockedLogs{}

		wrapper1 := func(ctx context.Context, next interceptor.PublishHandler) interceptor.PublishHandler {
			logger.write("interceptor1")
			return interceptor1.fn(ctx, next)
		}

		wrapper2 := func(ctx context.Context, next interceptor.PublishHandler) interceptor.PublishHandler {
			logger.write("interceptor2")
			return interceptor2.fn(ctx, next)
		}

		instance := interceptor.New(broker, interceptor.WithChainPublishInterceptors(wrapper1, wrapper2))

		require.NotNil(t, instance)

		assert.Len(t, interceptor1.m, 0)
		assert.Len(t, interceptor2.m, 0)
		require.NoError(t, instance.Publish(ctx, "dummyTopic", &CustomMessage{}))
		assert.Len(t, interceptor1.m, 1)
		assert.Len(t, interceptor2.m, 1)

		logs := logger.read()
		require.Len(t, logs, 2)
		assert.Equal(t, "interceptor1", logs[0])
		assert.Equal(t, "interceptor2", logs[1])
	})
}

func Test_Subscribe(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	t.Run("GIVEN an interceptor instance with one subscribe interceptor and one subscriber WHEN subscriber receives a message THEN message is captured by interceptor", func(t *testing.T) {
		broker := nop.NewBroker()
		spy := newSubscribeInterceptorSpy()
		tid := pubsub.Topic("yolo")
		instance := interceptor.New(broker, interceptor.WithSubscribeInterceptor(spy.fn))
		subscriber := pubsub.NewSubscriber(func(ctx context.Context, m proto.Message) error {
			return nil
		})

		require.NotNil(t, instance)
		require.NotNil(t, subscriber)

		assert.Len(t, spy.m, 0)
		require.NoError(t, instance.Subscribe(ctx, tid, subscriber))
		require.NoError(t, subscriber.Deliver(ctx, &CustomMessage{}))
		require.Len(t, spy.m, 1)
	})

	t.Run("GIVEN an interceptor instance with two chained subscriber interceptors and one subscriber WHEN subscriber receives a message THEN message is captured by each interceptor in correct order", func(t *testing.T) {
		broker := nop.NewBroker()
		interceptor1 := newSubscribeInterceptorSpy()
		interceptor2 := newSubscribeInterceptorSpy()
		logger := &lockedLogs{}

		wrapper1 := func(ctx context.Context, next interceptor.SubscribeMessageHandler) interceptor.SubscribeMessageHandler {
			logger.write("interceptor1")

			return interceptor1.fn(ctx, next)
		}

		wrapper2 := func(ctx context.Context, next interceptor.SubscribeMessageHandler) interceptor.SubscribeMessageHandler {
			logger.write("interceptor2")

			return interceptor2.fn(ctx, next)
		}

		tid := pubsub.Topic("yolo")
		instance := interceptor.New(broker, interceptor.WithChainSubscribeInterceptors(wrapper1, wrapper2))
		srx := &lockedCounter{}
		subscriber := pubsub.NewSubscriber(func(ctx context.Context, m proto.Message) error {
			srx.inc()

			return nil
		})

		require.NotNil(t, instance)
		require.NotNil(t, subscriber)

		assert.Len(t, interceptor1.m, 0)
		assert.Len(t, interceptor2.m, 0)
		require.NoError(t, instance.Subscribe(ctx, tid, subscriber))
		require.NoError(t, subscriber.Deliver(ctx, &CustomMessage{}))
		assert.Len(t, interceptor1.m, 1)
		assert.Len(t, interceptor2.m, 1)
		assert.Equal(t, 1, srx.read())

		logs := logger.read()
		require.Len(t, logs, 2)
		assert.Equal(t, "interceptor1", logs[0])
		assert.Equal(t, "interceptor2", logs[1])
	})
}

type publishInterceptorSpy struct {
	m  []interface{}
	fn interceptor.PublishInterceptor
}

func newPublishInterceptorSpy() *publishInterceptorSpy {
	s := &publishInterceptorSpy{}
	if s.m == nil {
		s.m = []interface{}{}
	}

	s.fn = func(ctx context.Context, next interceptor.PublishHandler) interceptor.PublishHandler {
		return func(ctx context.Context, m interface{}, topic pubsub.Topic) error {
			s.m = append(s.m, m)

			return next(ctx, m, topic)
		}
	}

	return s
}

type subscribeInterceptorSpy struct {
	m  []interface{}
	fn interceptor.SubscriberInterceptor
}

func newSubscribeInterceptorSpy() *subscribeInterceptorSpy {
	s := &subscribeInterceptorSpy{}
	if s.m == nil {
		s.m = []interface{}{}
	}

	s.fn = func(ctx context.Context, next interceptor.SubscribeMessageHandler) interceptor.SubscribeMessageHandler {
		return func(ctx context.Context, sc *pubsub.Subscriber, m interface{}) error {
			s.m = append(s.m, m)

			return next(ctx, sc, m)
		}
	}

	return s
}

type CustomMessage struct {
	emptypb.Empty
}

type lockedCounter struct {
	sync.RWMutex
	counter int
}

func (c *lockedCounter) inc() {
	c.Lock()
	defer c.Unlock()

	c.counter++
}

func (c *lockedCounter) read() int {
	c.Lock()
	defer c.Unlock()

	return c.counter
}

type lockedLogs struct {
	sync.RWMutex
	logs []string
}

func (c *lockedLogs) write(l string) {
	c.Lock()
	defer c.Unlock()

	if c.logs == nil {
		c.logs = []string{}
	}

	c.logs = append(c.logs, l)
}

func (c *lockedLogs) read() []string {
	c.Lock()
	defer c.Unlock()

	return c.logs
}
