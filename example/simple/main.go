package main

import (
	"context"
	"fmt"
	"time"

	"github.com/botchris/go-pubsub"
	"github.com/botchris/go-pubsub/provider/memory"
)

var myTopic pubsub.Topic = "my-topic"

type myMessage struct {
	Body string
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	broker := memory.NewBroker()

	h1 := pubsub.NewHandler(func(ctx context.Context, t pubsub.Topic, m myMessage) error {
		fmt.Printf("%s -> %+v -> [s1]\n", t, m)

		return nil
	})

	h2 := pubsub.NewHandler(func(ctx context.Context, t pubsub.Topic, m *myMessage) error {
		fmt.Printf("%s -> %+v -> [s2]\n", t, m)

		return nil
	})

	h3 := pubsub.NewHandler(func(ctx context.Context, t pubsub.Topic, m string) error {
		fmt.Printf("%s -> %+v -> [s3]\n", t, m)

		return nil
	})

	// subscribe
	var s1 pubsub.Subscription
	{
		s1l, err := broker.Subscribe(ctx, myTopic, h1)
		if err != nil {
			panic(err)
		}

		s1 = s1l

		if _, sErr := broker.Subscribe(ctx, myTopic, h2); sErr != nil {
			panic(sErr)
		}

		if _, sErr := broker.Subscribe(ctx, myTopic, h3); sErr != nil {
			panic(sErr)
		}
	}

	// publish
	{
		if err := broker.Publish(ctx, myTopic, myMessage{Body: "value(hello world)"}); err != nil {
			panic(err)
		}

		if err := broker.Publish(ctx, myTopic, &myMessage{Body: "pointer(hello world)"}); err != nil {
			panic(err)
		}

		if err := broker.Publish(ctx, myTopic, "string(hello world)"); err != nil {
			panic(err)
		}

		// unsubscribe S1
		if err := s1.Unsubscribe(); err != nil {
			panic(err)
		}

		// this will noop
		if err := broker.Publish(ctx, myTopic, myMessage{Body: "value(hello world)"}); err != nil {
			panic(err)
		}
	}

	// Output:
	//  {Body:value(hello world)} -> my-topic -> [s1]
	//  &{Body:pointer(hello world)} -> my-topic -> [s2]
	//  string(hello world) -> my-topic -> [s3]
	// <nothing>
}
