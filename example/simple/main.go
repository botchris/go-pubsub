package main

import (
	"context"
	"fmt"
	"time"

	"github.com/botchris/go-pubsub"
	"github.com/botchris/go-pubsub/provider/memory"
)

var Topic pubsub.Topic = "my-topic"

type MyMessage struct {
	Body string
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	broker := memory.NewBroker(memory.NopSubscriptionErrorHandler)

	s1 := pubsub.NewHandler(func(ctx context.Context, t pubsub.Topic, m MyMessage) error {
		fmt.Printf("%s -> %+v -> [s1]\n", t, m)

		return nil
	})

	s2 := pubsub.NewHandler(func(ctx context.Context, t pubsub.Topic, m *MyMessage) error {
		fmt.Printf("%s -> %+v -> [s2]\n", t, m)

		return nil
	})

	s3 := pubsub.NewHandler(func(ctx context.Context, t pubsub.Topic, m string) error {
		fmt.Printf("%s -> %+v -> [s3]\n", t, m)

		return nil
	})

	// subscribe
	{
		if err := broker.Subscribe(ctx, Topic, s1); err != nil {
			panic(err)
		}

		if err := broker.Subscribe(ctx, Topic, s2); err != nil {
			panic(err)
		}

		if err := broker.Subscribe(ctx, Topic, s3); err != nil {
			panic(err)
		}
	}

	// publish
	{
		if err := broker.Publish(ctx, Topic, MyMessage{Body: "value(hello world)"}); err != nil {
			panic(err)
		}

		if err := broker.Publish(ctx, Topic, &MyMessage{Body: "pointer(hello world)"}); err != nil {
			panic(err)
		}

		if err := broker.Publish(ctx, Topic, "string(hello world)"); err != nil {
			panic(err)
		}

		if err := broker.Unsubscribe(ctx, Topic, s1); err != nil {
			panic(err)
		}

		if err := broker.Publish(ctx, Topic, MyMessage{Body: "value(hello world)"}); err != nil {
			panic(err)
		}
	}

	// Output:
	//  {Body:value(hello world)} -> my-topic -> [s1]
	//  &{Body:pointer(hello world)} -> my-topic -> [s2]
	//  string(hello world) -> my-topic -> [s3]
	//  <nothing>
}
