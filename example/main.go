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

	broker := memory.NewBroker(memory.NopSubscriberErrorHandler)

	s1 := pubsub.NewSubscriber(func(ctx context.Context, m MyMessage) error {
		fmt.Printf("[s1] -> %+v\n", m)

		return nil
	})

	s2 := pubsub.NewSubscriber(func(ctx context.Context, m *MyMessage) error {
		fmt.Printf("[s2] -> %+v\n", m)

		return nil
	})

	s3 := pubsub.NewSubscriber(func(ctx context.Context, m string) error {
		fmt.Printf("[s3] -> %+v\n", m)

		return nil
	})

	if err := broker.Subscribe(ctx, Topic, s1); err != nil {
		panic(err)
	}

	if err := broker.Subscribe(ctx, Topic, s2); err != nil {
		panic(err)
	}

	if err := broker.Subscribe(ctx, Topic, s3); err != nil {
		panic(err)
	}

	if err := broker.Publish(ctx, Topic, MyMessage{Body: "value(hello world)"}); err != nil {
		panic(err)
	}
	// Output:
	//  [s1] -> {Body:value(hello world)}

	if err := broker.Publish(ctx, Topic, &MyMessage{Body: "pointer(hello world)"}); err != nil {
		panic(err)
	}
	// Output:
	//  [s2] -> &{Body:pointer(hello world)}

	if err := broker.Publish(ctx, Topic, "string(hello world)"); err != nil {
		panic(err)
	}
	// Output:
	//  [s3] -> string(hello world)

	if err := broker.Unsubscribe(ctx, Topic, s1); err != nil {
		panic(err)
	}

	if err := broker.Publish(ctx, Topic, MyMessage{Body: "value(hello world)"}); err != nil {
		panic(err)
	}
	// Output: <nothing>
}
