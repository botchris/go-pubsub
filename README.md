# Go PubSub

The `pubsub` package is a simple package for implementing publish-subscribe
asynchronous tasks in Golang. It allows writing publishers and subscribers fully
statically typed, and swap out Broker implementations (e.g. Memory, AWS SQS, 
etc.) as required.

This package imposes no restrictions on how messages should be represented, the 
idea is to keep subscribers agnostic to transport concerns and be fully typed
using golang definitions. How messages are encoded/decoded in order to be
transported over the network is up to the provider implementation. See
KubeMQ or AWS implementation for an example of how to encode/decode messages.

![broker overview][broker-overview]

## What is a PubSub System

A PubSub system is a messaging system that has, as its name implies, two
components: Publisher of messages and subscriber to messages. In contrast to
synchronous communication, the publisher doesn't have to wait for a message to
be received, as well as the receiver doesn't have to be online to retrieve
messages sent earlier. As such, a PubSub system acts like a buffer for
asynchronous messaging.

## Features

- Multi-topic support, the same subscriber may listen for messages on
  multiple topics at the same time.
- [Hybrid message filtering][hybrid-filtering], subscriber are free to
  decide whether they want to receive messages for a concrete type or not
  (content-based), or just receive everything that is pushed to a given
  topic/s (topic-based).
- Pluggable providers. Just implement the `Broker` interface.

## Providers

Providers are concrete implementations of the Broker interface. Examples of
providers could be messaging services such as Google's PubSub, Amazon's SNS
or Nats.io. Broker interface acts as a generalization for suh services.

The `pubsub` package comes with a set of built-in providers:

- `memory`: a simple Broker which allows communicating local process of your 
  system by interchanging messages, which can be used as a simple "Message 
  Bus" replacement.
- `nop`: s simple NOP broker implementation that can be used for testing.
- `sns`: a Broker that uses AWS SNS and AWS SQS.
- `kmq`: a KubeMQ implementation of the Broker interface.

## Middleware

The `pubsub` package provides a simple API for implementing and installing
interceptors. A middleware intercepts each message being published or
being delivered to subscribers. Users can use middleware to do logging, metrics
collection, and many other functionalities that can be shared across PubSub
Providers.

To use middleware capabilities you must simply wrap your broker using any of 
the provided middlewares, example:

```go
broker := printer.NewPrinterMiddleware(myProvider, os.Stdout)
```

Middlewares act as a wrapper for the given broker by adding interception 
capabilities. Included middlewares are:

- `codec`: a middleware that encodes and decodes messages using the given codec.
- `printer`: a simple middleware that prints each message to the given output.
- `recover`: a middleware that recovers from panics.
- `retry`: a middleware that retries publishing messages if the broker fails.

## TODO

- [ ] Kafka provider
- [ ] Google's Pub/Sub
- [x] Add protobuf support as a middleware codec
- [x] Recovery middleware for dealing with panics
- [x] Retry middleware for dealing with unreliable providers/subscribers

## Example

```go
package main

import (
  "context"
  "fmt"
  "time"

  "github.com/botchris/go-pubsub"
  "github.com/botchris/go-pubsub/provider/memory"
)

var myTopic pubsub.Topic = "my-topic"

type MyMessage struct {
  Body string
}

func main() {
  ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
  defer cancel()

  // Declare subscribers. This usually takes place within a `init()` function
  s1 := pubsub.NewSubscriber(func(ctx context.Context, t pubsub.Topic, m MyMessage) error {
    fmt.Printf("%s -> %+v -> [s1]\n", t, m)

    return nil
  })

  s2 := pubsub.NewSubscriber(func(ctx context.Context, t pubsub.Topic, m *MyMessage) error {
    fmt.Printf("%s -> %+v -> [s2]\n", t, m)

    return nil
  })

  s3 := pubsub.NewSubscriber(func(ctx context.Context, t pubsub.Topic, m string) error {
    fmt.Printf("%s -> %+v -> [s3]\n", t, m)

    return nil
  })

  // Declare provider.
  broker := memory.NewBroker(memory.NopSubscriberErrorHandler)

  // Bind subscribers to a topic.
  broker.Subscribe(ctx, myTopic, s1)
  broker.Subscribe(ctx, myTopic, s2)
  broker.Subscribe(ctx, myTopic, s3)

  // Publish some messages, we drop errors on purpose for simplification reasons
  _ = broker.Publish(ctx, myTopic, MyMessage{Body: "value(hello world)"})
  _ = broker.Publish(ctx, myTopic, &MyMessage{Body: "pointer(hello world)"})
  _ = broker.Publish(ctx, myTopic, "string(hello world)")

  // Output:
  //  {Body:value(hello world)} -> my-topic -> [s1]
  //  &{Body:pointer(hello world)} -> my-topic -> [s2]
  //  string(hello world) -> my-topic -> [s3]
}
```

[broker-overview]: doc/broker.overview.png
[hybrid-filtering]: https://en.wikipedia.org/wiki/Publish%E2%80%93subscribe_pattern#Message_filtering
