# Go PubSub

The `go-pubsub` package is a simple package for implementing publish-subscribe
asynchronous tasks in Golang. It allows writing publishers and subscribers fully
statically typed, and swap out Broker implementations (e.g. Memory, AWS SQS, 
etc.) as required.

This package imposes no restrictions on how messages should be represented, the 
idea is to keep subscribers agnostic to transport concerns and be fully typed
using golang definitions. How messages are encoded/decoded in order to be
transported over the network is up to the provider implementation in 
combination with Codec middlewares.

![broker overview][broker-overview]

## What is a PubSub system

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
- Pluggable providers. Just implement the `Broker` interface. See below for 
  a list of built-in providers

## Providers

Providers are concrete implementations of the Broker interface. Examples of
providers could be messaging services such as Google's PubSub, Amazon's SNS
or Nats.io. The Broker interface acts as a generalization for such services.

The `go-pubsub` package comes with a set of built-in providers:

- `memory`: A simple, synchronous Broker for in-process message communication, 
  acting as a straightforward "Message Bus" or "Event Dispatcher" replacement. 
  It ensures message processing in publication order and allows error feedback 
  to the consumer if a subscriber fails to process a message. Notably, it executes 
  all subscribers, requiring idempotent handling for retry functionality, as 
  subscribers may be invoked multiple times until successful processing.
- `nop`: a simple NO-OP broker implementation that can be used for testing.
- `redis`: a broker that uses redis streams as PubSub mechanism.
- `snssqs`: a Broker that uses AWS SNS and AWS SQS.
- `kmq`: a KubeMQ implementation of the Broker interface.

### Creating your own provider

This packages moves around the `Broker` interface definition, which is the 
central piece for dealing with PubSub systems. The `Broker` interface is a 
composition of three independent interfaces which can be used in order to keep
you application concerns clean and separated:

```go
type Broker interface {
	Publisher
	Subscriber
	Shutdowner
}

type Publisher interface {
	Publish(ctx context.Context, topic Topic, m interface{}) error
}

type Subscriber interface {
	Subscribe(ctx context.Context, topic Topic, handler Handler, option ...SubscribeOption) (Subscription, error)
}

type Shutdowner interface {
	Shutdown(ctx context.Context) error
}
```

Creating your own provider is as simple as implementing the Broker interface 
described above.

## Middleware

A middleware acts as a wrapper for a Broker implementation. It can be used 
to intercept each message being published or being delivered to subscribers. 
Users can use middleware to do logging, metrics collection, and many other 
functionalities that can be shared across PubSub Providers.

To use middleware capabilities you must simply wrap your broker using any of 
the provided middlewares, example:

```go
broker := printer.NewPrinterMiddleware(myProvider, os.Stdout)
```

Included middlewares are:

- `codec`: a middleware that encodes and decodes messages using the given codec.
- `lifecycle`: a middleware that allows to bind to broker lifecycle events.
- `printer`: a simple middleware that prints each message to the given output.
- `recover`: a middleware that recovers from panics.
- `retry`: a middleware that retries publishing messages if the broker fails.

Middlewares can be combined by wrapping each other, for example:

```go
broker := memory.NewMemoryBroker() 
broker = printer.NewPrinterMiddleware(myProvider, os.Stdout)
broker = codec.NewCodecMiddleware(broker, codec.JSON)
broker = recovery.NewRecoveryMiddleware(broker, func(ctx context.Context, p interface{}) error {
    println("panic:", p)
	
	return nil 
})
```

Please note that middlewares are applied in reverse order they are wrapped, so in
the example above, the `recovery` middleware will be applied first, then the
`codec` middleware, and so on:

Recovery -> Codec -> Printer -> Memory-Broker

## TODO

- [ ] Kafka provider
- [ ] Google's Pub/Sub provider
- [ ] Nats.io provider
- [x] Redis provider
- [x] Add protobuf support as a middleware codec
- [x] Recovery middleware for dealing with panics
- [x] Retry middleware for dealing with unreliable providers/handlers

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

type myMessage struct {
  Body string
}

func main() {
  ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
  defer cancel()

  broker := memory.NewBroker()

  // Define handlers 
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

  // Subscribe to topic
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

  // Publish test messages
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

    // Unsubscribe S1
    if err := s1.Unsubscribe(); err != nil {
      panic(err)
    }

    // This will noop
    if err := broker.Publish(ctx, myTopic, myMessage{Body: "value(hello world)"}); err != nil {
      panic(err)
    }
  }

  // Output:
  //  {Body:value(hello world)} -> my-topic -> [s1]
  //  &{Body:pointer(hello world)} -> my-topic -> [s2]
  //  string(hello world) -> my-topic -> [s3]
  //  <nothing>
}
```

[broker-overview]: doc/broker.overview.png
[hybrid-filtering]: https://en.wikipedia.org/wiki/Publish%E2%80%93subscribe_pattern#Message_filtering
