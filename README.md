# Overview

The `pubsub` package provides a simple helper library for doing publish-subscribe asynchronous tasks in Golang,
usually in a web or microservice. `pubsub` allows you to write publishers and subscribers, fully statically typed, and
swap out Broker implementations (e.g. Memory, AWS SQS, etc) as required.

![broker overview][broker-overview]

# What is a PubSub System?

A PubSub system is a messaging system that has, as its name implies, two components: Publisher of messages and
subscriber to messages. In contrast to synchronous communication, the publisher doesn't have to wait for a message to
be received, as well as the receiver doesn't have to be online to retrieve messages sent earlier. As such, a PubSub
system acts like a buffer for asynchronous messaging.

# Features

- Multi-topic support, the same subscriber may listen for messages on multiple topics at the same time.
- [Hybrid message filtering](https://en.wikipedia.org/wiki/Publish%E2%80%93subscribe_pattern#Message_filtering), subscriber are free
  to decide whether they want to receive messages for a concrete type or not (content-based), or just receive everything that is pushed
  to a given topic/s (topic-based). 
- Pluggable providers.

# Providers

Providers are concrete implementations of the Broker interface. Examples of providers could be messaging services such as
Google's PubSub, Amazon's SNS or Nats.io. Broker interface acts as a generalization for suh services.

The `pubsub` package comes with a built-in `memory` provider: a simple Broker which allows communicating local process
of your system by interchanging messages, which can be used as a simple "Message Bus" replacement.

# Interceptors

The `pubsub` package provides a simple API to implement and install interceptors. Interceptor intercepts each message
being published or being delivered to Subscribers. Users can use interceptors to do logging, metrics collection, and
many other functionalities that can be shared across PubSub Providers.

To use interception capabilities you must simply instantiate a new Interceptor as follows:

```go
provider := interceptor.New(myProvider, ...opts)
```

This "Interceptors" acts as a wrapper for the given provider by adding interception capabilities.
Moreover, the interceptor itself can be used interchangeably as a Broker; it implements the Broker interface. 

## Publishing Interceptors

Allows to intercept each message before its handled by underlying Provider.

```go
provider := interceptor.New(myProvider,
    interceptor.WithPublishInterceptor(myPublishingInterceptor),
)
```

## Subscribing Interceptors

Provides a hook to intercept each message before it gets delivered to subscribers.
In this case, interception occurs for each subscriber receiving a message. For instance, if two subscribers `S1` and `S2`
receives the same message `M`, then interception logic will be triggered twice for the same message `M`.

```go
provider := interceptor.New(myProvider,
    interceptor.WithSubscribeInterceptor(mySubscribingInterceptor),
)
```

## Creating Interceptors

You can create your own publishing/subscribing interceptor by defining functions that follows the signatures:

```go
type PublishInterceptor func(ctx context.Context, next PublishHandler) PublishHandler
``` 

```go
type SubscribeInterceptor func(ctx context.Context, next SubscribeMessageHandler) SubscribeMessageHandler
```

See the `printer` interceptor for a more detailed example.

## Example

```go
package main

import (
  "context"
  "fmt"
  "time"

  "github.com/ChristopherCastro/go-pubsub"
  "github.com/ChristopherCastro/go-pubsub/provider/memory"
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
}
```

[broker-overview]: doc/broker.overview.png
