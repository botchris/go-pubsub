package main

import (
	"context"
	"fmt"
	"time"

	"github.com/ChristopherCastro/go-pubsub"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	broker := pubsub.NewBroker()
	broker.Subscribe(ctx, receiver, "topic-1")

	go func() {
		i := 1
		for {
			broker.Publish(ctx, fmt.Sprintf("m%d", i), "topic-1")
			time.Sleep(time.Second)
			i++
		}
	}()

	<-ctx.Done()
}

func receiver(ctx context.Context, m interface{}) {
	fmt.Println(fmt.Sprintf("message received: %s", m))
}