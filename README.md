[![Build Status](https://travis-ci.com/ChristopherCastro/go-pubsub.svg?branch=master)](https://travis-ci.com/ChristopherCastro/go-pubsub)


## Example


```go
func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	broker := pubsub.NewBroker()
	broker.Subscribe(ctx, receiverFunc, "topic-1")

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

func receiverFunc(ctx context.Context, m interface{}) {
	fmt.Println(fmt.Sprintf("message received: %s", m))
}
```