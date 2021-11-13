package sns

import (
	"context"
	"errors"
	"sync"

	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/botchris/go-pubsub"
)

// topicsCache is used to hold an in-memory map of code-level topics names and their corresponding AWS identity (ARN).
// Topics are expected to be tagged with the key "topic-name" when defined on AWS platform. This key is used to store
// the name of the topic as seen by broker implementation, that is, as reflected in the code by using type `pubsub.Type`
type topicsCache struct {
	client AWSSNSAPI
	cache  map[pubsub.Topic]string
	mu     sync.RWMutex
}

type topicsPage map[pubsub.Topic]string

func (p topicsPage) merge(p2 topicsPage) topicsPage {
	out := make(map[pubsub.Topic]string)

	for k, v := range p {
		out[k] = v
	}

	for k, v := range p2 {
		out[k] = v
	}

	return out
}

func newTopicsCache(client AWSSNSAPI) *topicsCache {
	return &topicsCache{
		client: client,
		cache:  make(map[pubsub.Topic]string),
	}
}

func (t *topicsCache) all() map[pubsub.Topic]string {
	t.mu.RLock()
	defer t.mu.RUnlock()

	out := make(map[pubsub.Topic]string)
	for k, v := range t.cache {
		out[k] = v
	}

	return out
}

// arnOf returns the ARN of the topic for the given name.
func (t *topicsCache) arnOf(topic pubsub.Topic) (string, error) {
	t.mu.RLock()
	found, hit := t.cache[topic]
	t.mu.RUnlock()

	if hit {
		return found, nil
	}

	return "", errors.New("topic not found")
}

// nameOf returns the name of the topic for the specified ARN, or empty if not found
func (t *topicsCache) nameOf(arn string) pubsub.Topic {
	t.mu.RLock()
	defer t.mu.RUnlock()

	for k, v := range t.cache {
		if v == arn {
			return k
		}
	}

	return ""
}

func (t *topicsCache) reloadCache(ctx context.Context) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	out := make(topicsPage)
	p, next, err := t.fetchNextPage(ctx, "")
	out = out.merge(p)

	if err != nil {
		return err
	}

	for next != "" {
		p, n, err := t.fetchNextPage(ctx, next)
		if err != nil {
			continue
		}

		next = n
		out = out.merge(p)
	}

	t.cache = out

	return nil
}

func (t *topicsCache) fetchNextPage(ctx context.Context, next string) (topicsPage, string, error) {
	out := make(map[pubsub.Topic]string)
	res, err := t.client.ListTopics(ctx, &sns.ListTopicsInput{})

	if err != nil {
		return out, next, err
	}

	if res.NextToken != nil {
		next = *res.NextToken
	}

	for i := range res.Topics {
		tRes, gErr := t.client.ListTagsForResource(ctx, &sns.ListTagsForResourceInput{
			ResourceArn: res.Topics[i].TopicArn,
		})

		if gErr != nil {
			continue
		}

		for _, tag := range tRes.Tags {
			if *tag.Key == "topic-name" {
				out[pubsub.Topic(*tag.Value)] = *res.Topics[i].TopicArn
			}
		}
	}

	return out, next, nil
}
