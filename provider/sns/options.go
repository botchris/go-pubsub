package sns

import (
	"time"
)

// Option defines a function signature for configuration options.
type Option interface {
	apply(*options)
}

type options struct {
	snsClient            AWSSNSAPI
	sqsClient            AWSSQSAPI
	sqsQueueURL          string
	encoder              Encoder
	decoder              Decoder
	deliverTimeout       time.Duration
	topicsReloadInterval time.Duration
	maxMessages          int32
	visibilityTimeout    int32
	waitTimeSeconds      int32
}

type fnOption struct {
	f func(*options)
}

func (f fnOption) apply(o *options) {
	f.f(o)
}

// WithSNSClient sets the SNS client to be used by broker.
func WithSNSClient(snsClient AWSSNSAPI) Option {
	return fnOption{
		f: func(o *options) {
			o.snsClient = snsClient
		},
	}
}

// WithSQSClient sets the SQS client to be used by broker.
func WithSQSClient(sqsClient AWSSQSAPI) Option {
	return fnOption{
		f: func(o *options) {
			o.sqsClient = sqsClient
		},
	}
}

// WithSQSQueueURL sets the SQS queue URL to be used by broker.
func WithSQSQueueURL(sqsQueueURL string) Option {
	return fnOption{
		f: func(o *options) {
			o.sqsQueueURL = sqsQueueURL
		},
	}
}

// WithEncoder sets the encoder function to be used by broker.
// Use this to define how messages are encoded before sending to the SNS service.
func WithEncoder(e Encoder) Option {
	return fnOption{
		f: func(o *options) {
			o.encoder = e
		},
	}
}

// WithDecoder sets the decoder function to be used by broker.
// Use this to define how messages are decoded after receiving from the SQS service.
func WithDecoder(d Decoder) Option {
	return fnOption{
		f: func(o *options) {
			o.decoder = d
		},
	}
}

// WithDeliveryTimeout sets the max execution time a subscriber has to handle a message.
func WithDeliveryTimeout(t time.Duration) Option {
	return fnOption{
		f: func(o *options) {
			o.deliverTimeout = t
		},
	}
}

// WithTopicsReloadInterval determines how often in-memory topics cache should be reloaded by connecting to AWS.
// A lower value means that the broker will be less responsive as it will have to connect to AWS more often.
// Default: 60s.
func WithTopicsReloadInterval(t time.Duration) Option {
	return fnOption{
		f: func(o *options) {
			o.topicsReloadInterval = t
		},
	}
}

// WithMaxMessages sets the maximum number of messages to return. Amazon SQS never returns more messages than
// this value (however, fewer messages might be returned). Valid values: 1 to 10. Default: 5.
func WithMaxMessages(n int32) Option {
	return fnOption{
		f: func(o *options) {
			o.maxMessages = n
		},
	}
}

// WithVisibilityTimeout sets the duration (in seconds) that the received messages are hidden from subsequent retrieve
// requests after being retrieved by a ReceiveMessage request.
func WithVisibilityTimeout(t int32) Option {
	return fnOption{
		f: func(o *options) {
			o.visibilityTimeout = t
		},
	}
}

// WithWaitTimeSeconds sets the duration (in seconds) for which the call waits for a message to arrive in the queue
// before returning. If a message is available, the call returns sooner than WaitTimeSeconds. If no messages are
// available and the wait time expires, the call returns successfully with an empty list of messages.
func WithWaitTimeSeconds(t int32) Option {
	return fnOption{
		f: func(o *options) {
			o.waitTimeSeconds = t
		},
	}
}
