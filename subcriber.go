package pubsub

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"reflect"
	"time"

	"github.com/oklog/ulid/v2"
)

// List of known errors for subscriber signature validation process
var (
	ErrSubscriberNil                   = errors.New("handler function can not be nil")
	ErrSubscriberNotAFunction          = errors.New("provided handler is not a function")
	ErrSubscriberInputLengthMissMatch  = errors.New("handler must have exactly three input arguments")
	ErrSubscriberInputNoContext        = errors.New("first argument of handler must be a context")
	ErrSubscriberInputNoTopic          = errors.New("second argument of handler must be a topic")
	ErrSubscriberOutputLengthMissMatch = errors.New("handler must have exactly one output argument")
	ErrSubscriberOutputNoError         = errors.New("handler output must implements `error` interface")
)

// predefined values used internally when validation subscriber signatures
var (
	contextType = reflect.TypeOf((*context.Context)(nil)).Elem()
	errorType   = reflect.TypeOf((*error)(nil)).Elem()
	topicType   = reflect.TypeOf(Topic(""))
)

// Subscriber represents a handling function capable of receiving messages
type Subscriber struct {
	id          string
	handlerFunc reflect.Value
	messageType reflect.Type
	messageKind reflect.Kind
}

// NewSubscriber builds a new subscriber instance for the given function.
//
// This function WILL PANIC if the given handler does not match the signature
// `func (ctx context.Context, t pubsub.Topic, m <Type>) error`, e.g.:
//
// - func (ctx context.Context, t pubsub.Topic, pointer *MyCustomStruct) error
// - func (ctx context.Context, t pubsub.Topic, customS MyCustomStruct) error
// - func (ctx context.Context, t pubsub.Topic, customI MyCustomInterface) error
//
// Subscribers should return an error if they're unable to properly handle a
// given message. The same subscriber can be used on multiple topics. In the
// other hand, in order to increase Broker's throughput, is highly recommended
// designing each Broker in such a way that handling of each message is
// asynchronously, in a separated goroutine.
func NewSubscriber(handlerFunc interface{}) *Subscriber {
	if err := validateHandlerFn(handlerFunc); err != nil {
		panic(err)
	}

	t := time.Now()
	fnType := reflect.TypeOf(handlerFunc)
	entropy := rand.New(rand.NewSource(t.UnixNano()))
	id := ulid.MustNew(ulid.Timestamp(t), entropy).String()
	mType := fnType.In(2)

	return &Subscriber{
		id:          id,
		handlerFunc: reflect.ValueOf(handlerFunc),
		messageType: mType,
		messageKind: mType.Kind(),
	}
}

// ID returns subscription identifier
func (s *Subscriber) ID() string {
	return s.id
}

// Deliver delivers the given message to this subscribers if acceptable
func (s *Subscriber) Deliver(ctx context.Context, topic Topic, message interface{}) error {
	if messageType := reflect.TypeOf(message); !s.accepts(messageType) {
		return nil
	}

	args := []reflect.Value{
		reflect.ValueOf(ctx),
		reflect.ValueOf(topic),
		reflect.ValueOf(message),
	}

	if out := s.handlerFunc.Call(args); out[0].Interface() != nil {
		return out[0].Interface().(error)
	}

	return nil
}

// String returns a string representation of this subscription
func (s *Subscriber) String() string {
	in := s.messageType.String()

	return fmt.Sprintf("%s(%s)", s.id, in)
}

func (s *Subscriber) accepts(in reflect.Type) bool {
	return in.AssignableTo(s.messageType)
}

// validateHandlerFn ensures that the given handling function has the form: `func (ctx context.Context, m <Type>) error`
func validateHandlerFn(fn interface{}) error {
	if fn == nil {
		return ErrSubscriberNil
	}

	fnType := reflect.TypeOf(fn)

	if fnType.Kind() != reflect.Func {
		return ErrSubscriberNotAFunction
	}

	if fnType.NumIn() != 3 {
		return ErrSubscriberInputLengthMissMatch
	}

	if !fnType.In(0).Implements(contextType) {
		return ErrSubscriberInputNoContext
	}

	if !fnType.In(1).AssignableTo(topicType) {
		return ErrSubscriberInputNoTopic
	}

	if fnType.NumOut() != 1 {
		return ErrSubscriberOutputLengthMissMatch
	}

	if !fnType.Out(0).Implements(errorType) {
		return ErrSubscriberOutputNoError
	}

	return nil
}
