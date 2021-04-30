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
	ErrSubscriberNil                   = errors.New("encoder can not be nil")
	ErrSubscriberNotAFunction          = errors.New("provided subscriber is not a function")
	ErrSubscriberInputLengthMissMatch  = errors.New("subscriber must have exactly two input arguments")
	ErrSubscriberInputNoContext        = errors.New("first argument of subscriber must be a context")
	ErrSubscriberOutputLengthMissMatch = errors.New("subscriber must have exactly one output argument")
	ErrSubscriberOutputNoError         = errors.New("returned value must implements `error`")
)

// predefined values used internally when validation subscriber signatures
var (
	contextType = reflect.TypeOf((*context.Context)(nil)).Elem()
	errorType   = reflect.TypeOf((*error)(nil)).Elem()
)

// Subscriber represents a subscriber function capable of receiving messages
type Subscriber struct {
	id          string
	callable    reflect.Value
	messageType reflect.Type
	messageKind reflect.Kind
}

// NewSubscriber builds a new subscriber instance for the given function.
//
// This function WILL PANIC if the given handler does not match the signature
// `func (ctx context.Context, m <Type>) error`, e.g.:
//
// - func (ctx context.Context, pointer *MyCustomMessage) error
// - func (ctx context.Context, any MyCustomMessage) error
// - func (ctx context.Context, iface MyCustomInterface) error
//
// Subscribers should return an error if they're unable to properly handle a given message.
// In the other hand, is highly recommended to handle each message asynchronously in a separated goroutine in order
// to increase Broker's throughput.
func NewSubscriber(handlerFunc interface{}) *Subscriber {
	if err := validateSubscriberFn(handlerFunc); err != nil {
		panic(err)
	}

	t := time.Now()
	fnType := reflect.TypeOf(handlerFunc)
	entropy := rand.New(rand.NewSource(t.UnixNano()))
	id := ulid.MustNew(ulid.Timestamp(t), entropy).String()
	mType := fnType.In(1)

	return &Subscriber{
		id:          id,
		callable:    reflect.ValueOf(handlerFunc),
		messageType: mType,
		messageKind: mType.Kind(),
	}
}

// ID returns subscription identifier
func (s *Subscriber) ID() string {
	return s.id
}

// String returns a string representation of this subscription
func (s *Subscriber) String() string {
	in := s.messageType.String()

	return fmt.Sprintf("%s(%s)", s.id, in)
}

// Deliver delivers the given message to this subscribers if acceptable
func (s *Subscriber) Deliver(ctx context.Context, message interface{}) error {
	if messageType := reflect.TypeOf(message); !s.accepts(messageType) {
		return nil
	}

	args := []reflect.Value{
		reflect.ValueOf(ctx),
		reflect.ValueOf(message),
	}

	if out := s.callable.Call(args); out[0].Interface() != nil {
		return out[0].Interface().(error)
	}

	return nil
}

func (s *Subscriber) accepts(in reflect.Type) bool {
	return in.AssignableTo(s.messageType)
}

// validateSubscriberFn ensures that the given subscriber function has the form: `func (ctx context.Context, m <Type>) error`
func validateSubscriberFn(fn interface{}) error {
	if fn == nil {
		return ErrSubscriberNil
	}

	fnType := reflect.TypeOf(fn)

	if fnType.Kind() != reflect.Func {
		return ErrSubscriberNotAFunction
	}

	if fnType.NumIn() != 2 {
		return ErrSubscriberInputLengthMissMatch
	}

	if !fnType.In(0).Implements(contextType) {
		return ErrSubscriberInputNoContext
	}

	if fnType.NumOut() != 1 {
		return ErrSubscriberOutputLengthMissMatch
	}

	if !fnType.Out(0).Implements(errorType) {
		return ErrSubscriberOutputNoError
	}

	return nil
}
