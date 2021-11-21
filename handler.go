package pubsub

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	"github.com/google/uuid"
)

// List of known errors for handler signature validation process
var (
	ErrNilHandler                   = errors.New("handler function can not be nil")
	ErrHandlerNotAFunction          = errors.New("provided handler is not a function")
	ErrHandlerInputLengthMissMatch  = errors.New("handler function must have exactly three input arguments")
	ErrHandlerInputNoContext        = errors.New("first argument of handler must be a context")
	ErrHandlerInputNoTopic          = errors.New("second argument of handler must be a topic")
	ErrHandlerOutputLengthMissMatch = errors.New("handler must have exactly one output argument")
	ErrHandlerOutputNoError         = errors.New("handler output must implements `error` interface")
)

// predefined values used internally when validation handler signatures
var (
	contextType = reflect.TypeOf((*context.Context)(nil)).Elem()
	errorType   = reflect.TypeOf((*error)(nil)).Elem()
	topicType   = reflect.TypeOf(Topic(""))
)

// Handler represents a handling function.
type Handler interface {
	// Deliver delivers the message to the handler. If handler does not
	// accept this kind of message, it should NOT return an error.
	Deliver(ctx context.Context, topic Topic, message interface{}) error

	// Reflect returns a description of the message type the handler is
	// interested in.
	Reflect() HandlerReflection

	// String returns a string representation of the handler.
	String() string
}

// HandlerReflection describes the message a handler is interested in.
type HandlerReflection struct {
	MessageType reflect.Type
	MessageKind reflect.Kind
}

// Subscriber represents a handling function capable of receiving messages
type handler struct {
	id          string
	handlerFunc reflect.Value
	messageType reflect.Type
	messageKind reflect.Kind
}

// NewHandler builds a new handler instance for the given function.
//
// This function WILL PANIC if the given function does not match the signature
// `func (ctx context.Context, t pubsub.Topic, m <Type>) error`, e.g.:
//
// - func (ctx context.Context, t pubsub.Topic, pointer *MyCustomStruct) error
// - func (ctx context.Context, t pubsub.Topic, customS MyCustomStruct) error
// - func (ctx context.Context, t pubsub.Topic, customI MyCustomInterface) error
//
// Handlers should return an error if they're unable to properly handle a
// given message. The same handler can be used on multiple subscriptions. In the
// other hand, in order to increase Broker's throughput, is highly recommended
// designing each Broker in such a way that handling of each message is
// asynchronously, in a separated goroutine.
func NewHandler(handlerFunc interface{}) Handler {
	if err := validateHandlerFn(handlerFunc); err != nil {
		panic(err)
	}

	fnType := reflect.TypeOf(handlerFunc)
	id := uuid.New().String()
	mType := fnType.In(2)

	return &handler{
		id:          id,
		handlerFunc: reflect.ValueOf(handlerFunc),
		messageType: mType,
		messageKind: mType.Kind(),
	}
}

// Deliver delivers the given message to this subscribers if acceptable
func (s *handler) Deliver(ctx context.Context, topic Topic, message interface{}) error {
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

func (s *handler) Reflect() HandlerReflection {
	return HandlerReflection{
		MessageType: s.messageType,
		MessageKind: s.messageKind,
	}
}

// String returns a string representation of this subscription
func (s *handler) String() string {
	in := s.messageType.String()

	return fmt.Sprintf("%s(%s)", s.id, in)
}

func (s *handler) accepts(in reflect.Type) bool {
	return in.AssignableTo(s.messageType)
}

// validateHandlerFn ensures that the given handling function has the form: `func (ctx context.Context, m <Type>) error`
func validateHandlerFn(fn interface{}) error {
	if fn == nil {
		return ErrNilHandler
	}

	fnType := reflect.TypeOf(fn)

	if fnType.Kind() != reflect.Func {
		return ErrHandlerNotAFunction
	}

	if fnType.NumIn() != 3 {
		return ErrHandlerInputLengthMissMatch
	}

	if !fnType.In(0).Implements(contextType) {
		return ErrHandlerInputNoContext
	}

	if !fnType.In(1).AssignableTo(topicType) {
		return ErrHandlerInputNoTopic
	}

	if fnType.NumOut() != 1 {
		return ErrHandlerOutputLengthMissMatch
	}

	if !fnType.Out(0).Implements(errorType) {
		return ErrHandlerOutputNoError
	}

	return nil
}
