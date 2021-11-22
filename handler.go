package pubsub

import (
	"context"
	"errors"
	"reflect"
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

// Handler represents a function capable of handling a message arriving at a
// topic.
type Handler interface {
	// Deliver delivers the message to the handler. If handler does not
	// accept this kind of message, it should NOT return an error.
	Deliver(ctx context.Context, topic Topic, message interface{}) error

	// Reflect returns a description of the message type the handler is
	// interested in.
	Reflect() MessageReflection
}

// MessageReflection describes the message a handler is interested in.
type MessageReflection struct {
	MessageType reflect.Type
	MessageKind reflect.Kind
	Handler     reflect.Value
}

// Accepts whether the handler accepts the provided message.
func (r MessageReflection) Accepts(m interface{}) bool {
	in := reflect.TypeOf(m)

	return in.AssignableTo(r.MessageType)
}

// handler represents a handling function capable of receiving messages
type handler struct {
	reflection  MessageReflection
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
	mType := fnType.In(2)

	return &handler{
		reflection: MessageReflection{
			MessageType: mType,
			MessageKind: mType.Kind(),
			Handler:     reflect.ValueOf(handlerFunc),
		},
		messageKind: mType.Kind(),
	}
}

func (h *handler) Deliver(ctx context.Context, topic Topic, message interface{}) error {
	if !h.reflection.Accepts(message) {
		return nil
	}

	args := []reflect.Value{
		reflect.ValueOf(ctx),
		reflect.ValueOf(topic),
		reflect.ValueOf(message),
	}

	if out := h.reflection.Handler.Call(args); out[0].Interface() != nil {
		return out[0].Interface().(error)
	}

	return nil
}

func (h *handler) Reflect() MessageReflection {
	return h.reflection
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
