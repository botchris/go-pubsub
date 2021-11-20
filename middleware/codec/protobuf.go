package codec

import (
	"errors"

	"google.golang.org/protobuf/proto"
)

// Protobuf encodes and decodes messages using protocol buffers.
var Protobuf = protobuf{}

type protobuf struct{}

// Encode assumes that the given message is a proto message and encodes it using
// `proto.Marshal`
func (p protobuf) Encode(msg interface{}) ([]byte, error) {
	pb, ok := msg.(proto.Message)
	if !ok {
		return nil, errors.New("message is not a proto message")
	}

	return proto.Marshal(pb)
}

// Decode assumes that the given target is a pointer to a `proto.Message`
// implementation and decodes it using `proto.Unmarshal`
func (p protobuf) Decode(raw []byte, target interface{}) error {
	t, ok := target.(proto.Message)
	if !ok {
		return nil
	}

	if err := proto.Unmarshal(raw, t); err != nil {
		return nil
	}

	return nil
}
