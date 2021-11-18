package encoding

import (
	"errors"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// ProtoEncoder assumes that the given message is a proto message and converts it into a ProtoAny message which is
// later serialized into a byte slice.
var ProtoEncoder = func(msg interface{}) ([]byte, error) {
	p, ok := msg.(proto.Message)
	if !ok {
		return nil, errors.New("message is not a proto message")
	}

	any, err := anypb.New(p)
	if err != nil {
		return nil, err
	}

	return proto.Marshal(any)
}

// ProtoDecoder assumes that the incoming message is a ProtoAny message and converts it back to its underlying
// proto message.
var ProtoDecoder = func(msg []byte) (interface{}, error) {
	any := &anypb.Any{}

	if err := proto.Unmarshal(msg, any); err != nil {
		return nil, err
	}

	return any.UnmarshalNew()
}
