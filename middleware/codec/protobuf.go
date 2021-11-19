package codec

import (
	"errors"

	"google.golang.org/protobuf/proto"
)

// ProtoEncoder assumes that the given message is a proto message and converts it into a ProtoAny message which is
// later serialized into a byte slice.
var ProtoEncoder = func(msg interface{}) ([]byte, error) {
	p, ok := msg.(proto.Message)
	if !ok {
		return nil, errors.New("message is not a proto message")
	}

	return proto.Marshal(p)
}

// ProtoDecoder assumes that the incoming message is a ProtoAny message and converts it back to its underlying
// proto message.
var ProtoDecoder = func(raw []byte, target interface{}) error {
	t, ok := target.(proto.Message)
	if !ok {
		return nil
	}

	if err := proto.Unmarshal(raw, t); err != nil {
		return nil
	}

	return nil
}
