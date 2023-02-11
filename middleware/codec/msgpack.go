package codec

import "github.com/vmihailenco/msgpack"

// Msgpack is a simple encoder and decoder that uses the msgpack package.
// See: https://github.com/vmihailenco/msgpack
var Msgpack Codec = msgpackCodec{}

type msgpackCodec struct{}

func (m msgpackCodec) Encode(i interface{}) ([]byte, error) {
	return msgpack.Marshal(i)
}

func (m msgpackCodec) Decode(bytes []byte, i interface{}) error {
	return msgpack.Unmarshal(bytes, &i)
}
