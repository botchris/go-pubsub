package codec

import "encoding/json"

// JSON is a simple JSON encoder and decoder.
var JSON = jsonCodec{}

type jsonCodec struct{}

func (j jsonCodec) Encode(i interface{}) ([]byte, error) {
	return json.Marshal(i)
}

func (j jsonCodec) Decode(raw []byte, i interface{}) error {
	return json.Unmarshal(raw, i)
}
