package codec

import (
	"bytes"
	"encoding/gob"
)

// Gob is a simple encoder and decoder that uses the gob package.
var Gob = gobCodec{}

type gobCodec struct{}

func (g gobCodec) Encode(i interface{}) ([]byte, error) {
	var buf bytes.Buffer
	var enc = gob.NewEncoder(&buf)

	if err := enc.Encode(i); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func (g gobCodec) Decode(raw []byte, i interface{}) error {
	enc := gob.NewDecoder(bytes.NewBuffer(raw))

	return enc.Decode(i)
}
