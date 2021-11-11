package sns

// Encoder receives an object and encodes it into a byte array.
type Encoder func(msg interface{}) ([]byte, error)

// Decoder receives a byte array and decodes it into an object.
type Decoder func(data []byte) (interface{}, error)
