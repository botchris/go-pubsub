package sns

import "errors"

// Known errors.
var (
	ErrNoEncoder = errors.New("no encoder defined")
	ErrNoDecoder = errors.New("no decoder defined")
)
