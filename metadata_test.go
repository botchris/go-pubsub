package pubsub_test

import (
	"testing"

	"github.com/botchris/go-pubsub"
	"github.com/stretchr/testify/require"
)

const (
	privateKey   = "private key"
	privateValue = "private value"
)

func TestMetadata(t *testing.T) {
	meta := pubsub.NewMetadata()
	meta.Set(privateKey, privateValue)

	actual := meta.Get(privateKey)

	require.NotNil(t, actual)
	require.EqualValues(t, privateValue, actual)
}
