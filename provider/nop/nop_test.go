package nop_test

import (
	"testing"

	"github.com/ChristopherCastro/go-pubsub/provider/nop"
	"github.com/stretchr/testify/require"
)

func TestNewBroker(t *testing.T) {
	require.NotNil(t, nop.NewBroker())
}
