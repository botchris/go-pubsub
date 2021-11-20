package redis

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/botchris/go-pubsub"
)

func streamName(t pubsub.Topic) string {
	return fmt.Sprintf("stream-%s", t)
}

func incrementID(id string) string {
	// id is of form 12345-0
	parts := strings.Split(id, "-")
	if len(parts) != 2 {
		// not sure what to do with this
		return id
	}

	i, err := strconv.Atoi(parts[1])
	if err != nil {
		// not sure what to do with this
		return id
	}

	i++

	return fmt.Sprintf("%s-%d", parts[0], i)

}
