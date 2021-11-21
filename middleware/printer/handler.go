package printer

import (
	"context"
	"encoding/json"
	"fmt"
	"io"

	"github.com/botchris/go-pubsub"
)

type handler struct {
	pubsub.Handler
	writer io.Writer
}

func (s *handler) Deliver(ctx context.Context, topic pubsub.Topic, m interface{}) (err error) {
	j, err := json.Marshal(m)
	if err != nil {
		return err
	}

	log := fmt.Sprintf("[middleware] received @ %s : %s\n", topic, string(j))
	if _, err := s.writer.Write([]byte(log)); err != nil {
		return err
	}

	err = s.Handler.Deliver(ctx, topic, m)

	return
}
