package redis

import (
	"math/rand"
	"strings"
	"time"
)

const errMsgPoolTimeout = "redis: connection pool timeout"

// callWithRetry tries the call and reattempts uf we see a connection pool timeout error.
func callWithRetry(f func() error, retries int) error {
	var err error

	for i := 0; i < retries; i++ {
		err = f()

		if err == nil {
			return nil
		}

		if !isTimeoutError(err) {
			break
		}

		sleepWithJitter(2 * time.Second)
	}

	return err
}

func sleepWithJitter(max time.Duration) {
	// jitter the duration
	time.Sleep(max * time.Duration(rand.Int63n(200)) / 200)
}

func isTimeoutError(err error) bool {
	return err != nil && strings.Contains(err.Error(), errMsgPoolTimeout)
}
