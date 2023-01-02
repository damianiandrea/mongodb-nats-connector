package test

import (
	"context"
	"errors"
	"time"
)

var ErrTimedOut = errors.New("timed out")

func Await(time time.Duration, fn func() bool) error {
	timeout, cancel := context.WithTimeout(context.Background(), time)
	defer cancel()
	for {
		select {
		case <-timeout.Done():
			return ErrTimedOut
		default:
			if ok := fn(); ok {
				return nil
			}
		}
	}
}
