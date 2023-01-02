package test

import (
	"context"
	"testing"
	"time"
)

func Await(t *testing.T, time time.Duration, fn func() bool) {
	timeout, cancel := context.WithTimeout(context.Background(), time)
	defer cancel()
	for {
		select {
		case <-timeout.Done():
			t.Error("timed out")
			return
		default:
			if ok := fn(); ok {
				return
			}
		}
	}
}
