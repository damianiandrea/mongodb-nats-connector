package server

import (
	"errors"
	"net/http"
)

var ErrInternal = errors.New("internal server error")

func recoverer(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if err := recover(); err != nil {
				if err == http.ErrAbortHandler {
					panic(err)
				}
				writeJsonError(w, http.StatusInternalServerError, ErrInternal)
			}
		}()
		next.ServeHTTP(w, r)
	})
}
