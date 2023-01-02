package server

import (
	"encoding/json"
	"errors"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_writeJson(t *testing.T) {
	t.Run("should write a json response by encoding the given data", func(t *testing.T) {
		rec := httptest.NewRecorder()
		type response struct {
			Message string `json:"message"`
		}
		res := response{Message: "hello"}
		writeJson(rec, 200, res)
		require.Equal(t, 200, rec.Code)
		require.Equal(t, "application/json", rec.Header().Get("Content-Type"))
		gotBody := response{}
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&gotBody))
		require.Equal(t, res, gotBody)
	})
}

func Test_writeJsonError(t *testing.T) {
	t.Run("should write a json error response by encoding the given error", func(t *testing.T) {
		rec := httptest.NewRecorder()
		err := errors.New("generic error")
		writeJsonError(rec, 500, err)
		require.Equal(t, 500, rec.Code)
		require.Equal(t, "application/json", rec.Header().Get("Content-Type"))
		gotBody := errorResponse{}
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&gotBody))
		require.Equal(t, errorResponse{Error: errorDetails{Code: 500, Message: err.Error()}}, gotBody)
	})
}
