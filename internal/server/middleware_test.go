package server

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_recoverer(t *testing.T) {
	type args struct {
		next http.Handler
	}
	tests := []struct {
		name            string
		args            args
		wantPanic       bool
		wantCode        int
		wantContentType string
		wantBody        errorResponse
	}{
		{
			name:            "should recover and write json error response if http handler panics",
			args:            args{next: &panickingHttpHandler{err: errors.New("panic")}},
			wantPanic:       false,
			wantCode:        500,
			wantContentType: "application/json",
			wantBody:        errorResponse{Error: errorDetails{Code: 500, Message: ErrInternal.Error()}},
		},
		{
			name:      "should still panic if http handler panics with abort handler error",
			args:      args{next: &panickingHttpHandler{err: http.ErrAbortHandler}},
			wantPanic: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := recoverer(tt.args.next)
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
			fn := func() {
				h.ServeHTTP(rec, req)
			}
			if tt.wantPanic {
				require.Panics(t, fn)
			} else {
				require.NotPanics(t, fn)
				require.Equal(t, tt.wantCode, rec.Code)
				require.Equal(t, tt.wantContentType, rec.Header().Get("Content-Type"))
				gotBody := errorResponse{}
				require.NoError(t, json.NewDecoder(rec.Body).Decode(&gotBody))
				require.Equal(t, tt.wantBody, gotBody)
			}
		})
	}
}

type panickingHttpHandler struct {
	err error
}

func (t *panickingHttpHandler) ServeHTTP(_ http.ResponseWriter, _ *http.Request) {
	panic(t.err)
}
