package server

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_healthCheck(t *testing.T) {
	type fields struct {
		monitors []NamedMonitor
	}
	type args struct {
		w http.ResponseWriter
		r *http.Request
	}
	tests := []struct {
		name            string
		fields          fields
		args            args
		wantCode        int
		wantContentType string
		wantBody        healthResponse
	}{
		{
			name:   "should write a json response with component status up, if it was reachable",
			fields: fields{monitors: []NamedMonitor{&testComponent{name: "test", err: nil}}},
			args: args{
				w: httptest.NewRecorder(),
				r: httptest.NewRequest(http.MethodGet, "/healthz", nil),
			},
			wantCode:        200,
			wantContentType: "application/json",
			wantBody: healthResponse{
				Status: UP,
				Components: map[string]monitoredComponents{
					"test": {Status: UP},
				},
			},
		},
		{
			name: "should write a json response with component status down, if it was not reachable",
			fields: fields{monitors: []NamedMonitor{&testComponent{name: "test",
				err: errors.New("not reachable")}}},
			args: args{
				w: httptest.NewRecorder(),
				r: httptest.NewRequest(http.MethodGet, "/healthz", nil),
			},
			wantCode:        200,
			wantContentType: "application/json",
			wantBody: healthResponse{
				Status: UP,
				Components: map[string]monitoredComponents{
					"test": {Status: DOWN},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			healthCheck := healthCheck(tt.fields.monitors...)
			healthCheck(tt.args.w, tt.args.r)
			rec := tt.args.w.(*httptest.ResponseRecorder)
			require.Equal(t, tt.wantCode, rec.Code)
			require.Equal(t, tt.wantContentType, rec.Header().Get("Content-Type"))
			gotBody := healthResponse{}
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&gotBody))
			require.Equal(t, tt.wantBody, gotBody)
		})
	}
}

type testComponent struct {
	name string
	err  error
}

func (t *testComponent) Name() string {
	return t.name
}

func (t *testComponent) Monitor(_ context.Context) error {
	return t.err
}
