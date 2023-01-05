package server

import (
	"context"
	"net/http"
)

type NamedMonitor interface {
	Name() string
	Monitor(ctx context.Context) error
}

func healthCheck(monitors ...NamedMonitor) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		components := make(map[string]monitoredComponents, 0)
		for _, monitor := range monitors {
			if err := monitor.Monitor(r.Context()); err == nil {
				components[monitor.Name()] = monitoredComponents{Status: UP}
			} else {
				components[monitor.Name()] = monitoredComponents{Status: DOWN}
			}
		}
		response := &healthResponse{
			Status:     UP,
			Components: components,
		}
		writeJson(w, http.StatusOK, response)
	}
}

type healthResponse struct {
	Status     health                         `json:"status"`
	Components map[string]monitoredComponents `json:"components"`
}

type health string

const (
	UP   health = "UP"
	DOWN        = "DOWN"
)

type monitoredComponents struct {
	Status health `json:"status"`
}
