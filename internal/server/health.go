package server

import (
	"context"
	"net/http"
)

type MonitoredComponent interface {
	Name() string
	Ping(ctx context.Context) error
}

type HealthHandler struct {
	components []MonitoredComponent
}

func NewHealthHandler(components ...MonitoredComponent) *HealthHandler {
	return &HealthHandler{
		components: components,
	}
}

func (h *HealthHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	components := make(map[string]monitoredComponents, 0)
	for _, component := range h.components {
		if err := component.Ping(r.Context()); err == nil {
			components[component.Name()] = monitoredComponents{Status: UP}
		} else {
			components[component.Name()] = monitoredComponents{Status: DOWN}
		}
	}
	response := &healthResponse{
		Status:     UP,
		Components: components,
	}
	writeJson(w, http.StatusOK, response)
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
