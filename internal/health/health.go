package health

import (
	"encoding/json"
	"net/http"
)

type Handler struct {
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(&healthResponse{Status: UP})
}

type healthResponse struct {
	Status health `json:"status"`
}

type health string

const (
	UP   health = "UP"
	DOWN        = "DOWN"
)
