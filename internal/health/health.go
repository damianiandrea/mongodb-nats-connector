package health

import (
	"encoding/json"
	"net/http"

	"github.com/damianiandrea/go-mongo-nats-connector/internal/mongo"
	"github.com/damianiandrea/go-mongo-nats-connector/internal/nats"
)

type Handler struct {
	mongoClient *mongo.Client
	natsClient  *nats.Client
}

func NewHandler(mongoClient *mongo.Client, natsClient *nats.Client) *Handler {
	return &Handler{
		mongoClient: mongoClient,
		natsClient:  natsClient,
	}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	response := &healthResponse{Status: UP}
	ctx := r.Context()
	if err := h.mongoClient.Ping(ctx); err != nil {
		response.Components.Mongo.Status = DOWN
	} else {
		response.Components.Mongo.Status = UP
	}
	if err := h.natsClient.Ping(ctx); err != nil {
		response.Components.Nats.Status = DOWN
	} else {
		response.Components.Nats.Status = UP
	}
	_ = json.NewEncoder(w).Encode(response)
}

type healthResponse struct {
	Status     health     `json:"status"`
	Components components `json:"components"`
}

type health string

const (
	UP   health = "UP"
	DOWN        = "DOWN"
)

type components struct {
	Mongo mongoComponent `json:"mongo"`
	Nats  natsComponent  `json:"nats"`
}

type mongoComponent struct {
	Status health `json:"status"`
}

type natsComponent struct {
	Status health `json:"status"`
}
