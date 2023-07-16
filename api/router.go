package api

//go:generate mockgen github.com/maxpoletaev/kivi/api Cluster -destination=mocks.go -package=api

import (
	"github.com/go-chi/chi/v5"

	"github.com/maxpoletaev/kivi/api/handler"
	"github.com/maxpoletaev/kivi/membership"
)

func CreateRouter(cluster membership.Cluster) *chi.Mux {
	r := chi.NewRouter()

	handler.NewKeyValueHandler(cluster).Register(r)
	handler.NewNodesHandler(cluster).Register(r)

	return r
}
