package handler

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/render"

	"github.com/maxpoletaev/kivi/api/model"
	"github.com/maxpoletaev/kivi/membership"
)

type NodesHandler struct {
	cluster membership.Cluster
}

func NewNodesHandler(cluster membership.Cluster) *NodesHandler {
	return &NodesHandler{
		cluster: cluster,
	}
}

func (api *NodesHandler) Register(r chi.Router) {
	r.Get("/nodes", api.getNodes)
}

func (api *NodesHandler) getNodes(w http.ResponseWriter, r *http.Request) {
	nodes := api.cluster.Nodes()
	respNodes := make([]model.Node, len(nodes))

	for i, node := range nodes {
		respNodes[i] = model.Node{
			ID:     uint32(node.ID),
			Name:   node.Name,
			Addr:   node.PublicAddr,
			Status: node.Status.String(),
			Error:  node.Error,
		}
	}

	render.JSON(w, r, model.GetNodesResponse{
		Nodes: respNodes,
	})
}
