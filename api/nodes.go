package api

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/render"
)

type nodeInfo struct {
	ID         uint32
	Name       string
	Addr       string
	Status     string
	Generation int32
	Error      string `json:",omitempty"`
}

type nodesAPI struct {
	cluster Cluster
}

func newNodesAPI(cluster Cluster) *nodesAPI {
	return &nodesAPI{
		cluster: cluster,
	}
}

func (api *nodesAPI) Bind(r chi.Router) {
	r.Get("/cluster/nodes", api.handleGet)
}

func (api *nodesAPI) handleGet(w http.ResponseWriter, r *http.Request) {
	nodes := api.cluster.Nodes()
	resp := make([]nodeInfo, len(nodes))

	for i, node := range nodes {
		resp[i] = nodeInfo{
			ID:         uint32(node.ID),
			Name:       node.Name,
			Addr:       node.PublicAddr,
			Status:     node.Status.String(),
			Generation: node.Gen,
			Error:      node.Error,
		}
	}

	render.JSON(w, r, resp)
}
