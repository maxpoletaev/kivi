package api

import (
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/render"

	"github.com/maxpoletaev/kivi/nodeclient"
)

type kvAPI struct {
	cluster Cluster
}

func newKVAPI(cluster Cluster) *kvAPI {
	return &kvAPI{cluster: cluster}
}

func (api *kvAPI) Bind(r chi.Router) {
	r.Get("/kv/{key}", api.handleGet)
	r.Put("/kv/{key}", api.handlePut)
	r.Delete("/kv/{key}", api.handleDelete)
}

func (api *kvAPI) handleGet(w http.ResponseWriter, r *http.Request) {
	key := chi.URLParam(r, "key")
	conn := api.cluster.LocalConn()

	result, err := conn.RepGet(r.Context(), key)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	resp := struct {
		Values  []string `json:",omitempty"`
		Value   string   `json:",omitempty"`
		Version string
		Found   bool
	}{
		Version: result.Version,
	}

	if len(result.Values) == 0 {
		render.JSON(w, r, &resp)
		return
	} else if len(result.Values) > 1 {
		values := make([]string, len(result.Values))
		for i := range result.Values {
			values[i] = string(result.Values[i])
		}

		resp.Found = true
		resp.Values = values

		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(http.StatusMultipleChoices)
		render.JSON(w, r, &resp)

		return
	}

	resp.Value = string(result.Values[0])
	resp.Found = true

	render.JSON(w, r, &resp)
}

func (api *kvAPI) handlePut(w http.ResponseWriter, r *http.Request) {
	conn := api.cluster.LocalConn()
	key := chi.URLParam(r, "key")

	var req struct {
		Value   string `json:"value"`
		Version string `json:"version"`
	}

	if err := render.DecodeJSON(r.Body, &req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	version, err := conn.RepPut(r.Context(), key, []byte(req.Value), req.Version)
	if err != nil {
		status := http.StatusInternalServerError
		if errors.Is(err, nodeclient.ErrVersionConflict) {
			status = http.StatusConflict
		}

		http.Error(w, err.Error(), status)

		return
	}

	render.JSON(w, r, &struct {
		Version string
	}{Version: version})
}

func (api *kvAPI) handleDelete(w http.ResponseWriter, r *http.Request) {
	conn := api.cluster.LocalConn()
	key := chi.URLParam(r, "key")

	var req struct {
		Version string `json:"version"`
	}

	if err := render.DecodeJSON(r.Body, &req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	version, err := conn.RepDelete(r.Context(), key, req.Version)
	if err != nil {
		status := http.StatusInternalServerError
		if errors.Is(err, nodeclient.ErrVersionConflict) {
			status = http.StatusConflict
		}

		http.Error(w, err.Error(), status)

		return
	}

	render.JSON(w, r, &struct {
		Version string
	}{Version: version})
}
