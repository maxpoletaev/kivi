package handler

import (
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/render"

	"github.com/maxpoletaev/kivi/api/model"
	"github.com/maxpoletaev/kivi/membership"
	"github.com/maxpoletaev/kivi/nodeapi"
)

type KeyValueHandler struct {
	cluster membership.Cluster
}

func NewKeyValueHandler(cluster membership.Cluster) *KeyValueHandler {
	return &KeyValueHandler{cluster: cluster}
}

func (api *KeyValueHandler) Register(r chi.Router) {
	r.Get("/kv/{key}", api.getKey)
	r.Put("/kv/{key}", api.putKey)
	r.Delete("/kv/{key}", api.deleteKey)
}

func (api *KeyValueHandler) getKey(w http.ResponseWriter, r *http.Request) {
	key := chi.URLParam(r, "key")
	conn := api.cluster.LocalConn()

	result, err := conn.GetKey(r.Context(), key)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if len(result.Values) > 1 {
		values := make([]string, len(result.Values))
		for i := range result.Values {
			values[i] = string(result.Values[i])
		}

		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(http.StatusMultipleChoices)

		render.JSON(w, r, model.GetKeyResponse{
			Exists:  true,
			Values:  values,
			Version: result.Version,
		})
	} else if len(result.Values) == 1 {
		render.JSON(w, r, model.GetKeyResponse{
			Exists:  true,
			Version: result.Version,
			Value:   string(result.Values[0]),
		})
	} else {
		render.JSON(w, r, model.GetKeyResponse{
			Version: result.Version,
		})
	}
}

func (api *KeyValueHandler) putKey(w http.ResponseWriter, r *http.Request) {
	conn := api.cluster.LocalConn()
	key := chi.URLParam(r, "key")

	var params model.PutKeyParams
	if err := render.DecodeJSON(r.Body, &params); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	putResult, err := conn.PutKey(r.Context(), key, []byte(params.Value), params.Version)
	if err != nil {
		status := http.StatusInternalServerError
		if errors.Is(err, nodeapi.ErrVersionConflict) {
			status = http.StatusConflict
		}

		http.Error(w, err.Error(), status)

		return
	}

	render.JSON(w, r, &model.PutKeyResponse{
		Acknowledged: putResult.Acknowledged,
		Version:      putResult.Version,
	})
}

func (api *KeyValueHandler) deleteKey(w http.ResponseWriter, r *http.Request) {
	conn := api.cluster.LocalConn()
	key := chi.URLParam(r, "key")

	var params model.DeleteKeyParams
	if err := render.DecodeJSON(r.Body, &params); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	result, err := conn.DeleteKey(r.Context(), key, params.Version)
	if err != nil {
		status := http.StatusInternalServerError
		if errors.Is(err, nodeapi.ErrVersionConflict) {
			status = http.StatusConflict
		}

		http.Error(w, err.Error(), status)

		return
	}

	render.JSON(w, r, model.DeleteKeyResponse{
		Acknowledged: result.Acknowledged,
		Version:      result.Version,
	})
}
