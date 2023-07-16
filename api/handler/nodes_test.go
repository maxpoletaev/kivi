package handler

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/maxpoletaev/kivi/api/model"
	"github.com/maxpoletaev/kivi/membership"
	mockcluster "github.com/maxpoletaev/kivi/membership/mock"
)

func TestNodesAPI_getNodes(t *testing.T) {
	tests := map[string]struct {
		setupCluster func(c *mockcluster.MockCluster)
		wantBody     model.GetNodesResponse
	}{
		"Empty": {
			setupCluster: func(c *mockcluster.MockCluster) {
				c.EXPECT().Nodes().Return([]membership.Node{})
			},
			wantBody: model.GetNodesResponse{
				Nodes: []model.Node{},
			},
		},
		"SingleValue": {
			setupCluster: func(c *mockcluster.MockCluster) {
				c.EXPECT().Nodes().Return([]membership.Node{
					{ID: 1, Status: membership.StatusHealthy, Gen: 1},
				})
			},
			wantBody: model.GetNodesResponse{
				Nodes: []model.Node{
					{ID: 1, Status: "healthy"},
				},
			},
		},
		"MultipleValues": {
			setupCluster: func(c *mockcluster.MockCluster) {
				c.EXPECT().Nodes().Return([]membership.Node{
					{ID: 1, Status: membership.StatusHealthy, Gen: 1},
					{ID: 2, Status: membership.StatusUnhealthy, Gen: 1},
				})
			},
			wantBody: model.GetNodesResponse{
				Nodes: []model.Node{
					{ID: 1, Status: "healthy"},
					{ID: 2, Status: "unhealthy"},
				},
			},
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			var (
				mux     = chi.NewMux()
				ctrl    = gomock.NewController(t)
				cluster = mockcluster.NewMockCluster(ctrl)
			)

			tt.setupCluster(cluster)
			NewNodesHandler(cluster).Register(mux)

			req := httptest.NewRequest("GET", "/nodes", nil)
			recorder := httptest.NewRecorder()
			mux.ServeHTTP(recorder, req)

			require.Equal(t, http.StatusOK, recorder.Code)

			var resp model.GetNodesResponse
			err := json.NewDecoder(recorder.Body).Decode(&resp)
			require.NoError(t, err, "failed to unmarshal response: %v", err)

			require.Equal(t, tt.wantBody, resp)
		})
	}
}
