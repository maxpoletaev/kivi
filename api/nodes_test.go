package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/maxpoletaev/kivi/membership"
)

func TestNodesAPI_handleGet(t *testing.T) {
	tests := map[string]struct {
		setupCluster func(c *MockCluster)
		wantBody     []nodeInfo
	}{
		"Empty": {
			setupCluster: func(c *MockCluster) {
				c.EXPECT().Nodes().Return([]membership.Node{})
			},
			wantBody: []nodeInfo{},
		},
		"NotEmpty": {
			setupCluster: func(c *MockCluster) {
				c.EXPECT().Nodes().Return([]membership.Node{
					{ID: 1, Status: membership.StatusHealthy, Gen: 1},
					{ID: 2, Status: membership.StatusUnhealthy, Gen: 1},
				})
			},
			wantBody: []nodeInfo{
				{
					ID:     1,
					Status: "healthy",
				},
				{
					ID:     2,
					Status: "unhealthy",
				},
			},
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			ctrl := gomock.NewController(t)

			c := NewMockCluster(ctrl)
			tt.setupCluster(c)

			rr := httptest.NewRecorder()
			req := httptest.NewRequest("GET", "/cluster/nodes", nil)

			CreateRouter(c).ServeHTTP(rr, req)

			require.Equal(t, http.StatusOK, rr.Code)

			var resp []nodeInfo
			err := json.NewDecoder(rr.Body).Decode(&resp)
			require.NoError(t, err, "failed to unmarshal response: %v", err)

			require.Equal(t, tt.wantBody, resp)
		})
	}
}
