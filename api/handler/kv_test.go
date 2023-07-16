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
	mockcluster "github.com/maxpoletaev/kivi/membership/mock"
	"github.com/maxpoletaev/kivi/nodeapi"
	mocknodeapi "github.com/maxpoletaev/kivi/nodeapi/mock"
)

func TestKeyValueAPI_Get(t *testing.T) {
	tests := map[string]struct {
		setupConn    func(conn *mocknodeapi.MockConn)
		wantResponse model.GetKeyResponse
		wantStatus   int
	}{
		"NotFound": {
			setupConn: func(conn *mocknodeapi.MockConn) {
				result := &nodeapi.GetKeyResult{
					Values:  nil,
					Version: "",
				}

				conn.EXPECT().GetKey(gomock.Any(), "foo").Return(result, nil)
			},
			wantStatus: http.StatusOK,
			wantResponse: model.GetKeyResponse{
				Exists: false,
			},
		},
		"SingleValue": {
			setupConn: func(conn *mocknodeapi.MockConn) {
				result := &nodeapi.GetKeyResult{
					Values:  [][]byte{[]byte("bar")},
					Version: "1",
				}

				conn.EXPECT().GetKey(gomock.Any(), "foo").Return(result, nil)
			},
			wantStatus: http.StatusOK,
			wantResponse: model.GetKeyResponse{
				Exists:  true,
				Value:   "bar",
				Version: "1",
			},
		},
		"MultipleValues": {
			setupConn: func(conn *mocknodeapi.MockConn) {
				result := &nodeapi.GetKeyResult{
					Values:  [][]byte{[]byte("bar"), []byte("baz")},
					Version: "1",
				}

				conn.EXPECT().GetKey(gomock.Any(), "foo").Return(result, nil)
			},
			wantStatus: http.StatusMultipleChoices,
			wantResponse: model.GetKeyResponse{
				Exists:  true,
				Values:  []string{"bar", "baz"},
				Version: "1",
			},
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			ctrl := gomock.NewController(t)

			conn := mocknodeapi.NewMockConn(ctrl)
			cluster := mockcluster.NewMockCluster(ctrl)
			cluster.EXPECT().LocalConn().Return(conn)
			tt.setupConn(conn)

			mux := chi.NewMux()
			NewKeyValueHandler(cluster).Register(mux)

			rr := httptest.NewRecorder()
			req := httptest.NewRequest("GET", "/kv/foo", nil)
			mux.ServeHTTP(rr, req)

			require.Equal(t, tt.wantStatus, rr.Code)

			var resp model.GetKeyResponse
			err := json.Unmarshal(rr.Body.Bytes(), &resp)
			require.NoError(t, err, "failed to unmarshal response: %v", err)

			require.Equal(t, tt.wantResponse, resp)
		})
	}
}
