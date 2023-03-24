package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/maxpoletaev/kivi/nodeclient"
	nodeclientmock "github.com/maxpoletaev/kivi/nodeclient/mock"
)

func TestKeyValueAPI_Get(t *testing.T) {
	type Response struct {
		Values  []string
		Value   string
		Version string
		Found   bool
	}

	tests := map[string]struct {
		setupConn    func(conn *nodeclientmock.MockConn)
		wantResponse Response
		wantStatus   int
	}{
		"NoValues": {
			setupConn: func(conn *nodeclientmock.MockConn) {
				result := &nodeclient.RepGetResponse{
					Values:  nil,
					Version: "",
				}

				conn.EXPECT().RepGet(gomock.Any(), "foo").Return(result, nil)
			},
			wantStatus: http.StatusOK,
			wantResponse: Response{
				Found: false,
			},
		},
		"SingleValue": {
			setupConn: func(conn *nodeclientmock.MockConn) {
				result := &nodeclient.RepGetResponse{
					Values:  [][]byte{[]byte("bar")},
					Version: "1",
				}

				conn.EXPECT().RepGet(gomock.Any(), "foo").Return(result, nil)
			},
			wantStatus: http.StatusOK,
			wantResponse: Response{
				Value:   "bar",
				Version: "1",
				Found:   true,
			},
		},
		"MultipleValues": {
			setupConn: func(conn *nodeclientmock.MockConn) {
				result := &nodeclient.RepGetResponse{
					Values:  [][]byte{[]byte("bar"), []byte("baz")},
					Version: "1",
				}

				conn.EXPECT().RepGet(gomock.Any(), "foo").Return(result, nil)
			},
			wantStatus: http.StatusMultipleChoices,
			wantResponse: Response{
				Values:  []string{"bar", "baz"},
				Version: "1",
				Found:   true,
			},
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			ctrl := gomock.NewController(t)

			cluster := NewMockCluster(ctrl)
			conn := nodeclientmock.NewMockConn(ctrl)
			cluster.EXPECT().LocalConn().Return(conn)
			tt.setupConn(conn)

			mux := CreateRouter(cluster)
			rr := httptest.NewRecorder()
			req := httptest.NewRequest("GET", "/kv/foo", nil)
			mux.ServeHTTP(rr, req)

			require.Equal(t, tt.wantStatus, rr.Code)

			var resp Response
			err := json.Unmarshal(rr.Body.Bytes(), &resp)
			require.NoError(t, err, "failed to unmarshal response: %v", err)

			require.Equal(t, tt.wantResponse, resp)
		})
	}
}
