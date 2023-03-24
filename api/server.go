package api

import (
	"context"
	"fmt"
	"net/http"

	"github.com/go-chi/chi/v5"
	kitlog "github.com/go-kit/log"
	"github.com/go-kit/log/level"
)

func CreateRouter(cluster Cluster) *chi.Mux {
	r := chi.NewRouter()
	newKeyValueAPI(cluster).Bind(r)
	newNodesAPI(cluster).Bind(r)

	return r
}

func StartServer(ctx context.Context, cluster Cluster, logger kitlog.Logger, bindAddr string) error {
	r := chi.NewRouter()
	newNodesAPI(cluster).Bind(r)
	newKeyValueAPI(cluster).Bind(r)

	server := &http.Server{
		Addr:    bindAddr,
		Handler: r,
	}

	go func() {
		<-ctx.Done()

		if err := server.Shutdown(ctx); err != nil {
			level.Error(logger).Log("msg", "failed to shutdown server", "err", err)
		}
	}()

	if err := server.ListenAndServe(); err != nil {
		return fmt.Errorf("failed to start server: %w", err)
	}

	return nil
}
