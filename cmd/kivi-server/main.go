package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	kitlog "github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/jessevdk/go-flags"

	"github.com/maxpoletaev/kivi/membership"
)

func join(ctx context.Context, cluster *membership.Cluster, logger kitlog.Logger, addr string) {
	var (
		timeout = 10 * time.Second
		backoff = 1 * time.Second
		max     = 30 * time.Second
	)

	for {
		err := func() error {
			ctx, cancel := context.WithTimeout(ctx, timeout)
			defer cancel()

			if err := cluster.Join(ctx, addr); err != nil {
				return err
			}

			return nil
		}()

		if err == nil {
			level.Info(logger).Log("msg", "joined cluster", "addr", addr)
			return
		}

		level.Error(logger).Log(
			"msg", "failed to join cluster",
			"addr", addr,
			"err", err,
		)

		backoff = backoff * 2
		if backoff > max {
			backoff = max
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
			continue
		}
	}
}

func main() {
	p := flags.NewParser(&opts, flags.Default)

	if _, err := p.Parse(); err != nil {
		if err.(*flags.Error).Type != flags.ErrHelp {
			fmt.Println("cli error:", err)
		}

		os.Exit(2)
	}

	wg := sync.WaitGroup{}
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, syscall.SIGINT, syscall.SIGTERM)

	// Initialize all components.
	logger, closeLogger := setupLogger()
	cluster, closeCluster := setupCluster(logger)
	engine, closeEngine := setupEngine(logger)
	_, closeGrpcServer := setupGrpcServer(&wg, cluster, engine, logger)

	// Components must be shut down in a particular order.
	shutdownOrder := []shutdownFunc{
		closeCluster,
		closeGrpcServer,
		closeEngine,
		closeLogger,
	}

	if opts.RestAPI.Enabled {
		_, closeRestServer := setupRestServer(&wg, cluster)
		shutdownOrder = append([]shutdownFunc{closeRestServer}, shutdownOrder...)
	}

	// Join the cluster, in case we were given any addresses to join.
	joinCtx, cancelJoin := context.WithCancel(context.Background())
	for _, joinAddr := range parseAddrs(opts.Cluster.JoinAddrs) {
		go join(joinCtx, cluster, logger, joinAddr)
	}

	// Block until we receive a signal to shut down.
	<-interrupt
	cancelJoin()
	level.Info(logger).Log("msg", "received interrupt signal, shutting down")

	// Shutdown all components.
	for _, f := range shutdownOrder {
		if err := f(context.Background()); err != nil {
			level.Error(logger).Log("msg", "failed to shutdown component", "err", err)
		}
	}

	// Wait for all components to finish background tasks.
	wg.Wait()
}
