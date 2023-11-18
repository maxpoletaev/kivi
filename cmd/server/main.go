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

func join(ctx context.Context, cluster *membership.SWIMCluster, logger kitlog.Logger, addr string) {
	var (
		timeout = 10 * time.Second
		backoff = 1 * time.Second
		maxWait = 30 * time.Second
	)

	for {
		level.Info(logger).Log("msg", "attempting to join cluster", "addr", addr)

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

		backoff = backoff * 2
		if backoff > maxWait {
			backoff = maxWait
		}

		level.Error(logger).Log(
			"msg", "failed to join cluster",
			"retry_in", backoff/time.Second,
			"addr", addr,
			"err", err,
		)

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

		os.Exit(1)
	}

	wg := sync.WaitGroup{}
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, syscall.SIGINT, syscall.SIGTERM)

	var shutdownQueue []shutdownFunc

	// Initialize all components.
	logger, closeLogger := setupLogger()
	shutdownQueue = append(shutdownQueue, closeLogger)

	engine, closeEngine := setupEngine(logger)
	shutdownQueue = append(shutdownQueue, closeEngine)

	cluster, closeCluster := setupCluster(logger)
	shutdownQueue = append(shutdownQueue, closeCluster)

	_, closeGRPCServer := setupGRPCServer(&wg, cluster, engine, logger)
	shutdownQueue = append(shutdownQueue, closeGRPCServer)

	// Join the cluster, in case we were given any addresses to join.
	joinCtx, cancelJoin := context.WithCancel(context.Background())
	for _, joinAddr := range parseAddrs(opts.Cluster.JoinAddrs) {
		wg.Add(1)

		go func(addr string) {
			defer wg.Done()
			join(joinCtx, cluster, logger, addr)
		}(joinAddr)
	}

	// Block until we receive a signal to shut down.
	<-interrupt
	cancelJoin()
	level.Info(logger).Log("msg", "received interrupt signal, shutting down")

	// Shutdown all components in reverse order.
	for i := len(shutdownQueue) - 1; i >= 0; i-- {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)

		if err := shutdownQueue[i](ctx); err != nil {
			level.Error(logger).Log("msg", "failed to shutdown component", "err", err)
		}

		cancel()
	}

	// Wait for all components to finish background tasks.
	wg.Wait()
}
