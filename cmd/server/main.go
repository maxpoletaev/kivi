package main

import (
	"context"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	kitlog "github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"google.golang.org/grpc"

	"github.com/maxpoletaev/kiwi/membership"
	membershippb "github.com/maxpoletaev/kiwi/membership/proto"
	membershipsvc "github.com/maxpoletaev/kiwi/membership/service"
	nodegrpc "github.com/maxpoletaev/kiwi/nodeapi/grpc"
	replicationpb "github.com/maxpoletaev/kiwi/replication/proto"
	replicationsvc "github.com/maxpoletaev/kiwi/replication/service"
	"github.com/maxpoletaev/kiwi/storage/lsmtree"
	"github.com/maxpoletaev/kiwi/storage/lsmtree/engine"
	storagepb "github.com/maxpoletaev/kiwi/storage/proto"
	storagesvc "github.com/maxpoletaev/kiwi/storage/service"
)

func main() {
	appCtx, cancelApp := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancelApp()

	args := parseCliArgs()
	logger := kitlog.NewLogfmtLogger(kitlog.NewSyncWriter(os.Stderr))

	if !args.verbose {
		logger = level.NewFilter(logger, level.AllowInfo())
	}

	clusterConfig := membership.DefaultConfig()
	clusterConfig.NodeID = membership.NodeID(args.nodeID)
	clusterConfig.NodeName = args.nodeName
	clusterConfig.PublicAddr = args.grpcPublicAddr
	clusterConfig.LocalAddr = args.grpcLocalAddr
	clusterConfig.Dialer = nodegrpc.Dial
	clusterConfig.Logger = logger

	cluster := membership.NewCluster(clusterConfig)
	cluster.Start()

	lsmConfig := lsmtree.DefaultConfig()
	lsmConfig.MaxMemtableSize = args.memtableSize
	lsmConfig.DataRoot = args.dataDirectory
	lsmConfig.MmapDataFiles = true
	lsmConfig.Logger = logger

	lsmt, err := lsmtree.Create(lsmConfig)
	if err != nil {
		logger.Log("msg", "failed to initialize LSM-Tree storage", "err", err)
		os.Exit(1)
	}

	storage := engine.New(lsmt)
	grpcServer := grpc.NewServer()

	storageService := storagesvc.New(storage, uint32(args.nodeID))
	storagepb.RegisterStorageServiceServer(grpcServer, storageService)

	membershipService := membershipsvc.NewMembershipServer(cluster)
	membershippb.RegisterMembershipServer(grpcServer, membershipService)

	replicationService := replicationsvc.New(cluster, logger)
	replicationpb.RegisterReplicationServer(grpcServer, replicationService)

	wg := sync.WaitGroup{}
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, syscall.SIGINT, syscall.SIGTERM)

	wg.Add(1)

	go func() {
		defer wg.Done()

		for len(args.joinAddr) > 0 {
			ctx, cancel := context.WithTimeout(appCtx, 10*time.Second)

			if err := cluster.Join(ctx, args.joinAddr); err != nil {
				level.Error(logger).Log("msg", "failed to join the cluster", "addr", args.joinAddr, "err", err)
				cancel()

				continue
			}

			cancel()

			break
		}
	}()

	wg.Add(1)

	go func() {
		<-interrupt

		defer wg.Done()

		level.Info(logger).Log("msg", "shutting down the server")

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		if err := cluster.Leave(ctx); err != nil {
			logger.Log("msg", "failed to leave the cluster", "err", err)
		}

		grpcServer.GracefulStop()

		_ = lsmt.Close()
	}()

	// NewCluster a TCP listener for the GRPC server.
	listener, err := net.Listen("tcp", args.grpcBindAddr)
	if err != nil {
		logger.Log("msg", "failed to listen tcp address", "addr", args.grpcBindAddr, "err", err)
		os.Exit(1)
	}

	// Start the GRPC server, which will block until the server is stopped.
	if err := grpcServer.Serve(listener); err != nil {
		logger.Log("msg", "failed to start grpc server", "err", err)
		os.Exit(1)
	}

	wg.Wait()
}
