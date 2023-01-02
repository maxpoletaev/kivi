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

	clusterpkg "github.com/maxpoletaev/kv/cluster"
	"github.com/maxpoletaev/kv/cluster/nodeclient"
	"github.com/maxpoletaev/kv/faildetector"
	faildetectorpb "github.com/maxpoletaev/kv/faildetector/proto"
	faildetectorsvc "github.com/maxpoletaev/kv/faildetector/service"
	"github.com/maxpoletaev/kv/gossip"
	"github.com/maxpoletaev/kv/membership"
	"github.com/maxpoletaev/kv/membership/broadcast"
	membershippb "github.com/maxpoletaev/kv/membership/proto"
	membershipsvc "github.com/maxpoletaev/kv/membership/service"
	"github.com/maxpoletaev/kv/replication/consistency"
	replicationpb "github.com/maxpoletaev/kv/replication/proto"
	replicationsvc "github.com/maxpoletaev/kv/replication/service"
	"github.com/maxpoletaev/kv/storage/lsmtree"
	"github.com/maxpoletaev/kv/storage/lsmtree/engine"
	storagepb "github.com/maxpoletaev/kv/storage/proto"
	storagesvc "github.com/maxpoletaev/kv/storage/service"
)

type App struct {
	wg         sync.WaitGroup
	onShutdown []func()
}

func NewApp() *App {
	return &App{}
}

func (a *App) AddWorker(fn func()) {
	a.wg.Add(1)

	go func() {
		fn()
		a.wg.Done()
	}()
}

func (a *App) AddShutdownHook(fn func()) {
	a.onShutdown = append(a.onShutdown, fn)
}

func (a *App) Run() {
	a.wg.Wait()
}

func (a *App) Shutdown() {
	for _, fn := range a.onShutdown {
		fn()
	}

	a.wg.Wait()
}

func main() {
	appctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	logger := kitlog.NewLogfmtLogger(kitlog.NewSyncWriter(os.Stderr))
	args := parseCliArgs()

	defer cancel()

	if !args.verbose {
		logger = level.NewFilter(logger, level.AllowInfo())
	}

	gossipConf := gossip.DefaultConfig()
	gossipConf.PeerID = gossip.PeerID(args.nodeID)
	gossipConf.BindAddr = args.gossipBindAddr
	gossipConf.Logger = logger

	eventDelegate := broadcast.NewGossipEventDelegate()
	gossipConf.Delegate = eventDelegate

	// Start gossip publisher/listener process.
	gossiper, err := gossip.Start(gossipConf)
	if err != nil {
		logger.Log("msg", "failed to start gossip listener", "addr", args.gossipBindAddr, "err", err)
		os.Exit(1)
	}

	// Event publisher that uses gossip protocol to broadcast messages within the cluster.
	eventBroadcaster := broadcast.New(gossiper)

	localMember := membership.Member{
		ID:         membership.NodeID(args.nodeID),
		Name:       args.nodeName,
		GossipAddr: args.gossipPublicAddr,
		ServerAddr: args.grpcPublicAddr,
		Status:     membership.StatusHealthy,
		Version:    1,
	}

	memberlist := membership.New(localMember, logger, eventBroadcaster)

	dialer := nodeclient.NewDialer()
	connections := clusterpkg.NewConnRegistry(memberlist, dialer)
	cluster := clusterpkg.New(localMember.ID, memberlist, connections, dialer)

	lsmConfig := lsmtree.DefaultConfig()
	lsmConfig.MaxMemtableSize = args.memtableSize
	lsmConfig.DataRoot = args.dataDirectory
	lsmConfig.MmapDataFiles = true
	lsmConfig.Logger = logger

	lsmt, err := lsmtree.New(lsmConfig)
	if err != nil {
		logger.Log("msg", "failed to initialize LSM-Tree storage", "err", err)
		os.Exit(1)
	}

	storage := engine.New(lsmt)

	grpcServer := grpc.NewServer()
	storageService := storagesvc.New(storage, uint32(args.nodeID))
	storagepb.RegisterStorageServiceServer(grpcServer, storageService)
	membershipService := membershipsvc.NewMembershipService(memberlist)
	membershippb.RegisterMembershipServiceServer(grpcServer, membershipService)
	replicationService := replicationsvc.New(cluster, logger, consistency.Quorum, consistency.Quorum)
	replicationpb.RegisterCoordinatorServiceServer(grpcServer, replicationService)
	faildetectorService := faildetectorsvc.New(cluster)
	faildetectorpb.RegisterFailDetectorServiceServer(grpcServer, faildetectorService)

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, syscall.SIGINT, syscall.SIGTERM)

	detector := faildetector.New(memberlist, connections, logger, faildetector.WithIndirectPingNodes(1))

	wg := sync.WaitGroup{}

	wg.Add(1)
	go func() {
		defer wg.Done()
		detector.RunLoop(appctx)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()

		for event := range eventDelegate.Events() {
			memberlist.HandleEvent(event)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()

		for {
			select {
			case <-appctx.Done():
				return
			case <-time.After(5 * time.Second):
				connections.CollectGarbage()
			}
		}
	}()

	if len(args.joinAddr) != 0 {
		for {
			level.Info(logger).Log("msg", "attempting to join the cluster", "addr", args.joinAddr)
			ctx := context.Background()

			if err = cluster.JoinTo(ctx, []string{args.joinAddr}); err != nil {
				level.Error(logger).Log("msg", "failed to join cluster", "err", err)
				time.Sleep(3 * time.Second)
				continue
			}

			break
		}
	}

	wg.Add(1)
	go func() {
		<-interrupt
		defer wg.Done()

		if err := memberlist.Leave(); err != nil {
			logger.Log("msg", "failed to leave the cluster", "err", err)
		}

		gossiper.Shutdown()
		eventDelegate.Close()
		grpcServer.GracefulStop()
	}()

	// Create a TCP listener for the GRPC server.
	listener, err := net.Listen("tcp", args.grpcBindAddr)
	if err != nil {
		logger.Log("msg", "failed to listen tcp address", "addr", args.grpcBindAddr, "err", err)
		os.Exit(1)
	}

	// Start the GRPC server.
	if err := grpcServer.Serve(listener); err != nil {
		logger.Log("msg", "failed to start grpc server", "err", err)
		os.Exit(1)
	}

	wg.Wait()
}
