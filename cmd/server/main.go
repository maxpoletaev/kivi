package main

import (
	"context"
	"math/rand"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	kitlog "github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"google.golang.org/grpc"

	"github.com/maxpoletaev/kiwi/faildetector"
	faildetectorpb "github.com/maxpoletaev/kiwi/faildetector/proto"
	faildetectorsvc "github.com/maxpoletaev/kiwi/faildetector/service"
	"github.com/maxpoletaev/kiwi/gossip"
	"github.com/maxpoletaev/kiwi/membership"
	"github.com/maxpoletaev/kiwi/membership/broadcast"
	membershippb "github.com/maxpoletaev/kiwi/membership/proto"
	membershipsvc "github.com/maxpoletaev/kiwi/membership/service"
	"github.com/maxpoletaev/kiwi/nodeclient"
	"github.com/maxpoletaev/kiwi/replication/consistency"
	replicationpb "github.com/maxpoletaev/kiwi/replication/proto"
	replicationsvc "github.com/maxpoletaev/kiwi/replication/service"
	"github.com/maxpoletaev/kiwi/storage/lsmtree"
	"github.com/maxpoletaev/kiwi/storage/lsmtree/engine"
	storagepb "github.com/maxpoletaev/kiwi/storage/proto"
	storagesvc "github.com/maxpoletaev/kiwi/storage/service"
)

func main() {
	appctx, cancel := signal.NotifyContext(
		context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	logger := kitlog.NewLogfmtLogger(kitlog.NewSyncWriter(os.Stderr))
	args := parseCliArgs()

	if !args.verbose {
		logger = level.NewFilter(logger, level.AllowInfo())
	}

	gossipConf := gossip.DefaultConfig()
	gossipConf.PeerID = gossip.PeerID(args.nodeID)
	gossipConf.BindAddr = args.gossipBindAddr
	gossipConf.Logger = logger

	eventReceiver := broadcast.NewReceiver()
	gossipConf.Delegate = eventReceiver

	// Start gossip publisher/listener process.
	gossiper, err := gossip.Start(gossipConf)
	if err != nil {
		logger.Log("msg", "failed to start gossip listener", "addr", args.gossipBindAddr, "err", err)
		os.Exit(1)
	}

	rnd := rand.New(rand.NewSource(int64(args.nodeID)))
	localMember := membership.Member{
		ID:         membership.NodeID(args.nodeID),
		RunID:      rnd.Uint32(),
		Name:       args.nodeName,
		GossipAddr: args.gossipPublicAddr,
		ServerAddr: args.grpcPublicAddr,
		Status:     membership.StatusHealthy,
		Version:    1,
	}

	dialer := nodeclient.NewGrpcDialer()
	eventSender := broadcast.NewSender(gossiper)
	memberlist := membership.New(localMember, logger, eventSender)
	connections := nodeclient.NewConnRegistry(memberlist, dialer)
	memberlist.ConsumeEvents(eventReceiver.Chan())

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

	membershipService := membershipsvc.NewMembershipService(memberlist)
	membershippb.RegisterMembershipServiceServer(grpcServer, membershipService)

	replicationService := replicationsvc.New(memberlist, logger, connections, consistency.Quorum, consistency.Quorum)
	replicationpb.RegisterCoordinatorServiceServer(grpcServer, replicationService)

	faildetectorService := faildetectorsvc.New(memberlist, connections)
	faildetectorpb.RegisterFailDetectorServiceServer(grpcServer, faildetectorService)

	wg := sync.WaitGroup{}
	interrupt := make(chan os.Signal, 1)
	detector := faildetector.New(memberlist, connections, logger, faildetector.WithIndirectPingNodes(1))

	signal.Notify(interrupt, syscall.SIGINT, syscall.SIGTERM)

	wg.Add(1)

	go func() {
		defer wg.Done()
		detector.RunLoop(appctx)
	}()

	wg.Add(1)

	go func() {
		defer wg.Done()

		for {
			select {
			case <-appctx.Done():
				return
			case <-time.After(30 * time.Second):
				connections.CollectGarbage()
			}
		}
	}()

	wg.Add(1)

	go func() {
		defer wg.Done()

		for len(args.joinAddr) > 0 {
			ctx, cancel := context.WithTimeout(appctx, 10*time.Second)

			level.Info(logger).Log("msg", "attempting to join the cluster", "addr", args.joinAddr)

			// At this point the local grpc server should be already listening.
			localConn, err := connections.Get(localMember.ID)
			if err != nil {
				level.Error(logger).Log("msg", "failed to connect to self", "err", err)
				time.Sleep(3 * time.Second)
				cancel()

				continue
			}

			// Connect to the remote node in order to get the list of members.
			remoteConn, err := dialer.DialContext(ctx, args.joinAddr)
			if err != nil {
				level.Error(logger).Log("msg", "failed to dial remote node", "err", err)
				time.Sleep(3 * time.Second)
				cancel()

				continue
			}

			if err := joinClusters(ctx, localConn, remoteConn); err != nil {
				level.Error(logger).Log("msg", "failed to join cluster", "err", err)
				time.Sleep(3 * time.Second)
				remoteConn.Close()
				cancel()

				continue
			}

			remoteConn.Close()
			cancel()

			break
		}
	}()

	wg.Add(1)

	go func() {
		<-interrupt

		defer wg.Done()

		level.Info(logger).Log("msg", "shutting down the server")

		// Leave the cluster before shutting down the server.
		if err := memberlist.Leave(); err != nil {
			logger.Log("msg", "failed to leave the cluster", "err", err)
		}

		grpcServer.GracefulStop()
		eventReceiver.Close()
		gossiper.Shutdown()
		lsmt.Close()
	}()

	// Create a TCP listener for the GRPC server.
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
