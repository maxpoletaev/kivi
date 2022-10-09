package main

import (
	"flag"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"

	kitlog "github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"google.golang.org/grpc"

	"github.com/maxpoletaev/kv/clustering"
	"github.com/maxpoletaev/kv/clustering/faildetect"
	clusterpb "github.com/maxpoletaev/kv/clustering/proto"
	clustersvc "github.com/maxpoletaev/kv/clustering/service"
	"github.com/maxpoletaev/kv/gossip"
	replicationpb "github.com/maxpoletaev/kv/replication/proto"
	replicationsvc "github.com/maxpoletaev/kv/replication/service"
	"github.com/maxpoletaev/kv/storage/inmemory"
	storagepb "github.com/maxpoletaev/kv/storage/proto"
	storagesvc "github.com/maxpoletaev/kv/storage/service"
)

func main() {
	var (
		nodeID        uint
		nodeName      string
		localAddr     string
		bindAddr      string
		advertiseAddr string
		gossipAddr    string
		joinAddr      string
	)

	flag.UintVar(&nodeID, "id", 0, "unique node id")
	flag.StringVar(&nodeName, "name", "", "node name")
	flag.StringVar(&localAddr, "local-addr", "", "addres of local grpc server")
	flag.StringVar(&bindAddr, "bind-addr", "", "address of local grpc server")
	flag.StringVar(&advertiseAddr, "advertise-addr", "", "address to advertise to other nodes")
	flag.StringVar(&gossipAddr, "gossip-addr", "", "address of local gossip listener")
	flag.StringVar(&joinAddr, "join", "", "any cluster node to join")
	flag.Parse()

	logger := kitlog.NewLogfmtLogger(kitlog.NewSyncWriter(os.Stderr))
	logger = kitlog.With(logger, "replica", nodeName)

	grpcServer := grpc.NewServer()

	storage := inmemory.New()
	storageService := storagesvc.New(storage, uint32(nodeID))
	storagepb.RegisterStorageServiceServer(grpcServer, storageService)

	gossipDelegate := clustering.NewGossipDelegate()

	gossipConf := gossip.DefaultConfig()
	gossipConf.PeerID = gossip.PeerID(nodeID)
	gossipConf.Delegate = gossipDelegate
	gossipConf.BindAddr = gossipAddr
	gossipConf.Logger = logger

	gossiper, err := gossip.Start(gossipConf)
	if err != nil {
		logger.Log("msg", "failed to start gossip listener", "addr", gossipAddr, "err", err)
		os.Exit(1)
	}

	if advertiseAddr == "" {
		advertiseAddr = bindAddr
	}

	localNode := clustering.Member{
		ID:              clustering.NodeID(nodeID),
		Status:          clustering.StatusAlive,
		Name:            nodeName,
		GossipAddr:      gossipAddr,
		ServerAddr:      advertiseAddr,
		LocalServerAddr: localAddr,
	}

	eventPubSub := clustering.NewGossipEventPublisher(gossiper)
	cluster := clustering.NewCluster(localNode, logger, eventPubSub)
	failDetector := faildetect.New(cluster, logger)

	if len(joinAddr) != 0 {
		level.Debug(logger).Log("msg", "attempting to join the cluster", "addr", joinAddr)

		if err = cluster.JoinTo(joinAddr); err != nil {
			level.Error(logger).Log("msg", "failed to join cluster", "err", err)
			os.Exit(1)
		}
	}

	clusterService := clustersvc.NewClusteringService(cluster)
	clusterpb.RegisterClusteringSericeServer(grpcServer, clusterService)

	replicationService := replicationsvc.New(cluster, logger)
	replicationpb.RegisterCoordinatorServiceServer(grpcServer, replicationService)

	wg := sync.WaitGroup{}
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, syscall.SIGINT, syscall.SIGTERM)

	serverReady := make(chan struct{})

	wg.Add(1)
	go func() {
		defer wg.Done()

		for event := range gossipDelegate.Events() {
			cluster.DispatchEvent(event)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()

		listener, err := net.Listen("tcp", bindAddr)
		if err != nil {
			logger.Log("msg", "failed to listen tcp address", "addr", bindAddr, "err", err)
			os.Exit(1)
		}

		err = cluster.ConnectLocal()
		if err != nil {
			logger.Log("msg", "local connection failed", "err", err)
			os.Exit(1)
		}

		close(serverReady)

		if err := grpcServer.Serve(listener); err != nil {
			logger.Log("msg", "failed to start grpc server", "err", err)
			os.Exit(1)
		}
	}()

	wg.Add(1)
	go func() {
		<-serverReady

		defer wg.Done()

		failDetector.RunLoop()
	}()

	wg.Add(1)
	go func() {
		<-interrupt

		defer wg.Done()

		if err := cluster.Leave(); err != nil {
			logger.Log("msg", "failed to leave the cluster", "err", err)
		}

		failDetector.Stop()
		gossiper.Shutdown()
		gossipDelegate.Close()
		grpcServer.GracefulStop()
	}()

	wg.Wait()
}
