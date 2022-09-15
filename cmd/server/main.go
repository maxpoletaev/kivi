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
	"github.com/maxpoletaev/kv/gossip"
	"github.com/maxpoletaev/kv/network"
	"github.com/maxpoletaev/kv/network/proto"
	"github.com/maxpoletaev/kv/network/service"
	"github.com/maxpoletaev/kv/storage/inmemory"
	storagepb "github.com/maxpoletaev/kv/storage/proto"
	storagesrv "github.com/maxpoletaev/kv/storage/service"
	"google.golang.org/grpc"
)

func main() {
	var (
		nodeID     uint
		nodeName   string
		serverAddr string
		localAddr  string
		gossipAddr string
		joinAddr   string
	)

	flag.UintVar(&nodeID, "id", 0, "unique node id")
	flag.StringVar(&nodeName, "name", "", "node name")
	flag.StringVar(&serverAddr, "server-addr", "", "address of local grpc server")
	flag.StringVar(&localAddr, "local-addr", "", "addres of local grpc server")
	flag.StringVar(&gossipAddr, "gossip-addr", "", "address of local gossip listener")
	flag.StringVar(&joinAddr, "join", "", "any cluster node to join")
	flag.Parse()

	logger := kitlog.NewLogfmtLogger(kitlog.NewSyncWriter(os.Stderr))
	logger = kitlog.With(logger, "replica", nodeName)

	grpcServer := grpc.NewServer()

	storage := inmemory.New()
	storageService := storagesrv.New(storage, uint32(nodeID))
	storagepb.RegisterStorageServiceServer(grpcServer, storageService)

	gossipDelegate := network.NewGossipDelegate()

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

	localNode := network.Node{
		ID:              network.NodeID(nodeID),
		Name:            nodeName,
		GossipAddr:      gossipAddr,
		ServerAddr:      serverAddr,
		LocalServerAddr: localAddr,
	}

	eventPubSub := network.NewGossipEventPubSub(gossiper)
	cluster := network.NewCluster(localNode, logger, eventPubSub)

	if len(joinAddr) != 0 {
		level.Debug(logger).Log("msg", "attempting to join the cluster", "addr", joinAddr)

		if err = cluster.JoinTo(joinAddr); err != nil {
			level.Error(logger).Log("msg", "failed to join cluster", "err", err)
			os.Exit(1)
		}
	}

	clusterService := service.NewClusterService(cluster)
	proto.RegisterClusterSericeServer(grpcServer, clusterService)

	wg := sync.WaitGroup{}
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, syscall.SIGINT, syscall.SIGTERM)

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

		listener, err := net.Listen("tcp", serverAddr)
		if err != nil {
			logger.Log("msg", "failed to listen tcp address", "addr", serverAddr, "err", err)
			os.Exit(1)
		}

		connected, _, err := cluster.Connect()
		if err != nil {
			logger.Log("msg", "cluster connect failed", "connected", connected, "err", err)
		}

		if err := grpcServer.Serve(listener); err != nil {
			logger.Log("msg", "failed to start grpc server", "err", err)
			os.Exit(1)
		}
	}()

	wg.Add(1)
	go func() {
		<-interrupt

		defer wg.Done()

		if err := cluster.Leave(); err != nil {
			logger.Log("msg", "failed to leave the cluster", "err", err)
		}

		gossiper.Shutdown()

		gossipDelegate.Close()

		grpcServer.GracefulStop()
	}()

	wg.Wait()
}
