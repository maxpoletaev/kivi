package main

import "flag"

type cliArgs struct {
	nodeID           uint
	nodeName         string
	grpcLocalAddr    string
	grpcBindAddr     string
	grpcPublicAddr   string
	gossipBindAddr   string
	gossipPublicAddr string
	joinAddr         string
	dataDirectory    string
	inMemory         bool
	verbose          bool
	memtableSize     int
}

func parseCliArgs() cliArgs {
	args := cliArgs{}

	flag.UintVar(&args.nodeID, "node-id", 0, "unique node id")
	flag.StringVar(&args.nodeName, "node-name", "", "node name")

	flag.StringVar(&args.grpcBindAddr, "grpc-bind-addr", "", "address to bind grpc server")
	flag.StringVar(&args.grpcLocalAddr, "grpc-local-addr", "", "address to connect to local grpc server")
	flag.StringVar(&args.grpcPublicAddr, "grpc-public-addr", "", "address to advertise to other nodes")

	flag.StringVar(&args.gossipBindAddr, "gossip-bind-addr", "", "address to bind gossip listener")
	flag.StringVar(&args.gossipPublicAddr, "gossip-public-addr", "", "address to advertise to other nodes")

	flag.StringVar(&args.joinAddr, "join-addr", "", "address of a node to join the cluster")

	flag.BoolVar(&args.verbose, "verbose", false, "verbose mode")

	flag.BoolVar(&args.inMemory, "in-memory", false, "use in-memory storage")
	flag.IntVar(&args.memtableSize, "memtable-size", 1000, "max memtable size")
	flag.StringVar(&args.dataDirectory, "data-dir", "", "data directory")

	flag.Parse()

	return args
}
