# 🦄 My Little KV

**This is still WIP and is in the early stage of development.**

The goal of the project is to build a distributed key value store from scratch
using as less third party libraries as possible (I couldn’t resist using GRPC
and Protobuf for networking and data serialization, though).

It falls into a category of so called dynamo-inspired databases (like Cassandra
and Riak), the key properties of which are:

 - No leader/follower separation. Any node can accept reads and writes.
 - Eventually consistent or strongly eventually consistent. Techniques like read
   repair and hinted handoff are used to get replicas to the consistent state
   after temporary failures.
 - Conflicting writes are unavoidable when the same key is updated concurrently
   on different replicas. Once a conflict is detected, a client must perform
   conflict resolution before the next write or to apply automatic conflict
   resolution strategies such as last-write-wins or utilize special
   conflict-free data structures (CRDT).

## Components

 - **storage** – implements a persistent key value storage on a single node.
   This is basically LSM-Trees with an RPC interface. However, the engine can be
   easily replaced with anything that can do gets and puts. LevelDB and BoltDB
   are good candidates.
 - **gossip** – implements a generic gossip-based broadcast protocol to exchange
   asynchronous messages between cluster nodes. This is the main building block
   used in the following layer to exchange the information about cluster members
   and their status.
 - **clustering** – contains the code to group several storage nodes into a cluster
  abstraction and also implements a simple membership protocol for decentralized
  nodes discovery and failure detection.
 - **replication** – does all the coordination heavy-lifting and ensures the data
   is replicated across the nodes with desired consistency guarantees. The
   consistency/availability tradeoff is configurable, so the database can be
   tuned into a highly consistent storage with poor tolerance to failures and
   network partitions, or into a highly-available storage but with more
   inconsistency anomalies to expect.

Each layer is implemented as an individual GRPC service so that each node can
talk to any layer of any other node within the cluster.

Although in this example they are running within a single process, it is
possible that storage and replication layers may run independently on different
machines.

## How to try it out

The `docker-compose.yaml` contains a minimal configuration of a cluster of
three replicas. To run it, use:

 1. `make image`
 2. `docker compose up`

At the moment, GRPC is the only way to interact with the database. The replicas
are available at localhost:3001-3003 and can be accessed via a GRPC client such
as BloomRPC. Any replica can be used for reads and writes.

With the default consistency level, you need the majority of nodes (2 out of 3)
to be available to perform reads and writes. A failure can be simulated by
killing one or two of the containers with `docker kill`.


