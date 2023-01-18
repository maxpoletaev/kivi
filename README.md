# 🥝 Kiwi DB

The goal of the project is to build a simple distributed key-value store from
scratch, without relying much on third-party libraries. The project falls into
the category of Dynamo-like databases, such as Cassandra and Riak, the key
properties of which are:

 * All nodes have the capability to accept read and write operations, eliminating
   the need for leader/follower separation.
 * The system implements quorum-based reads and writes for data durability and
    availability, by requiring a minimum number of replicas to acknowledge
    a read or write operation.
  * The system is eventually consistent, utilizing techniques such as read
    repair to achieve consistency among replicas after temporary failures.
  * Concurrent writes to the same key may result in conflicting updates. When
     conflicts are detected, clients must either perform conflict resolution
     before the next write or utilize automatic resolution strategies such
     as last-write-wins or use conflict-free data structures (CRDT).

Kiwi uses LSM-Tree as a storage engine and a SWIM-like protocol for cluster
membership and failure detection. On top of that, there is a replication layer
that coordinates reads and writes to multiple nodes according to the desired
consistency level.

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


