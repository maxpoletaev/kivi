# 🥝 Kivi DB

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

Kivi uses LSM-Tree as a storage engine and a SWIM-like protocol for cluster
membership and failure detection. On top of that, there is a replication layer
that coordinates reads and writes to multiple nodes according to the desired
consistency level.

## How to try it out

The `docker-compose.yaml` contains a minimal configuration of a cluster of
three replicas. To run it, use:

 1. `make image`
 2. `docker compose up`

With the default consistency level, you need the majority of nodes (2 out of 3)
to be available to perform reads and writes. A failure can be simulated by
killing one or two of the containers with `docker kill`.

## REST API

The REST API is available at `localhost:8001-8003`. It is a simple wrapper around
the existing GRPC API, allowing to perform reads and writes via HTTP requests and
check the health of the cluster. Note that the REST API is not part of the core 
functionality of the database and is only provided for convenience.

The following endpoints are available:

### `GET /kv/{key}`

*Getting the value of a key in the database.*

<details>
<summary><strong>Response - No value</strong></summary>

```
HTTP/1.1 200 OK

{
   "Found": false,
   "Version": ""
}
```
</details>

<details>
<summary><strong>Response - Single value</strong></summary>


```
HTTP/1.1 200 OK
Content-Type: application/json

{
  "Found": true,
  "Value": "bar",
  "Version": "CgQIARADCgQIAhABCgQIAxAC"
}
```
</details>

<details>
<summary><strong>Response - Conflicting values</strong></summary>


```
HTTP/1.1 300 Multiple Choices
Content-Type: application/json

{
  "Values": [
    "bar1", 
    "bar2", 
    "bar3"
  ],
  "Found": true,
  "Version": "CgQIARADCgQIAhABCgQIAxAC"
}
```
</details>

### `PUT /kv/{key}`

*Updating or inserting a new value into the database. In case of updating an
existing value, the version of the previous value must be provided. The 
response will contain the updated version of a value.*

<details>
<summary><strong>Request - Inserting a new key</strong></summary>

```
PUT /kv/foo
Content-Type: application/json

{
   "Value": "bar"
}
```
</details>

<details>
<summary><strong>Request - Updating an existing key</strong></summary>

```
PUT /kv/foo
Content-Type: application/json

{
   "Value": "bar2"
   "Version": "CgQIAxAB"
}
```
</details>

<details>
<summary><strong>Response</strong></summary>

```
HTTP/1.1 200 OK
Content-Type: application/json

{
  "Version": "CgQIAxAB"
}
```
</details>

### `DELETE /kv/{key}`

*Deleting a key from the database. The version of the current value must be
provided. In case there is a concurrent update, only the versions that are bellow
the provided version will be deleted. Since delete is basically inserting a new
empty value, the version is updated and returned in the response.*

<details>
<summary><strong>Request</strong></summary>

```
DELETE /kv/foo
Content-Type: application/json

{
   "version": "CgQIAxAB"
}
```
</details>

### `GET /cluster/nodes`

*Getting the list of nodes in the cluster and their status.*

<details>
<summary><strong>Response</strong></summary>

```
HTTP/1.1 200 OK
Content-Type: application/json

[
  {
    "ID": 1,
    "Name: "node1",
    "Addr": "172.24.2.1:3000",
    "Status": "left",
  },
  {
    "ID": 2,
    "Name: "node2",
    "Addr": "172.24.2.2:3000",
    "Status": "healthy",
  }
]
```
</details>