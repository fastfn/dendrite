# Dendrite

Package dendrite implements a distributed hash table (DTH) based on Chord Protocol.
Included sub-package 'dtable' is built on top of dendrite and implements
distributed in-memory key/value database, with replication and failover support,
with query interface to Get() or Set() items with different consistency levels.

For better key distribution, dendrite allows configurable number of virtual nodes
per instance (vnodes). The number of replicas in dtable is also configurable.

Calling application can bootstrap the cluster, or join existing one by connecting to any of
existing nodes (must be manually specified). Node discovery is not part of the implementation.
Use consul (consul.io) or something else for that purpose.

Chord protocol defines ring stabilization. In dendrite, stabilization period is configurable.

Node to node (network) communication is built on top of TCP over ZeroMQ sockets for speed, clustering
and reliability. Dendrite starts configurable number of goroutines (default: 10) for load balanced
serving of remote requests, but scales that number up and down depending on the load (aka prefork model).

All messages sent through dendrite are encapsulated in ChordMsg structure, where first byte indicates message type,
and actual data follows. Data part is serialized with protocol buffers.

Dendrite can be extended through two interfaces:
- TransportHook
- DelegateHook

TransportHook allows other packages to provide additional message types, decoders and handlers, while DelegateHook
can be used to capture chord events that dendrite emits:
- EvPredecessorJoined
- EvPredecessorLeft
- EvReplicasChanged


## Usage


## Todo
- dtable: support SetMulti() and GetMulti() on public interface
- dendrite: add some kind of security for inter communication between nodes
- dtable: support batches on replication/migration ops

