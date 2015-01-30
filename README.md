Chord protocol implementation.
Messaging with protocol buffers over zeromq sockets for speed and reliability.
Primary purpose: to be used as a backbone library for stream processing pipeline, but anything's possible.
Features:
 - complete chord protocol implementation. Uses 160bit(sha1) keyspace.
 - reliability comes first.
 - dtable: in-memory KV store implementation

Todo:
 - write more/better documentation
 - dendrite: implement delegate hooks for node leave event
 - dtable: use delegate hooks to copy data to another node
 - dtable: implement replication and data failover

