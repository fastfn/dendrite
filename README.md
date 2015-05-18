Chord protocol implementation.
Messaging with protocol buffers over zeromq sockets for speed and reliability.
Primary purpose: to be used as a backbone library for stream processing pipeline, but anything's possible.
Features:
 - complete chord protocol implementation. Uses 160bit(sha1) keyspace.
 - reliability comes first.
 - dtable: in-memory KV store implementation

Todo:
 - write more/better documentation
 - dtable: support SetMulti() and GetMulti() on public interface
 - dendrite: add some kind of security for inter communication between nodes
 - dtable: support batches on replication/migration ops

