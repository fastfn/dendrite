Chord protocol implementation.
Messaging with protocol buffers over zeromq sockets for speed and reliability.
Primary purpose: to be used as a backbone library for stream processing pipeline, but anything's possible.
Features:
 - complete chord protocol implementation. Uses 160bit(sha1) keyspace.
 - reliability comes first.
 - dtable: in-memory KV store implementation

Todo:
 - force all internal ops to use Query interface (and build internal query interface) in dtable
 - write more/better documentation
 - support batches on various operations
