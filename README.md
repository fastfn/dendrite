Chord protocol implementation.
Messaging with protocol buffers over zeromq sockets for speed and reliability.
Primary purpose: to be used as a backbone library for stream processing pipeline, but anything's possible.
Features:
 - chord protocol implementation with health checks and everything. Uses SHA1 for keyspace
 - because it's meant to be highly available, reliability comes first.
 - provides interface for writing replicated data store implementations
 - in-memory KV store implementation

