/*
	Package dtable implements highly available, distributed in-memory key/value datastore.

	DTable is built on top of dendrite for key distribution and high availability, replication
	and failover. It exposes Query interface for Get() and Set() operations.

	It hooks on dendrite as a TransportHook and uses ZeroMQ for communication between remote nodes.
	All messages between the nodes are serialized with protocol buffers.
*/
package dtable
