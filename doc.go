/*
Package dendrite implements a distributed hash table (DTH) based on Chord Protocol.
Included sub-package 'dtable' is built on top of dendrite and implements
distributed in-memory key/value database, with replication and failover support,
with query interface to Get() or Set() items with different consistency levels.

For better key distribution, dendrite allows configurable number of virtual nodes
per instance (vnodes).
*/
package dendrite
