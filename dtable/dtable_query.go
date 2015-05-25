package dtable

import (
	"fmt"
	"github.com/fastfn/dendrite"
	"sync"
	"time"
)

type queryType int

const (
	qErr queryType = -1
	qGet queryType = 0
	qSet queryType = 1
)

// Query is dtable's native interface for doing data operations.
type Query interface {
	Consistency(int) Query
	Get([]byte) (*KVItem, error)
	Set([]byte, []byte) error // (key, val)
	GetLocalKeys() [][]byte
}

// Query defines a dtable query.
type query struct {
	dt      *DTable
	qType   queryType
	minAcks int
	kvItem  *kvItem
	err     error
}

// NewQuery returns Query.
func (dt *DTable) NewQuery() Query {
	return &query{
		dt:      dt,
		qType:   -1,
		minAcks: 1,
	}
}

// Consistency is used prior to Set() to request minimum writes before operation returns success.
// If dtable runs with 2 replicas, user may request 2 writes (primary + 1 replica) and let dtable
// handle final write in the background. If requested value is larger than configured dendrite replicas,
// value is reset to 1. Default is 1.
func (q *query) Consistency(n int) Query {
	if n >= 1 && n <= q.dt.ring.Replicas()+1 {
		q.minAcks = n
	}
	return q
}

// Get returns *KVItem for a key. If key is not found on this node, but node holds key replica, replica is returned.
// If key is not found on this node, and node does not hold replica, request is forwarded to the node responsible
// for this key. *KVItem is nil if key was not found, and error is set if there was an error during request.
func (q *query) Get(key []byte) (*KVItem, error) {
	if key == nil || len(key) == 0 {
		return nil, fmt.Errorf("key can not be nil or empty")
	}
	reqItem := new(kvItem)
	reqItem.Key = key
	reqItem.keyHash = dendrite.HashKey(key)

	item, err := q.dt.get(reqItem)
	if err != nil {
		return nil, err
	}
	if item == nil {
		return nil, nil
	}

	return &item.KVItem, nil
}

// Set writes to dtable.
func (q *query) Set(key, val []byte) error {
	if key == nil || len(key) == 0 {
		return fmt.Errorf("key can not be nil or empty")
	}
	q.qType = qSet
	reqItem := new(kvItem)
	reqItem.lock = new(sync.Mutex)

	reqItem.Key = make([]byte, len(key))
	copy(reqItem.Key, key)

	if val != nil {
		reqItem.Val = make([]byte, len(val))
		copy(reqItem.Val, val)
	}

	reqItem.keyHash = dendrite.HashKey(key)
	reqItem.timestamp = time.Now()
	reqItem.replicaInfo = new(kvReplicaInfo)
	reqItem.replicaInfo.vnodes = make([]*dendrite.Vnode, q.dt.ring.Replicas())
	reqItem.replicaInfo.orphan_vnodes = make([]*dendrite.Vnode, 0)

	wait := make(chan error)
	succs, err := q.dt.ring.Lookup(1, reqItem.keyHash)
	if err != nil {
		return err
	}
	if len(succs) != 1 || succs[0] == nil {
		return fmt.Errorf("successor lookup failed for key, %x", reqItem.keyHash)
	}
	// see if this node is responsible for this key
	_, ok := q.dt.table[succs[0].String()]
	if ok {
		go q.dt.set(succs[0], reqItem, q.minAcks, wait)
	} else {
		// pass to remote
		reqItem.replicaInfo.master = succs[0]
		go q.dt.remoteSet(succs[0], succs[0], reqItem, q.minAcks, false, wait)
	}
	err = <-wait
	return err
}

// GetLocalKeys returns the list of keys that are stored on this node (across all vnodes).
func (q *query) GetLocalKeys() [][]byte {
	rv := make([][]byte, 0)
	for _, table := range q.dt.table {
		for _, item := range table {
			copy_key := make([]byte, len(item.Key))
			copy(copy_key, item.Key)
			rv = append(rv, copy_key)
		}
	}
	return rv
}
