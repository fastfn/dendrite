package dtable

import (
	"fmt"
	"github.com/fastfn/dendrite"
	"time"
	//"log"
)

type queryType int

const (
	qErr queryType = -1
	qGet queryType = 0
	qSet queryType = 1
)

type Query struct {
	dt      *DTable
	qType   queryType
	minAcks int
	kvItem  *kvItem
	err     error
}

func (dt *DTable) NewQuery() *Query {
	return &Query{
		dt:      dt,
		qType:   -1,
		minAcks: 1,
	}
}

// we always do 1 write
func (q *Query) Consistency(n int) *Query {
	if n >= 1 && n <= q.dt.ring.Replicas()+1 {
		q.minAcks = n
	}
	return q
}

func (q *Query) Get(key []byte) (*KVItem, error) {
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

func (q *Query) Set(key, val []byte) error {
	q.qType = qSet
	reqItem := new(kvItem)

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
		go q.dt.remoteSet(succs[0], succs[0], reqItem, q.minAcks, false, wait)
	}
	err = <-wait
	return err
}
