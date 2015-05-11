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
	key     []byte
	val     *value
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

func (q *Query) Get(key []byte) *Query {
	q.key = key
	q.qType = qGet
	return q
}

func (q *Query) Set(key, val []byte) *Query {
	q.key = dendrite.HashKey(key)
	q.val = &value{
		Val:       val,
		isReplica: false,
		commited:  false,
		rstate:    replicaIncomplete,
		timestamp: time.Now(),
	}
	q.qType = qSet
	return q
}

func (q *Query) Exec() ([]byte, error) {
	switch q.qType {
	case qGet:
		return q.dt.get(q.key)
	case qSet:
		wait := make(chan error)
		succs, err := q.dt.ring.Lookup(1, q.key)
		if err != nil {
			return nil, err
		}
		if len(succs) != 1 || succs[0] == nil {
			return nil, fmt.Errorf("successor lookup failed for key, %x", q.key)
		}
		// see if this node is responsible for this key
		_, ok := q.dt.table[succs[0].String()]
		if ok {
			go q.dt.set(succs[0], q.key, q.val, q.minAcks, wait)
		} else {
			// pass to remote
			go q.dt.remoteSet(succs[0], q.key, q.val, q.minAcks, wait)
		}
		err = <-wait
		return nil, err
	default:
		return nil, fmt.Errorf("unknown query")
	}
}
