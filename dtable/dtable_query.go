package dtable

import (
	"fmt"
	//"log"
)

type queryType int

const (
	qErr queryType = -1
	qGet queryType = 0
	qSet queryType = 1
)

type Query struct {
	dt       *DTable
	qType    queryType
	replicas int
	minAcks  int
	key      []byte
	val      []byte
	err      error
}

func (dt *DTable) NewQuery() *Query {
	return &Query{
		dt:    dt,
		qType: -1,
	}
}

func (q *Query) Consistency(n int) *Query {
	q.minAcks = n
	return q
}

func (q *Query) Get(key []byte) *Query {
	q.key = key
	q.qType = qGet
	return q
}

func (q *Query) Set(key, val []byte) *Query {
	q.key = key
	q.val = val
	q.qType = qSet
	return q
}

func (q *Query) Exec() ([]byte, error) {
	switch q.qType {
	case qGet:
		return q.dt.get(q.key)
	case qSet:
		return nil, q.dt.set(q.key, q.val)
	default:
		return nil, fmt.Errorf("unknown query")
	}
}
