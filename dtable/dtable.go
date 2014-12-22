package dtable

import (
	"github.com/fastfn/dendrite"
	"time"
)

type DItem struct {
	Key       []byte
	Val       []byte
	timestamp time.Time
}

type DTable struct {
	table map[*dendrite.Vnode][]*DItem
}

func Init(ring *dendrite.Ring, transport dendrite.Transport) *DTable {
	return nil
}

// Implement dendrite TransportHook
func (dt *DTable) Decode(data []byte) (*dendrite.ChordMsg, error) {
	return nil, nil
}
