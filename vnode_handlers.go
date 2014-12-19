package dendrite

import (
	"fmt"
)

// handles vnode operations
// transports use this interface to avoid duplicate implementations
type VnodeHandler interface {
	FindSuccessors([]byte, int) ([]*Vnode, *Vnode, error) // args: key, limit # returns: succs, forward, error
	GetPredecessor() (*Vnode, error)
}

// localVnode implements vnodeHandler interface
// vn here is just for convenience because we're often passing *Vnode
type localHandler struct {
	vn      *Vnode
	handler VnodeHandler
}

func (vn *localVnode) FindSuccessors(key []byte, limit int) ([]*Vnode, *Vnode, error) {
	// check if we have direct successor for requested key
	if between(vn.Id, vn.successors[0].Id, key, true) {
		succs := make([]*Vnode, 0)
		max_vnodes := min(limit, len(vn.successors))
		for i := 0; i < max_vnodes; i++ {
			if vn.successors[i] == nil {
				continue
			}
			succs = append(succs, &Vnode{
				Id:   vn.successors[i].Id,
				Host: vn.successors[i].Host,
			})
		}
		return succs, nil, nil
	}
	// if finger table has been initialized - forward request to closest finger
	// otherwise forward to my successor
	var forward_vn *Vnode
	if vn.stabilized.IsZero() {
		forward_vn = vn.successors[0]
	} else {
		forward_vn = vn.closest_preceeding_finger(key)
	}
	return nil, forward_vn, nil
}

func (vn *localVnode) GetPredecessor() (*Vnode, error) {
	if vn.predecessor == nil {
		return nil, fmt.Errorf("predecessor not set yet")
	}
	return vn.predecessor, nil
}
