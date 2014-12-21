package dendrite

import (
	"fmt"
	//"log"
)

// handles vnode operations
// transports use this interface to avoid duplicate implementations
type VnodeHandler interface {
	FindSuccessors([]byte, int) ([]*Vnode, *Vnode, error) // args: key, limit # returns: succs, forward, error
	GetPredecessor() (*Vnode, error)
	Notify(*Vnode) ([]*Vnode, error)
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
	} else {
		//log.Printf("between returned false for %X, %X, key: %X\n", vn.Id, vn.successors[0].Id, key)
	}
	// if finger table has been initialized - forward request to closest finger
	// otherwise forward to my successor
	var forward_vn *Vnode
	if len(vn.finger) == 0 {
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

// Notify is invoked when a Vnode gets notified
func (vn *localVnode) Notify(maybe_pred *Vnode) ([]*Vnode, error) {
	// Check if we should update our predecessor
	if vn.predecessor == nil || between(vn.predecessor.Id, vn.Id, maybe_pred.Id, false) {
		// Inform the delegate
		//conf := vn.ring.config
		//old := vn.predecessor
		/*
			vn.ring.invokeDelegate(func() {
				conf.Delegate.NewPredecessor(&vn.Vnode, maybe_pred, old)
			})
		*/
		vn.predecessor = maybe_pred
	}

	// Return our successors list
	return vn.successors, nil
}
