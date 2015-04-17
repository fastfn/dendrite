package dendrite

import (
	"bytes"
	//"fmt"
	"log"
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
	succs := make([]*Vnode, 0)
	max_vnodes := min(limit, len(vn.successors))
	if bytes.Compare(key, vn.Id) == 0 || between(vn.Id, vn.successors[0].Id, key, true) {
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

	forward_vn := vn.closest_preceeding_finger(key)

	// if we got ourselves back, that's it - I'm the successor
	if bytes.Compare(forward_vn.Id, vn.Id) == 0 {
		succs = append(succs, &Vnode{
			Id:   vn.Id,
			Host: vn.Host,
		})
		for i := 1; i < max_vnodes; i++ {
			if vn.successors[i-1] == nil {
				break
			}
			succs = append(succs, vn.successors[i-1])
		}
		return succs, nil, nil
	}
	//log.Printf("findsuccessor (%X) forwarding to %X\n", vn.Id, forward_vn.Id)
	return nil, forward_vn, nil
}

func (vn *localVnode) GetPredecessor() (*Vnode, error) {
	if vn.predecessor == nil {
		return nil, nil
	}
	return vn.predecessor, nil
}

// Notify is invoked when a Vnode gets notified
func (vn *localVnode) Notify(maybe_pred *Vnode) ([]*Vnode, error) {
	// Check if we should update our predecessor
	if vn.predecessor == nil || between(vn.predecessor.Id, vn.Id, maybe_pred.Id, false) {
		log.Printf("Setting new predecessor for %X - %X\n", vn.Id, maybe_pred.Id)
		go vn.ring.Delegate(&vn.Vnode, vn.predecessor, maybe_pred)
		vn.predecessor = maybe_pred
	}

	// Return our successors list
	return vn.successors, nil
}
