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
	FindRemoteSuccessors(int) ([]*Vnode, error)
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
		print_pred := &Vnode{}
		if vn.old_predecessor != nil {
			print_pred = vn.old_predecessor
		}
		log.Printf("Setting new predecessor for %X : %X -> %X\n", vn.Id, print_pred.Id, maybe_pred.Id)
		// maybe we're just joining and one of our local vnodes is closer to us than this predecessor
		vn.ring.Delegate(&vn.Vnode, vn.old_predecessor, maybe_pred, vn.delegateMux)
		vn.predecessor = maybe_pred
	}

	// Return our successors list
	return vn.successors, nil
}

// FindRemoteSuccessors returns up to 'limit' successor vnodes,
// that are uniq and do not reside on same physical node as vnode
// it is asumed this method is called from origin vnode
func (vn *localVnode) FindRemoteSuccessors(limit int) ([]*Vnode, error) {
	remote_succs := make([]*Vnode, 0)
	seen_vnodes := make(map[string]bool)
	seen_hosts := make(map[string]bool)
	seen_vnodes[vn.String()] = true
	seen_hosts[vn.Host] = true
	var pivot_succ *Vnode

	for _, succ := range vn.successors {
		if len(remote_succs) == limit {
			return remote_succs, nil
		}
		if succ == nil {
			continue
		}
		seen_vnodes[succ.String()] = true
		pivot_succ = succ
		if _, ok := seen_hosts[succ.Host]; ok {
			continue
		}
		if succ.Host == vn.Host {
			continue
		}
		seen_hosts[succ.Host] = true
		remote_succs = append(remote_succs, succ)
	}

	// forward through pivot successor until we reach the limit or detect loopback
	for {
		if len(remote_succs) == limit {
			return remote_succs, nil
		}
		if pivot_succ == nil {
			return remote_succs, nil
		}
		next_successors, err := vn.ring.transport.FindSuccessors(pivot_succ, vn.ring.config.NumSuccessors, pivot_succ.Id)
		if err != nil {
			return nil, err
		}
		for _, succ := range next_successors {
			if len(remote_succs) == limit {
				return remote_succs, nil
			}
			if succ == nil {
				continue
			}
			if _, ok := seen_vnodes[succ.String()]; ok {
				// loop detected, must return
				return remote_succs, nil
			}
			seen_vnodes[succ.String()] = true
			pivot_succ = succ
			if _, ok := seen_hosts[succ.Host]; ok {
				// we have this host already
				continue
			}
			seen_hosts[succ.Host] = true
			remote_succs = append(remote_succs, succ)
		}
	}
	return remote_succs, nil
}
