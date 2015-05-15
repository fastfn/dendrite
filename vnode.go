package dendrite

import (
	"bytes"
	"crypto/sha1"
	"encoding/binary"
	"fmt"
	"log"
	"sync"
	"time"
)

// Any vnode (local or remote)
type Vnode struct {
	Id   []byte
	Host string // ip:port
}

func (vn *Vnode) String() string {
	return fmt.Sprintf("%x", vn.Id)
}

// local Vnode
type localVnode struct {
	Vnode
	ring              *Ring
	successors        []*Vnode // "backlog" of known successors
	remote_successors []*Vnode
	finger            []*Vnode
	last_finger       int
	predecessor       *Vnode
	old_predecessor   *Vnode
	stabilized        time.Time
	timer             *time.Timer
	delegateMux       sync.Mutex
}

func (vn *localVnode) init(idx int) {
	// combine hostname with idx to generate hash
	hash := sha1.New()
	hash.Write([]byte(vn.ring.config.Hostname))
	binary.Write(hash, binary.BigEndian, uint16(idx))
	vn.Id = hash.Sum(nil)
	vn.Host = vn.ring.config.Hostname
	vn.successors = make([]*Vnode, vn.ring.config.NumSuccessors)
	vn.remote_successors = make([]*Vnode, vn.ring.config.Replicas)
	vn.finger = make([]*Vnode, 160) // keyspace size is 160 with SHA1
	vn.ring.transport.Register(&vn.Vnode, vn)
}

// Schedules the Vnode to do regular maintenence
func (vn *localVnode) schedule() {
	// Setup our stabilize timer
	vn.timer = time.AfterFunc(randStabilize(vn.ring.config), vn.stabilize)
}

func (vn *localVnode) stabilize() {
	defer vn.schedule()

	start := time.Now()
	if err := vn.checkNewSuccessor(); err != nil {
		log.Println("[stabilize] Error checking successor:", err)
	}
	//log.Printf("CheckSucc returned for %X - %X\n", vn.Id, vn.successors[0].Id)

	// Notify the successor
	if err := vn.notifySuccessor(); err != nil {
		log.Println("[stabilize] Error notifying successor:", err)
	}
	//log.Printf("NotifySucc returned for %X\n", vn.Id)

	if err := vn.fixFingerTable(); err != nil {
		log.Println("[stabilize] Error fixing finger table, last:", time.Since(start), vn.last_finger, err)
	}

	if err := vn.checkPredecessor(); err != nil {
		log.Printf("[stabilize] Predecessor failed for %X - %s\n", vn.Id, err)
	}
	//log.Println("[stabilize] completed in", time.Since(start))
}

// Find closest preceeding finger node
func (vn *localVnode) closest_preceeding_finger(id []byte) *Vnode {
	var finger_node, successor_node *Vnode

	// loop through finger table, keysize(i) down to 1
	for i := vn.last_finger; i >= 0; i-- {
		if vn.finger[i] == nil {
			continue
		}
		// check if id falls after this finger (finger[i] IN (n, id))
		if between(vn.Id, id, vn.finger[i].Id, false) {
			finger_node = vn.finger[i]
			break
		}
	}

	// loop through successors
	for i := len(vn.successors) - 1; i >= 1; i-- {
		if vn.successors[i] == nil {
			continue
		}
		if between(vn.Id, id, vn.successors[i].Id, false) {
			successor_node = vn.successors[i]
			break
		}
	}

	// return the best result
	if finger_node == nil {
		if successor_node == nil {
			return &vn.Vnode
		}
		return successor_node
	}
	if successor_node == nil {
		return finger_node
	}

	finger_dist := distance(vn.Id, finger_node.Id)
	successor_dist := distance(vn.Id, successor_node.Id)
	if finger_dist.Cmp(successor_dist) <= 0 {
		return successor_node
	} else {
		return finger_node
	}
	return nil
}

// Check if there's new successor ahead
func (vn *localVnode) checkNewSuccessor() error {
	update_remotes := false
	for {
		if vn.successors[0] == nil {
			log.Fatal("Node has no more successors :(")
		}
		// Ask our successor for it's predecessor
		maybe_suc, err := vn.ring.transport.GetPredecessor(vn.successors[0])
		if err != nil {
			log.Println("[stabilize]... trying next known successor due to error:", err)
			copy(vn.successors[0:], vn.successors[1:])
			update_remotes = true
			continue
		}

		if maybe_suc != nil && between(vn.Id, vn.successors[0].Id, maybe_suc.Id, false) {
			alive, _ := vn.ring.transport.Ping(maybe_suc)
			if alive {
				copy(vn.successors[1:], vn.successors[0:len(vn.successors)-1])
				vn.successors[0] = maybe_suc
				update_remotes = true
				log.Printf("[stabilize] new successor set: %X -> %X\n", vn.Id, maybe_suc.Id)
			} else {
				// skip this one, it's not alive
				//log.Println("[stabilize] new successor found, but it's not alive")
			}
			break
		} else {
			// we're good for now, checkPredcessor should fix this (maybe_suc is nil)
			break
		}
	}
	// while we're here, ping other successors to make sure they're alive
	for i := 0; i < len(vn.successors); i++ {
		if vn.successors[i] == nil {
			continue
		}
		alive, _ := vn.ring.transport.Ping(vn.successors[i])
		if !alive {
			//log.Printf("found inactive successor, removing it: %X\n", vn.successors[i].Id)
			copy(vn.successors[i:], vn.successors[i+1:])
			update_remotes = true
		}
	}

	// update remote successors if our list changed
	if update_remotes {
		vn.updateRemoteSuccessors()
	}
	return nil
}

// Notifies our successor of us, updates successor list
func (vn *localVnode) notifySuccessor() error {
	old_successors := make([]*Vnode, 0)
	copy(old_successors, vn.successors)
	// Notify successor
	succ := vn.successors[0]
	//log.Printf("Notifying successor of us: %X -> %X\n", vn.Id, succ.Id)
	succ_list, err := vn.ring.transport.Notify(succ, &vn.Vnode)
	if err != nil {
		return err
	}

	// Trim the successors list if too long
	max_succ := vn.ring.config.NumSuccessors
	if len(succ_list) > max_succ-1 {
		succ_list = succ_list[:max_succ-1]
	}

	// Update local successors list
	for idx, s := range succ_list {
		if s == nil {
			break
		}
		// Ensure we don't set ourselves as a successor!
		if s == nil || s.String() == vn.String() {
			break
		}
		//fmt.Printf("Updating successor from notifySuccessor(), %X -> %X\n", vn.Id, s.Id)
		vn.successors[idx+1] = s
	}
	// lets see if our successor list changed
	for idx, new_succ := range vn.successors {
		if bytes.Compare(new_succ.Id, old_successors[idx].Id) != 0 {
			// changed! we should update our remotes now
			vn.updateRemoteSuccessors()
			break
		}
	}
	return nil
}

// Checks the health of our predecessor
func (vn *localVnode) checkPredecessor() error {
	// Check predecessor
	if vn.predecessor != nil {
		ok, err := vn.ring.transport.Ping(vn.predecessor)
		if err != nil || !ok {
			log.Println("[stabilize] detected predecessor failure")
			vn.old_predecessor = vn.predecessor
			vn.predecessor = nil
			return err
		}
	}
	return nil
}

func (vn *localVnode) fixFingerTable() error {
	//log.Printf("Starting fixFingerTable, %X - %X\n", vn.Id, vn.successors[0].Id)
	idx := 0
	self := &vn.Vnode
	for i := 0; i < 160; i++ {
		offset := powerOffset(self.Id, i, 160)
		//log.Printf("\t\tidx: %d: %X\n", i, offset)
		succs, err := vn.ring.transport.FindSuccessors(self, 1, offset)
		if err != nil {
			vn.last_finger = idx
			return err
		}
		if succs == nil || len(succs) == 0 {
			vn.last_finger = idx
			return fmt.Errorf("no successors found for key")
		}
		// see if we already have this node, keeps finger table short
		if idx > 0 && bytes.Compare(vn.finger[vn.last_finger].Id, succs[0].Id) == 0 {
			continue
		}
		// don't set ourselves as finger
		if bytes.Compare(succs[0].Id, vn.Id) == 0 {
			//log.Printf("\t\t\t GOT OURSELVES BACK.. HOW????, skipping\n")
			break
		}
		vn.finger[idx] = succs[0]
		vn.last_finger = idx
		idx += 1
		//log.Printf("\t\t\t set id: %X\n", succs[0].Id)
	}
	return nil
}

// updateRemoteSuccessors()
func (vn *localVnode) updateRemoteSuccessors() {
	old_remotes := make([]*Vnode, 0)
	copy(old_remotes, vn.remote_successors)

	remotes, err := vn.findRemoteSuccessors(vn.ring.Replicas())
	if err != nil {
		log.Println("Error finding remote successors:", err)
		return
	}

	changed := false
	for idx, remote := range remotes {
		if remote != nil && old_remotes[idx] != nil {
			if bytes.Compare(remote.Id, old_remotes[idx].Id) != 0 {
				vn.remote_successors[idx] = remote
				changed = true
			}
		} else if remote == nil && old_remotes[idx] != nil {
			vn.remote_successors[idx] = remote
			changed = true
		} else if remote != nil && old_remotes[idx] == nil {
			vn.remote_successors[idx] = remote
			changed = true
		} else {
			// we're good
		}
	}
	if changed {
		ctx := &EventCtx{
			EvType:   EvReplicasChanged,
			Target:   &vn.Vnode,
			ItemList: vn.remote_successors,
		}
		vn.ring.emit(ctx)
	}
}

// findRemoteSuccessors returns up to 'limit' successor vnodes,
// that are uniq and do not reside on same physical node as vnode
// it is asumed this method is called from origin vnode
func (vn *localVnode) findRemoteSuccessors(limit int) ([]*Vnode, error) {
	remote_succs := make([]*Vnode, limit)
	seen_vnodes := make(map[string]bool)
	seen_hosts := make(map[string]bool)
	seen_vnodes[vn.String()] = true
	seen_hosts[vn.Host] = true
	var pivot_succ *Vnode
	num_appended := 0

	for _, succ := range vn.successors {
		if num_appended == limit {
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
		num_appended += 1
	}

	// forward through pivot successor until we reach the limit or detect loopback
	for {
		if num_appended == limit {
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
			if num_appended == limit {
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
			num_appended += 1
		}
	}
	return remote_succs, nil
}
