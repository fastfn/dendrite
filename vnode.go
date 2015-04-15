package dendrite

import (
	"bytes"
	"crypto/sha1"
	"encoding/binary"
	"fmt"
	"log"
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
	ring        *Ring
	successors  []*Vnode // "backlog" of known successors
	finger      []*Vnode
	last_finger int
	predecessor *Vnode
	stabilized  time.Time
	timer       *time.Timer
}

func (vn *localVnode) init(idx int) {
	// combine hostname with idx to generate hash
	hash := sha1.New()
	hash.Write([]byte(vn.ring.config.Hostname))
	binary.Write(hash, binary.BigEndian, uint16(idx))
	vn.Id = hash.Sum(nil)
	vn.Host = vn.ring.config.Hostname
	vn.successors = make([]*Vnode, vn.ring.config.NumSuccessors)
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
	for {
		if vn.successors[0] == nil {
			panic("Node has no more successors :(")
		}
		// Ask our successor for it's predecessor
		maybe_suc, err := vn.ring.transport.GetPredecessor(vn.successors[0])
		if err != nil {
			log.Println("[stabilize]... trying next known successor due to error:", err)
			copy(vn.successors[0:], vn.successors[1:])
			continue
		}

		if maybe_suc != nil && between(vn.Id, vn.successors[0].Id, maybe_suc.Id, false) {
			alive, _ := vn.ring.transport.Ping(maybe_suc)
			if alive {
				copy(vn.successors[1:], vn.successors[0:len(vn.successors)-1])
				vn.successors[0] = maybe_suc
				log.Println("[stabilize] new successor set")
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
		}
	}
	return nil
}

// Notifies our successor of us, updates successor list
func (vn *localVnode) notifySuccessor() error {
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
		vn.successors[idx+1] = s
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
