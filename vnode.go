package dendrite

import (
	"crypto/sha1"
	"encoding/binary"
	"time"
)

// Any vnode (local or remote)
type Vnode struct {
	Id   []byte
	Host string // ip:port
}

func (vn *Vnode) selfVnode() *Vnode {
	return vn
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
}

// Schedules the Vnode to do regular maintenence
func (vn *localVnode) schedule() {
	// Setup our stabilize timer
	vn.timer = time.AfterFunc(randStabilize(vn.ring.config), vn.stabilize)
}

func (vn *localVnode) stabilize() {
	defer vn.schedule()
}

// returns successor for requested id
// second return argument indicates whether client should forward request to another node
func (vn *localVnode) find_successor(id []byte) (*Vnode, bool) {
	// check if Id falls between me and my successor
	if between(vn.Id, vn.successors[0].Id, id, true) {
		return vn.successors[0], true
	}
	return vn.closest_preceeding_finger(id), false
}

// Find closest preceeding finger node
func (vn *localVnode) closest_preceeding_finger(id []byte) *Vnode {
	// keysize(i) down to 1
	for i := len(vn.finger) - 1; i >= 0; i-- {
		if vn.finger[i] == nil {
			continue
		}
		// check if id falls after this finger (finger[i] IN (n, id))
		if between(vn.Id, id, vn.finger[i].Id, false) {
			return vn.finger[i]
		}
	}
	return vn.selfVnode()
}
