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
