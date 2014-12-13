package dendrite

import (
	"bytes"
	"sort"
	"time"
)

type MsgType byte
type ChordMsg struct {
	Type MsgType
	Data []byte
}
type Transport interface {
	// Gets a list of the vnodes on the box
	ListVnodes(string) ([]*Vnode, error)

	// Ping a Vnode, check for liveness
	Ping(*Vnode) (bool, error)

	// Request a nodes predecessor
	//GetPredecessor(*Vnode) (*Vnode, error)

	// Notify our successor of ourselves
	//Notify(target, self *Vnode) ([]*Vnode, error)

	// Find a successor
	//FindSuccessors(*Vnode, int, []byte) ([]*Vnode, error)

	// Clears a predecessor if it matches a given vnode. Used to leave.
	//ClearPredecessor(target, self *Vnode) error

	// Instructs a node to skip a given successor. Used to leave.
	//SkipSuccessor(target, self *Vnode) error

	// Register listener
	//Register(*Vnode, VnodeRPC)

	// encode encodes dendrite msg into two frame byte stream
	// first byte is message type, and the rest is protobuf data
	Encode(MsgType, []byte) []byte
	// decode reverses the above process
	Decode([]byte) (*ChordMsg, error)
}

type Config struct {
	Hostname      string
	NumVnodes     int // num of vnodes to create
	StabilizeMin  time.Duration
	StabilizeMax  time.Duration
	NumSuccessors int // number of successor to keep in self log
}

func DefaultConfig(hostname string) *Config {
	return &Config{
		Hostname: hostname,
		// NumVnodes should be set around logN
		// N is approximate number of real nodes in cluster
		// this way we get O(logN) lookup speed
		NumVnodes:     3,
		StabilizeMin:  15 * time.Second,
		StabilizeMax:  45 * time.Second,
		NumSuccessors: 8, // number of known successors to keep track with
	}
}

type Ring struct {
	config    *Config
	transport Transport
	vnodes    []*localVnode
	shutdown  chan bool
}

// implement sort.Interface (Len(), Less() and Swap())
func (r *Ring) Less(i, j int) bool {
	return bytes.Compare(r.vnodes[i].Id, r.vnodes[j].Id) == -1
}

func (r *Ring) Swap(i, j int) {
	r.vnodes[i], r.vnodes[j] = r.vnodes[j], r.vnodes[i]
}

func (r *Ring) Len() int {
	return len(r.vnodes)
}

func CreateRing(config *Config, transport Transport) (*Ring, error) {
	r := &Ring{
		config:    config,
		transport: transport,
		vnodes:    make([]*localVnode, config.NumVnodes),
		shutdown:  make(chan bool),
	}
	// initialize vnodes
	for i := 0; i < config.NumVnodes; i++ {
		vn := &localVnode{}
		r.vnodes[i] = vn
		vn.ring = r
		vn.init(i)
	}
	sort.Sort(r)

	// for each vnode, setup successors
	numV := len(r.vnodes)
	numSuc := min(r.config.NumSuccessors, numV-1)
	for idx, vnode := range r.vnodes {
		for i := 0; i < numSuc; i++ {
			vnode.successors[i] = &r.vnodes[(idx+i+1)%numV].Vnode
		}
	}

	// schedule vnode stabilizers
	for i := 0; i < len(r.vnodes); i++ {
		r.vnodes[i].schedule()
	}
	return r, nil
}

func JoinRing(config *Config, transport Transport, existing string) (*Ring, error) {
	r := &Ring{
		config:    config,
		transport: transport,
		vnodes:    make([]*localVnode, config.NumVnodes),
		shutdown:  make(chan bool),
	}
	// initialize vnodes
	for i := 0; i < config.NumVnodes; i++ {
		vn := &localVnode{}
		r.vnodes[i] = vn
		vn.ring = r
		vn.init(i)
	}
	sort.Sort(r)
	r.transport.Ping(&Vnode{Host: existing})
	return r, nil
}
