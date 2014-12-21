package dendrite

import (
	"bytes"
	"fmt"
	//"log"
	"sort"
	"time"
)

type MsgType byte
type ChordMsg struct {
	Type             MsgType
	Data             []byte
	TransportMsg     interface{} // unmarshalled data, depending on transport
	TransportHandler func(*ChordMsg, chan *ChordMsg)
}

type Transport interface {
	// Gets a list of the vnodes on the box
	ListVnodes(string) ([]*Vnode, error)

	// Ping a Vnode, check for liveness
	Ping(*Vnode) (bool, error)

	// Request a vnode's predecessor
	GetPredecessor(*Vnode) (*Vnode, error)

	// Notify our successor of ourselves
	Notify(dest, self *Vnode) ([]*Vnode, error)

	// Find a successors for vnode key
	FindSuccessors(*Vnode, int, []byte) ([]*Vnode, error)

	// Clears a predecessor if it matches a given vnode. Used to leave.
	//ClearPredecessor(target, self *Vnode) error

	// Instructs a node to skip a given successor. Used to leave.
	//SkipSuccessor(target, self *Vnode) error

	// Register vnode handlers
	Register(*Vnode, VnodeHandler)

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
		StabilizeMin:  7 * time.Second,
		StabilizeMax:  10 * time.Second,
		NumSuccessors: 8, // number of known successors to keep track with
	}
}

type Ring struct {
	config         *Config
	transport      Transport
	vnodes         []*localVnode
	shutdown       chan bool
	Stabilizations int
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

// Initializes the vnodes with their local successors
// Vnodes need to be sorted before this method is called
func (r *Ring) setLocalSuccessors() {
	numV := len(r.vnodes)
	if numV == 1 {
		for _, vnode := range r.vnodes {
			vnode.successors[0] = &vnode.Vnode
		}
		return
	}
	// we use numV-1 in order to avoid setting ourselves as last successor
	numSuc := min(r.config.NumSuccessors, numV-1)
	for idx, vnode := range r.vnodes {
		for i := 0; i < numSuc; i++ {
			vnode.successors[i] = &r.vnodes[(idx+i+1)%numV].Vnode
		}
	}

}

func (r *Ring) init(config *Config, transport Transport) {
	r.config = config
	r.transport = InitLocalTransport(transport)
	r.vnodes = make([]*localVnode, config.NumVnodes)
	r.shutdown = make(chan bool)
	// initialize vnodes
	for i := 0; i < config.NumVnodes; i++ {
		vn := &localVnode{}
		r.vnodes[i] = vn
		vn.ring = r
		vn.init(i)
	}
	sort.Sort(r)
}

func (r *Ring) schedule() {
	for i := 0; i < len(r.vnodes); i++ {
		r.vnodes[i].schedule()
	}
}
func CreateRing(config *Config, transport Transport) (*Ring, error) {
	// initialize the ring and sort vnodes
	r := &Ring{}
	r.init(config, transport)

	// for each vnode, setup local successors
	r.setLocalSuccessors()

	// schedule vnode stabilizers
	r.schedule()

	return r, nil
}

func JoinRing(config *Config, transport Transport, existing string) (*Ring, error) {
	hosts, err := transport.ListVnodes(existing)
	if err != nil {
		return nil, err
	}
	if hosts == nil || len(hosts) == 0 {
		return nil, fmt.Errorf("Remote host has no vnodes registered yet")
	}

	// initialize the ring and sort vnodes
	r := &Ring{}
	r.init(config, transport)

	// for each vnode, get the new list of live successors from remote
	for _, vn := range r.vnodes {
		resolved := false
		var last_error error
		// go through each host until we get successor list from one of them
		for _, remote_host := range hosts {
			succs, err := transport.FindSuccessors(remote_host, config.NumSuccessors, vn.Id)
			if err != nil {
				last_error = err
				continue
			}
			if succs == nil || len(succs) == 0 {
				return nil, fmt.Errorf("Failed to find successors for vnode, got empty list")
			}
			for idx, s := range succs {
				vn.successors[idx] = s
			}
			resolved = true
		}
		if !resolved {
			return nil, fmt.Errorf("Exhausted all remote vnodes while trying to get the list of successors. Last error: %s", last_error.Error())
		}

	}
	r.transport.Ping(&Vnode{Host: existing})

	// We can now initiate stabilization protocol
	for _, vn := range r.vnodes {
		vn.stabilize()
	}
	return r, nil
}
