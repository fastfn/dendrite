package dendrite

import (
	"time"
)

type Transport interface {
	// Gets a list of the vnodes on the box
	//ListVnodes(string) ([]*Vnode, error)

	// Ping a Vnode, check for liveness
	//Ping(*Vnode) (bool, error)

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
		Hostname:      hostname,
		NumVnodes:     8,
		StabilizeMin:  15 * time.Second,
		StabilizeMax:  45 * time.Second,
		NumSuccessors: 8,
	}
}
