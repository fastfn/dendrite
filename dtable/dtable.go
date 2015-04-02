package dtable

import (
	"fmt"
	"github.com/fastfn/dendrite"
	"github.com/golang/protobuf/proto"
	"log"
	"time"
)

const (
	// message types here start at 20
	msgTypeStart                  = 20
	PbDtableGet  dendrite.MsgType = msgTypeStart + iota
	PbDtableGetResp
	PbDtableSet
	PbDtableSetResp
	PbDtableSetMulti
)

type value struct {
	Val       []byte
	timestamp time.Time
}
type kvMap map[string]*value

type DTable struct {
	table     map[string]kvMap
	ring      *dendrite.Ring
	transport dendrite.Transport
}

func Init(ring *dendrite.Ring, transport dendrite.Transport) *DTable {
	log.Printf("%x, %x\n", PbDtableGet, PbDtableGetResp)
	dt := &DTable{
		table:     make(map[string]kvMap),
		ring:      ring,
		transport: transport,
	}
	// each local vnode needs to be separate key in dtable
	for _, vnode := range ring.MyVnodes() {
		node_kv := make(map[string]*value)
		vn_key_str := fmt.Sprintf("%x", vnode.Id)
		dt.table[vn_key_str] = node_kv
	}
	transport.RegisterHook(dt)
	return dt
}

// Implement dendrite TransportHook
func (dt *DTable) Decode(data []byte) (*dendrite.ChordMsg, error) {
	data_len := len(data)
	if data_len == 0 {
		return nil, fmt.Errorf("data too short: %d", len(data))
	}

	cm := &dendrite.ChordMsg{Type: dendrite.MsgType(data[0])}

	if data_len > 1 {
		cm.Data = data[1:]
	}

	// parse the data and set the handler
	switch cm.Type {
	case PbDtableGet:
		var dtableGetMsg PBDTableGet
		err := proto.Unmarshal(cm.Data, &dtableGetMsg)
		if err != nil {
			return nil, fmt.Errorf("error decoding PBDTableGet message - %s", err)
		}
		cm.TransportMsg = dtableGetMsg
		cm.TransportHandler = dt.zmq_get_handler
	case PbDtableGetResp:
		var dtableGetRespMsg PBDTableGetResp
		err := proto.Unmarshal(cm.Data, &dtableGetRespMsg)
		if err != nil {
			return nil, fmt.Errorf("error decoding PBDTableGetResp message - %s", err)
		}
		cm.TransportMsg = dtableGetRespMsg
	case PbDtableSet:
		var dtableSetMsg PBDTableSet
		err := proto.Unmarshal(cm.Data, &dtableSetMsg)
		if err != nil {
			return nil, fmt.Errorf("error decoding PBDTableSet message - %s", err)
		}
		cm.TransportMsg = dtableSetMsg
		cm.TransportHandler = dt.zmq_set_handler
	case PbDtableSetResp:
		var dtableSetRespMsg PBDTableSetResp
		err := proto.Unmarshal(cm.Data, &dtableSetRespMsg)
		if err != nil {
			return nil, fmt.Errorf("error decoding PBDTableSetResp message - %s", err)
		}
		cm.TransportMsg = dtableSetRespMsg
	default:
		// must return unknownType error
		var rv dendrite.ErrHookUnknownType = "unknown request type"
		return nil, rv
	}

	return cm, nil
}

// Get() returns value for a given key
func (dt *DTable) get(key []byte) ([]byte, error) {
	succs, err := dt.ring.Lookup(3, key)
	if err != nil {
		return nil, err
	}
	// check if successor exists in local dtable
	vn_key_str := fmt.Sprintf("%x", succs[0].Id)
	vn_table, ok := dt.table[vn_key_str]
	if ok {
		key_str := fmt.Sprintf("%x", key)
		if v, exists := vn_table[key_str]; exists {
			return v.Val, nil
		} else {
			return nil, nil
		}
	}
	// make remote call to all successors
	var last_err error
	for _, succ := range succs {
		val, _, err := dt.remoteGet(succ, key)
		if err != nil {
			last_err = err
			log.Println("ZMQ::remoteGet error - ", err)
			continue
		}
		return val, nil
	}
	return nil, last_err
}

// set() writes to table
// writes: total writes to execute
// wtl: writes to live. 0 means we're done
// skip: how many remote successors to skip in loop
// reports back on done ch
func (dt *DTable) set(key, val []byte, writes, wtl, skip int, done chan error) {
	fmt.Println(writes, wtl, skip)
	if wtl == 0 {
		done <- nil
		return
	}

	succs, err := dt.ring.Lookup(3, key)
	if err != nil {
		done <- err
		return
	}

	if len(succs) == skip {
		// not enough remote successors found
		done <- fmt.Errorf("not enough remote successors found for replicated write")
		return
	}

	// check if successor exists in local dtable
	vn_key_str := fmt.Sprintf("%x", succs[skip].Id)
	vn_table, ok := dt.table[vn_key_str]

	// only do local write if this is first hop
	if ok && writes == wtl {
		key_str := fmt.Sprintf("%x", key)
		if val == nil {
			delete(vn_table, key_str)
		} else {
			vn_table[key_str] = &value{
				Val:       val,
				timestamp: time.Now(),
			}
		}
		wtl--
		skip++
		dt.set(key, val, writes, wtl, skip, done)
		return
	}
	if ok {
		skip++
		dt.set(key, val, writes, wtl, skip, done)
		return
	}

	// make remote call to successor
	for _, succ := range succs[skip:] {
		err = dt.remoteSet(succ, key, val)
		log.Println("Calling remote")
		if err != nil {
			log.Println("ZMQ::remoteSet error - ", err)
			done <- err
			return
		}
		wtl--
		skip++
		dt.set(key, val, writes, wtl, skip, done)
		return
	}
}

// setOnReplicas() finds replicas to write to
// writes: total writes to execute
// wtl: writes to live. 0 means we're done
// skip: how many remote successors to skip in loop
// reports back on done ch
func (dt *DTable) setOnReplicas(key, val []byte, writes, wtl, skip int, done chan error) {
	fmt.Println(writes, wtl, skip)
	if wtl == 0 {
		done <- nil
		return
	}

	succs, err := dt.ring.Lookup(3, key)
	if err != nil {
		done <- err
		return
	}

	if len(succs) == skip {
		// not enough remote successors found
		done <- fmt.Errorf("not enough remote successors found for replicated write")
		return
	}

	// check if successor exists in local dtable
	vn_key_str := fmt.Sprintf("%x", succs[skip].Id)
	vn_table, ok := dt.table[vn_key_str]

	// only do local write if this is first hop
	if ok && writes == wtl {
		key_str := fmt.Sprintf("%x", key)
		if val == nil {
			delete(vn_table, key_str)
		} else {
			vn_table[key_str] = &value{
				Val:       val,
				timestamp: time.Now(),
			}
		}
		wtl--
		skip++
		dt.set(key, val, writes, wtl, skip, done)
		return
	}
	if ok {
		skip++
		dt.set(key, val, writes, wtl, skip, done)
		return
	}

	// make remote call to successor
	for _, succ := range succs[skip:] {
		err = dt.remoteSet(succ, key, val)
		log.Println("Calling remote")
		if err != nil {
			log.Println("ZMQ::remoteSet error - ", err)
			done <- err
			return
		}
		wtl--
		skip++
		dt.set(key, val, writes, wtl, skip, done)
		return
	}
}
func (dt *DTable) DumpStr() {
	fmt.Println("Dumping DTABLE")
	for vn_id, vn_table := range dt.table {
		fmt.Printf("\tvnode: %x\n", vn_id)
		for key, val := range vn_table {
			fmt.Printf("\t\t%s - %s\n", key, val.Val)
		}
	}
}
