package dtable

import (
	"encoding/hex"
	"fmt"
	"github.com/fastfn/dendrite"
	"github.com/golang/protobuf/proto"
	"log"
	"sync"
	"time"
)

const (
	PbDtableGet      dendrite.MsgType = 20
	PbDtableGetResp  dendrite.MsgType = 21
	PbDtableSet      dendrite.MsgType = 22
	PbDtableSetResp  dendrite.MsgType = 23
	PbDtableSetMulti dendrite.MsgType = 24
)

type value struct {
	Val       []byte
	timestamp time.Time
	replicas  int
}
type kvMap map[string]*value

type DTable struct {
	table     map[string]kvMap
	ring      *dendrite.Ring
	transport dendrite.Transport
}

func Init(ring *dendrite.Ring, transport dendrite.Transport) *DTable {
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
	ring.RegisterDelegateHook(dt)
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
			return nil, fmt.Errorf("error decoding PBDTableSet message - %s - %+v", err, cm.Data)
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
		fmt.Printf("GOT UNKNOWN!!!!!!! %x - %x\n", cm.Type, byte(cm.Type))
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
		key_str := fmt.Sprintf("%x", dendrite.HashKey(key))
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
	succs, err := dt.ring.Lookup(dt.ring.Replicas(), key)
	if err != nil {
		done <- err
		return
	}

	// write to as many "writes" was requested
	// if writes is lower than number of replicas, send signal to done chan after writing to n writes
	// but continue with writing to the rest of replicas
	last_idx := 0
	key_str := fmt.Sprintf("%x", dendrite.HashKey(key))
	for i := 0; i < writes; i++ {
		if succs[i] == nil {
			done <- fmt.Errorf("not enough successors found for replicated write")
			// TODO: cleanup previously written keys if necessary (i>0 at this point)
			return
		}
		succ_key_str := fmt.Sprintf("%x", succs[i].Id)
		vn_table, ok := dt.table[succ_key_str]
		if ok {
			if val == nil {
				delete(vn_table, key_str)
			} else {
				vn_table[key_str] = &value{
					Val:       val,
					timestamp: time.Now(),
					replicas:  writes,
				}
			}
		} else {
			err = dt.remoteSet(succs[i], dendrite.HashKey(key), val)
			if err != nil {
				log.Println("ZMQ::remoteSet error - ", err)
				done <- err
				return
			}
		}
		last_idx += 1
	}
	// we're done as far as client should know
	done <- nil

	// write to the rest of replicas if necessary
	max_replicas := dendrite.Min(dt.ring.Replicas(), len(succs))
	for i := last_idx; i < max_replicas; i++ {
		if succs[i] == nil {
			// there are not enough replicas to complete the write
			return
		}
		succ_key_str := fmt.Sprintf("%x", succs[i].Id)
		vn_table, ok := dt.table[succ_key_str]
		if ok {
			if val == nil {
				delete(vn_table, key_str)
			} else {
				vn_table[key_str] = &value{
					Val:       val,
					timestamp: time.Now(),
					replicas:  writes,
				}
			}
		} else {
			err = dt.remoteSet(succs[i], dendrite.HashKey(key), val)
			if err != nil {
				log.Println("ZMQ::remoteSet error - ", err)
				return
			}
		}
	}
}

func (dt *DTable) set2(key, val []byte, writes, wtl, skip int, done chan error) {
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
		key_str := fmt.Sprintf("%x", dendrite.HashKey(key))
		if val == nil {
			delete(vn_table, key_str)
		} else {
			vn_table[key_str] = &value{
				Val:       val,
				timestamp: time.Now(),
				replicas:  writes,
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
		err = dt.remoteSet(succ, dendrite.HashKey(key), val)
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
		fmt.Printf("\tvnode: %s\n", vn_id)
		for key, val := range vn_table {
			fmt.Printf("\t\t%s - %s\n", key, val.Val)
		}
	}
}

// if node left, maintain consistency by finding local replicas and push them one step further if possible
// if node joined, find all keys in local tables that are < new_pred, copy them to new_pred and strip last replica for them
//                 for other keys, just copy them to all replicas as we might be in deficite
func (dt *DTable) Delegate(localVn, new_pred *dendrite.Vnode, changeType dendrite.RingEventType, mux sync.Mutex) {

	time.Sleep(dt.ring.MaxStabilize())
	mux.Lock()
	defer mux.Unlock()
	// find my successors
	max_replicas := dt.ring.Replicas()
	replicas, err := dt.transport.FindSuccessors(localVn, max_replicas, localVn.Id)
	if err != nil {
		log.Println("DTable::Delegate - error while finding replicas:", err)
		return
	}

	vn_table, _ := dt.table[localVn.String()]
	var last_replica *dendrite.Vnode

	switch changeType {
	case dendrite.EvNodeLeft:
		log.Printf("Node left me: %X for %X now replicating to:\n", localVn.Id, new_pred.Id)
		for _, r := range replicas {
			log.Printf("\t - %X\n", r.Id)
		}
		if len(replicas) == 0 {
			return
		}
		for _, succ := range replicas {
			for key_str, val := range vn_table {
				key, _ := hex.DecodeString(key_str)
				err := dt.remoteSet(succ, key, val.Val)
				if err != nil {
					log.Println("Dendrite::Delegate - failed to replicate key:", err)
				}
			}
		}

	case dendrite.EvNodeJoined:
		log.Printf("Node joined me: %X  ... %X replicating to:\n", localVn.Id, new_pred.Id)
		for _, r := range replicas {
			log.Printf("\t - %X\n", r.Id)
		}
		// find all local keys that are < new predecessor
		for key_str, val := range vn_table {
			key, _ := hex.DecodeString(key_str)
			if dendrite.Between(key, localVn.Id, new_pred.Id, true) {
				// copy the key to new predecessor
				err := dt.remoteSet(new_pred, key, val.Val)
				if err != nil {
					log.Println("Dendrite::Delegate -- failed to delegate key to new predecessor:", err)
					continue
				}
				// remove the key from last replica unless last replica is our new predecessor
				if len(replicas) == 0 {
					continue
				}
				last_replica = replicas[len(replicas)-1]
				if last_replica.Host == new_pred.Host {
					continue
				}
				err = dt.remoteSet(last_replica, key, nil)
				if err != nil {
					log.Println("Dendrite::Delegate - failed to strip key from last replica:", err)
				}
			} else {
				//for _, replica := range replicas {
				// err := dt.remoteSet(replica, key, val.Val)
				// if err != nil {
				// 	log.Println("Dendrite::Delegate -- failed to propagate key to replica:", err)
				// }
				//}
			}
		}
	default:
		return
	}
}
