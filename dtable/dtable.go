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

type replicaState int

const (
	PbDtableGet       dendrite.MsgType = 0x20
	PbDtableGetResp   dendrite.MsgType = 0x21
	PbDtableSet       dendrite.MsgType = 0x22
	PbDtableSetResp   dendrite.MsgType = 0x23
	PbDtableSetMulti  dendrite.MsgType = 0x24
	PbDtableSetMeta   dendrite.MsgType = 0x25
	replicaComplete   replicaState     = 0
	replicaIncomplete replicaState     = 1
)

type rvalue struct {
	Val           []byte
	timestamp     time.Time
	depth         int
	state         replicaState
	master        *dendrite.Vnode
	replicaVnodes []*dendrite.Vnode
}

type value struct {
	Val       []byte
	timestamp time.Time
	isReplica bool
	commited  bool
	rstate    replicaState
}

type kvMap map[string]*value
type rkvMap map[string]*rvalue

type DTable struct {
	table     map[string]kvMap
	rtable    map[string]rkvMap // rtable is table of replicas
	ring      *dendrite.Ring
	transport dendrite.Transport
}

func Init(ring *dendrite.Ring, transport dendrite.Transport) *DTable {
	dt := &DTable{
		table:     make(map[string]kvMap),
		rtable:    make(map[string]rkvMap),
		ring:      ring,
		transport: transport,
	}
	// each local vnode needs to be separate key in dtable
	for _, vnode := range ring.MyVnodes() {
		node_kv := make(map[string]*value)
		node_rkv := make(map[string]*rvalue)
		vn_key_str := fmt.Sprintf("%x", vnode.Id)
		dt.table[vn_key_str] = node_kv
		dt.rtable[vn_key_str] = node_rkv
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
	case PbDtableSetMeta:
		var dtableSetMetaMsg PBDTableSetMeta
		err := proto.Unmarshal(cm.Data, &dtableSetMetaMsg)
		if err != nil {
			return nil, fmt.Errorf("error decoding PBDTableSetMeta message - %s - %+v", err, cm.Data)
		}
		cm.TransportMsg = dtableSetMetaMsg
		cm.TransportHandler = dt.zmq_setmeta_handler
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

// handle remote replica requests
func (dt *DTable) setReplica(vnode *dendrite.Vnode, key_str string, rval *rvalue) {
	if rval.Val == nil {
		delete(dt.rtable[vnode.String()], key_str)
	} else {
		dt.rtable[vnode.String()][key_str] = rval
	}
}

// set() writes to table. It is called from both DTable api and by remote clients via zmq
// writes: total writes to execute
// wtl: writes to live. 0 means we're done
// skip: how many remote successors to skip in loop
// reports back on done ch

// handles local write
func (dt *DTable) set(vn *dendrite.Vnode, key []byte, val *value, minAcks int, done chan error) {
	// write to as many "acks" as was requested
	// if acks is lower than number of replicas, send signal to done chan after writing to n writes
	// but continue with writing to the rest of replicas

	// make sure we have local handler before doing any write
	handler, _ := dt.transport.GetVnodeHandler(vn)
	if handler == nil {
		done <- fmt.Errorf("local handler could not be found for vnode %x", vn.Id)
		return
	}
	write_count := 0
	vn_table, _ := dt.table[vn.String()]
	key_str := fmt.Sprintf("%x", key)

	// see if key exists with older timestamp
	if v, ok := vn_table[key_str]; ok {
		if v.timestamp.UnixNano() <= val.timestamp.UnixNano() {
			// key exists but the record is older than new one
			if val.Val == nil {
				delete(vn_table, key_str)
			} else {
				vn_table[key_str] = val
			}
		} else {
			done <- fmt.Errorf("set() refused write for key %s. Record too old: %d > %d",
				key_str, v.timestamp.UnixNano(), val.timestamp.UnixNano())
			return
		}
	} else {
		if val.Val == nil {
			delete(vn_table, key_str)
		} else {
			vn_table[key_str] = val
		}
	}

	write_count += 1
	returned := false
	// should we return to client immediately?
	if minAcks == write_count {
		if dt.ring.Replicas() == write_count {
			val.rstate = replicaComplete
			vn_table[key_str] = val
		}
		localCommit(vn_table, key_str, val)
		done <- nil
		returned = true
	}
	if dt.ring.Replicas() == 0 {
		if !returned {
			done <- nil
		}
		return
	}

	// find remote successors to write replicas to
	remote_succs, err := handler.FindRemoteSuccessors(dt.ring.Replicas())
	if err != nil {
		done <- fmt.Errorf("could not find enough replica nodes due to error %s", err)
		return
	}
	log.Printf("Looking up remote replicas for %x\n", vn.Id)
	for _, rep := range remote_succs {
		log.Printf("\t - %x\n", rep.Id)
	}
	// now lets write replicas
	item_replicas := make([]*dendrite.Vnode, 0)
	repwrite_count := 0

	for _, succ := range remote_succs {
		// let client know we're done if minAcks is reached
		if repwrite_count+1 == minAcks && !returned {
			returned = true
			localCommit(vn_table, key_str, val)
			done <- nil
		}
		nval := &value{
			Val:       val.Val,
			timestamp: val.timestamp,
			rstate:    replicaIncomplete,
			isReplica: true,
			commited:  false,
		}
		done_c := make(chan error)
		go dt.remoteSet(succ, key, nval, minAcks, done_c)
		err = <-done_c
		if err != nil {
			if !returned {
				done <- fmt.Errorf("could not write replica due to error %s", err)
				return
			}
			return
		}
		item_replicas = append(item_replicas, succ)
		repwrite_count += 1
	}

	// replicas have been written, lets now update metadata
	for idx, replica := range item_replicas {
		rval := &rvalue{
			depth:         idx,
			state:         replicaComplete,
			master:        vn,
			replicaVnodes: item_replicas,
		}
		err := dt.remoteSetMeta(replica, key, rval)
		if err != nil {
			break
		}
	}
	if !returned {
		localCommit(vn_table, key_str, val)
		done <- nil
	}
}

func (dt *DTable) DumpStr() {
	fmt.Println("Dumping DTABLE")
	for vn_id, vn_table := range dt.table {
		fmt.Printf("\tvnode: %s\n", vn_id)
		for key, val := range vn_table {
			fmt.Printf("\t\t%s - %s - %v\n", key, val.Val, val.commited)
		}
		rt, _ := dt.rtable[vn_id]
		for key, val := range rt {
			state := "out of sync"
			if val.state == replicaComplete {
				state = "in sync"
			}
			fmt.Printf("\t\t- r - %s - %s - %s\n", key, val.Val, state)
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
	// get the handler for this vnode
	handler, ok := dt.transport.GetVnodeHandler(localVn)
	if !ok {
		// can't do this
		return
	}
	replicas, err := handler.FindRemoteSuccessors(max_replicas)

	//replicas, err := dt.transport.FindSuccessors(localVn, max_replicas, localVn.Id)
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
				done_c := make(chan error)
				go dt.remoteSet(succ, key, val, 0, done_c)
				err := <-done_c
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
				done_c := make(chan error)
				go dt.remoteSet(new_pred, key, val, 0, done_c)
				err := <-done_c
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
				go dt.remoteSet(last_replica, key, nil, 0, done_c)
				err = <-done_c
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

func localCommit(vn_table map[string]*value, key_str string, val *value) {
	if val.Val != nil {
		item, _ := vn_table[key_str]
		item.commited = true
		vn_table[key_str] = item
	}
}
