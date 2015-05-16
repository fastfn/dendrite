package dtable

import (
	"bytes"
	"fmt"
	"github.com/fastfn/dendrite"
	"github.com/golang/protobuf/proto"
	"log"
	"sync"
	"time"
)

type replicaState int

const (
	PbDtableStatus            dendrite.MsgType = 0x20 // status request to see if remote dtable is initialized
	PbDtableResponse          dendrite.MsgType = 0x21 // generic response
	PbDtableItem              dendrite.MsgType = 0x22 // single item response
	PbDtableMultiItemResponse dendrite.MsgType = 0x23 // response with multiple items
	PbDtableGetItem           dendrite.MsgType = 0x24 // getItem request
	PbDtableSetItem           dendrite.MsgType = 0x25 // setItem request
	PbDtableSetMultiItem      dendrite.MsgType = 0x26 // setMultiItem request
	PbDtableClearReplica      dendrite.MsgType = 0x27 // clearReplica request
	PbDtableSetReplica        dendrite.MsgType = 0x28 // setReplica request
	PbDtableSetReplicaInfo    dendrite.MsgType = 0x29 // setReplicaInfo request

	replicaStable     replicaState = 0 // all replicas commited
	replicaPartial    replicaState = 1 // all available replicas commited but there's no enough remote nodes
	replicaIncomplete replicaState = 2 // some of the replicas did not commit
)

type KVItem struct {
	Key []byte
	Val []byte
}

type kvReplicaInfo struct {
	master        *dendrite.Vnode
	vnodes        []*dendrite.Vnode
	orphan_vnodes []*dendrite.Vnode
	state         replicaState
	depth         int
}

type kvItem struct {
	KVItem
	timestamp   time.Time
	commited    bool
	keyHash     []byte
	lock        *sync.Mutex
	replicaInfo *kvReplicaInfo
}

type demotedKvItem struct {
	item       *kvItem
	new_master *dendrite.Vnode
	demoted_ts time.Time
}

type itemMap map[string]*kvItem
type demotedItemMap map[string]*demotedKvItem

type DTable struct {
	// base structures
	table         map[string]itemMap
	rtable        map[string]itemMap // rtable is table of replicas
	demoted_table map[string]demotedItemMap
	ring          *dendrite.Ring
	transport     dendrite.Transport
	// communication channels
	event_c chan *dendrite.EventCtx
}

func Init(ring *dendrite.Ring, transport dendrite.Transport) *DTable {
	dt := &DTable{
		table:         make(map[string]itemMap),
		rtable:        make(map[string]itemMap),
		demoted_table: make(map[string]demotedItemMap),
		ring:          ring,
		transport:     transport,
		event_c:       make(chan *dendrite.EventCtx),
	}
	// each local vnode needs to be separate key in dtable
	for _, vnode := range ring.MyVnodes() {
		node_kv := make(map[string]*kvItem)
		node_rkv := make(map[string]*kvItem)
		node_demoted := make(map[string]*demotedKvItem)
		vn_key_str := fmt.Sprintf("%x", vnode.Id)
		dt.table[vn_key_str] = node_kv
		dt.rtable[vn_key_str] = node_rkv
		dt.demoted_table[vn_key_str] = node_demoted
	}
	transport.RegisterHook(dt)
	go dt.delegator()
	ring.RegisterDelegateHook(dt)
	return dt
}

func (dt *DTable) EmitEvent(ctx *dendrite.EventCtx) {
	dt.event_c <- ctx
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
	case PbDtableStatus:
		var dtableStatusMsg PBDTableStatus
		err := proto.Unmarshal(cm.Data, &dtableStatusMsg)
		if err != nil {
			return nil, fmt.Errorf("error decoding PBDTableStatus message - %s", err)
		}
		cm.TransportMsg = dtableStatusMsg
		cm.TransportHandler = dt.zmq_status_handler
	case PbDtableGetItem:
		var dtableGetItemMsg PBDTableGetItem
		err := proto.Unmarshal(cm.Data, &dtableGetItemMsg)
		if err != nil {
			return nil, fmt.Errorf("error decoding PBDTableGetItem message - %s", err)
		}
		cm.TransportMsg = dtableGetItemMsg
		cm.TransportHandler = dt.zmq_get_handler
	case PbDtableItem:
		var dtableItemMsg PBDTableItem
		err := proto.Unmarshal(cm.Data, &dtableItemMsg)
		if err != nil {
			return nil, fmt.Errorf("error decoding PBDTableItem message - %s", err)
		}
		cm.TransportMsg = dtableItemMsg
	case PbDtableSetItem:
		var dtableSetItemMsg PBDTableSetItem
		err := proto.Unmarshal(cm.Data, &dtableSetItemMsg)
		if err != nil {
			return nil, fmt.Errorf("error decoding PBDTableSetItem message - %s - %+v", err, cm.Data)
		}
		cm.TransportMsg = dtableSetItemMsg
		cm.TransportHandler = dt.zmq_set_handler
	case PbDtableSetReplicaInfo:
		var dtableSetReplicaInfoMsg PBDTableSetReplicaInfo
		err := proto.Unmarshal(cm.Data, &dtableSetReplicaInfoMsg)
		if err != nil {
			return nil, fmt.Errorf("error decoding PBDTableSetReplicaInfo message - %s - %+v", err, cm.Data)
		}
		cm.TransportMsg = dtableSetReplicaInfoMsg
		cm.TransportHandler = dt.zmq_setReplicaInfo_handler
	case PbDtableClearReplica:
		var dtableClearReplicaMsg PBDTableClearReplica
		err := proto.Unmarshal(cm.Data, &dtableClearReplicaMsg)
		if err != nil {
			return nil, fmt.Errorf("error decoding PBDTableClearReplica message - %s - %+v", err, cm.Data)
		}
		cm.TransportMsg = dtableClearReplicaMsg
		cm.TransportHandler = dt.zmq_clearreplica_handler
	case PbDtableSetReplica:
		var dtableSetReplicaMsg PBDTableSetItem
		err := proto.Unmarshal(cm.Data, &dtableSetReplicaMsg)
		if err != nil {
			return nil, fmt.Errorf("error decoding PBDTableSetReplica message - %s - %+v", err, cm.Data)
		}
		cm.TransportMsg = dtableSetReplicaMsg
		cm.TransportHandler = dt.zmq_setReplica_handler
	case PbDtableResponse:
		var dtableResponseMsg PBDTableResponse
		err := proto.Unmarshal(cm.Data, &dtableResponseMsg)
		if err != nil {
			return nil, fmt.Errorf("error decoding PBDTableResponse message - %s", err)
		}
		cm.TransportMsg = dtableResponseMsg
	default:
		// must return unknownType error
		fmt.Printf("GOT UNKNOWN!!!!!!! %x - %x\n", cm.Type, byte(cm.Type))
		var rv dendrite.ErrHookUnknownType = "unknown request type"
		return nil, rv
	}

	return cm, nil
}

// Get() returns value for a given key
func (dt *DTable) get(reqItem *kvItem) (*kvItem, error) {
	succs, err := dt.ring.Lookup(3, reqItem.keyHash)
	if err != nil {
		return nil, err
	}
	// check if successor exists in local dtable
	vn_key_str := fmt.Sprintf("%x", succs[0].Id)
	vn_table, ok := dt.table[vn_key_str]
	if ok {
		key_str := reqItem.keyHashString()
		if item, exists := vn_table[key_str]; exists {
			return item.dup(), nil
		} else {
			return nil, fmt.Errorf("not found")
		}
	}
	// make remote call to all successors
	var last_err error
	for _, succ := range succs {
		respItem, _, err := dt.remoteGet(succ, reqItem)
		if err != nil {
			last_err = err
			log.Println("ZMQ::remoteGet error - ", err)
			continue
		}
		return respItem, nil
	}
	return nil, last_err
}

// handle remote replica requests
func (dt *DTable) setReplica(vnode *dendrite.Vnode, item *kvItem) {
	key_str := item.keyHashString()
	if item.Val == nil {
		delete(dt.rtable[vnode.String()], key_str)
	} else {
		dt.rtable[vnode.String()][key_str] = item
	}
}

// set() writes to table. It is called from both DTable api and by remote clients via zmq
// writes: total writes to execute
// reports back on done ch when ready
// handles local write
func (dt *DTable) set(vn *dendrite.Vnode, item *kvItem, minAcks int, done chan error) {
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
	key_str := item.keyHashString()

	//item.lock.Lock()
	//defer item.lock.Unlock()

	// see if key exists with older timestamp
	if oldItem, ok := vn_table[key_str]; ok {
		if oldItem.timestamp.UnixNano() <= item.timestamp.UnixNano() {
			// key exists but the record is older than new one
			if item.Val == nil {
				delete(vn_table, key_str)
			} else {
				item.replicaInfo.master = vn
				vn_table[key_str] = item
			}
		} else {
			done <- fmt.Errorf("set() refused write for key %s. Record too old: %d > %d",
				key_str, oldItem.timestamp.UnixNano(), item.timestamp.UnixNano())
			return
		}
	} else {
		if item.Val != nil {
			item.replicaInfo.master = vn
			vn_table[key_str] = item
		}
	}

	write_count += 1
	returned := false

	// should we return to client immediately?
	if minAcks == write_count {
		if dt.ring.Replicas() == write_count {
			item.replicaInfo.state = replicaStable
			item.commited = true
		}
		log.Printf("Returning set to user because %d == %d\n", minAcks, write_count)
		done <- nil
		returned = true
	}
	if dt.ring.Replicas() == 0 {
		item.replicaInfo.state = replicaStable
		item.commited = true
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
			item.commited = true
			log.Printf("Returning set to user because %d == %d == %d\n", repwrite_count+1, minAcks, write_count)
			done <- nil
		}
		newItem := item.dup()
		newItem.replicaInfo.state = replicaIncomplete
		newItem.commited = false

		done_c := make(chan error)
		go dt.remoteSet(vn, succ, newItem, minAcks, false, done_c)
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
	replication_success := true
	repl_item := item.dup()
	repl_item.commited = true
	repl_item.replicaInfo = new(kvReplicaInfo)
	repl_item.replicaInfo.state = replicaStable
	repl_item.replicaInfo.vnodes = item_replicas
	repl_item.replicaInfo.master = vn

	for idx, replica := range item_replicas {
		repl_item.replicaInfo.depth = idx
		err := dt.remoteSetReplicaInfo(replica, repl_item)
		if err != nil {
			replication_success = false
			break
		}
	}
	if replication_success {
		item.replicaInfo.vnodes = item_replicas
		item.commited = true
	}
	if !returned {
		item.commited = true
		done <- nil
	}
}

func (dt *DTable) DumpStr() {
	fmt.Println("Dumping DTABLE")
	for vn_id, vn_table := range dt.table {
		fmt.Printf("\tvnode: %s\n", vn_id)
		for key, item := range vn_table {
			fmt.Printf("\t\t%s - %s - %v\n", key, item.Val, item.commited)
		}
		rt, _ := dt.rtable[vn_id]
		for key, item := range rt {
			state := "out of sync"
			if item.replicaInfo.state == replicaStable {
				state = "in sync"
			}
			fmt.Printf("\t\t- r%d - %s - %s - %s\n", item.replicaInfo.depth, key, item.Val, state)
		}
	}
}

/*
func localCommit(vn_table map[string]*value, key_str string, val *value) {
	if val.Val != nil {
		item, _ := vn_table[key_str]
		item.commited = true
		vn_table[key_str] = item
	}
}
*/
// processDemoteKey() is called when our successor is demoting key to us
// AFTER we took over the key and built new replicas
// here we clear old replicas if necessary
// at the end, we make a call to origin (old primary for this key) to clear demotedItem there
func (dt *DTable) processDemoteKey(vnode, origin *dendrite.Vnode, reqItem, origItem *kvItem) {
	// find the key in our primary table
	key_str := reqItem.keyHashString()
	if item, ok := dt.table[vnode.String()][key_str]; ok {
		// compare old replicas to active replicas
		// replica depth is already updated across replicas when we wrote key to primary table
		// if oldReplica.Id is not listed in active replicas - we need to remove that replica
	OLD:
		for _, oldReplica := range origItem.replicaInfo.vnodes {
			for _, replica := range item.replicaInfo.vnodes {
				if bytes.Compare(replica.Id, oldReplica.Id) == 0 {
					continue OLD
				}
			}
			// lets remove it
			err := dt.remoteClearReplica(oldReplica, reqItem, false)
			if err != nil {
				log.Printf("processDemoteKey() - failed while removing old replica on %x for key %s\n", oldReplica.Id, key_str)
				continue
			}
		}
		// now clear demoted item on origin
		err := dt.remoteClearReplica(origin, reqItem, true)
		if err != nil {
			log.Printf("processDemoteKey() - failed while removing demoted key from origin %x for key %s\n", origin.Id, key_str)
		}
	} else {
		log.Println("processDemoteKey failed - key not found:", key_str)
		return
	}
}
