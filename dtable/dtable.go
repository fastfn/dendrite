package dtable

import (
	"fmt"
	"github.com/fastfn/dendrite"
	"github.com/golang/protobuf/proto"
	"sync"
	"time"
)

type replicaState int
type dtableEventType int

// KVItem is basic database item struct.
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

type dtableEvent struct {
	evType dtableEventType
	vnode  *dendrite.Vnode
	item   *kvItem
}
type itemMap map[string]*kvItem
type demotedItemMap map[string]*demotedKvItem

// DTable is main dtable struct.
type DTable struct {
	// base structures
	table         map[string]itemMap        // primary k/v table
	rtable        map[string]itemMap        // rtable is table of replicas
	demoted_table map[string]demotedItemMap // demoted items
	ring          *dendrite.Ring
	transport     dendrite.Transport
	confLogLevel  LogLevel
	// communication channels
	event_c     chan *dendrite.EventCtx // dendrite sends events here
	dtable_c    chan *dtableEvent       // internal dtable events
	selfcheck_t *time.Ticker
}

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
	PbDtablePromoteKey        dendrite.MsgType = 0x30 // promote remote vnode for given key

	replicaStable     replicaState = 0 // all replicas commited
	replicaPartial    replicaState = 1 // all available replicas commited but there's no enough remote nodes
	replicaIncomplete replicaState = 2 // some of the replicas did not commit

	evPromoteKey dtableEventType = 0
)

func (m itemMap) put(item *kvItem) error {
	if oldItem, ok := m[item.keyHashString()]; ok {
		if oldItem.timestamp.UnixNano() <= item.timestamp.UnixNano() {
			// key exists but the record is older than new one
			if item.Val == nil {
				delete(m, item.keyHashString())
			} else {
				m[item.keyHashString()] = item
			}
		} else {
			return fmt.Errorf("map.put() refused write for key %s. Record too old: %d > %d",
				item.keyHashString(), oldItem.timestamp.UnixNano(), item.timestamp.UnixNano())
		}
	} else {
		if item.Val != nil {
			m[item.keyHashString()] = item
		} else {
			return fmt.Errorf("map.put() - empty value not allowed")
		}
	}
	return nil
}

// Init initializes dtable and registers with dendrite as a TransportHook and DelegateHook.
func Init(ring *dendrite.Ring, transport dendrite.Transport, level LogLevel) *DTable {
	dt := &DTable{
		table:         make(map[string]itemMap),
		rtable:        make(map[string]itemMap),
		demoted_table: make(map[string]demotedItemMap),
		ring:          ring,
		transport:     transport,
		confLogLevel:  level,
		event_c:       make(chan *dendrite.EventCtx),
		dtable_c:      make(chan *dtableEvent),
		selfcheck_t:   time.NewTicker(10 * time.Minute),
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

// EmitEvent implements dendrite's DelegateHook.
func (dt *DTable) EmitEvent(ctx *dendrite.EventCtx) {
	dt.event_c <- ctx
}

// Decode implements dendrite's TransportHook.
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
			return nil, fmt.Errorf("error decoding PBDTableSetItem message - %s", err)
		}
		cm.TransportMsg = dtableSetItemMsg
		cm.TransportHandler = dt.zmq_set_handler
	case PbDtableSetReplicaInfo:
		var dtableSetReplicaInfoMsg PBDTableSetReplicaInfo
		err := proto.Unmarshal(cm.Data, &dtableSetReplicaInfoMsg)
		if err != nil {
			return nil, fmt.Errorf("error decoding PBDTableSetReplicaInfo message - %s", err)
		}
		cm.TransportMsg = dtableSetReplicaInfoMsg
		cm.TransportHandler = dt.zmq_setReplicaInfo_handler
	case PbDtableClearReplica:
		var dtableClearReplicaMsg PBDTableClearReplica
		err := proto.Unmarshal(cm.Data, &dtableClearReplicaMsg)
		if err != nil {
			return nil, fmt.Errorf("error decoding PBDTableClearReplica message - %s", err)
		}
		cm.TransportMsg = dtableClearReplicaMsg
		cm.TransportHandler = dt.zmq_clearreplica_handler
	case PbDtableSetReplica:
		var dtableSetReplicaMsg PBDTableSetItem
		err := proto.Unmarshal(cm.Data, &dtableSetReplicaMsg)
		if err != nil {
			return nil, fmt.Errorf("error decoding PBDTableSetReplica message - %s", err)
		}
		cm.TransportMsg = dtableSetReplicaMsg
		cm.TransportHandler = dt.zmq_setReplica_handler
	case PbDtablePromoteKey:
		var dtablePromoteKeyMsg PBDTablePromoteKey
		err := proto.Unmarshal(cm.Data, &dtablePromoteKeyMsg)
		if err != nil {
			return nil, fmt.Errorf("error decoding PBDTablePromoteKey message - %s", err)
		}
		cm.TransportMsg = dtablePromoteKeyMsg
		cm.TransportHandler = dt.zmq_promoteKey_handler
	case PbDtableResponse:
		var dtableResponseMsg PBDTableResponse
		err := proto.Unmarshal(cm.Data, &dtableResponseMsg)
		if err != nil {
			return nil, fmt.Errorf("error decoding PBDTableResponse message - %s", err)
		}
		cm.TransportMsg = dtableResponseMsg
	default:
		// must return unknownType error
		//fmt.Printf("GOT UNKNOWN!!!!!!! %x - %x\n", cm.Type, byte(cm.Type))
		var rv dendrite.ErrHookUnknownType = "unknown request type"
		return nil, rv
	}
	return cm, nil
}

// get returns value for a given key
func (dt *DTable) get(reqItem *kvItem) (*kvItem, error) {
	succs, err := dt.ring.Lookup(3, reqItem.keyHash)
	if err != nil {
		return nil, err
	}
	// check if successor exists in local dtable
	vn_table, ok := dt.table[succs[0].String()]
	key_str := reqItem.keyHashString()
	if ok {
		if item, exists := vn_table[key_str]; exists && item.commited {
			return item.dup(), nil
		} else {
			return nil, nil
		}
	} else {
		// check against replica tables
		for _, rtable := range dt.rtable {
			if item, exists := rtable[key_str]; exists && item.replicaInfo.state == replicaIncomplete {
				return item, nil
			}
		}
	}

	// make remote call to all successors
	var last_err error
	for _, succ := range succs {
		respItem, _, err := dt.remoteGet(succ, reqItem)
		if err != nil {
			last_err = err
			dt.Logln(LogDebug, "ZMQ::remoteGet error - ", err)
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
		//log.Println("SetReplica() - value for key", key_str, "is nil, removing item")
		delete(dt.rtable[vnode.String()], key_str)
	} else {
		//log.Println("SetReplica() - success for key", key_str)
		item.commited = true
		dt.rtable[vnode.String()][key_str] = item
	}
}

/* set writes to dtable's primary(non-replica table). It is called from both Query api and
by remote clients via zmq.

It reports back on done chan when minAcks is reached so that clients can continue without
blocking while replication takes place.
*/
func (dt *DTable) set(vn *dendrite.Vnode, item *kvItem, minAcks int, done chan error) {
	// make sure we have local handler before doing any write
	handler, _ := dt.transport.GetVnodeHandler(vn)
	if handler == nil {
		done <- fmt.Errorf("local handler could not be found for vnode %x", vn.Id)
		return
	}
	write_count := 0
	vn_table, _ := dt.table[vn.String()]

	item.lock.Lock()
	defer item.lock.Unlock()

	item.replicaInfo.master = vn
	err := vn_table.put(item)
	if err != nil {
		done <- err
		return
	}

	write_count++
	repwrite_count := 0
	returned := false
	item.replicaInfo.state = replicaIncomplete

	// should we return to client immediately?
	if minAcks == write_count {
		// cover the case where ring.Replicas() returns 0
		if dt.ring.Replicas() == repwrite_count {
			item.replicaInfo.state = replicaStable
			item.commited = true
			done <- nil
			return
		}
		item.commited = true
		done <- nil
		returned = true
	}

	// find remote successors to write replicas to
	remote_succs, err := handler.FindRemoteSuccessors(dt.ring.Replicas())
	if err != nil {
		if !returned {
			done <- fmt.Errorf("could not find replica nodes due to error %s", err)
		}
		dt.Logf(LogDebug, "could not find replica nodes due to error %s\n", err)
		dt.rollback(vn, item)
		return
	}

	// don't write any replica if not enough replica nodes have been found for requested consistency
	if minAcks > len(remote_succs)+1 {
		done <- fmt.Errorf("insufficient nodes found for requested consistency level (%d)\n", minAcks)
		dt.rollback(vn, item)
		return
	}

	// now lets write replicas
	item_replicas := make([]*dendrite.Vnode, 0)
	repl_item := item.dup()
	repl_item.commited = false

	for _, succ := range remote_succs {
		err := dt.remoteWriteReplica(vn, succ, repl_item)
		if err != nil {
			dt.Logf(LogDebug, "could not write replica due to error: %s\n", err)
			continue
		}
		item_replicas = append(item_replicas, succ)
	}

	// check if we have enough written replicas for requested minAcks
	if minAcks > len(item_replicas)+1 {
		done <- fmt.Errorf("insufficient active nodes found for requested consistency level (%d)\n", minAcks)
		dt.rollback(vn, item)
		return
	}

	// update replication state based on available replicas
	var target_state replicaState
	if dt.ring.Replicas() <= len(item_replicas) {
		target_state = replicaStable
	} else {
		target_state = replicaPartial
	}

	// replicas have been written, lets now update metadata
	real_idx := 0
	fail_count := 0
	repl_item.commited = true
	repl_item.replicaInfo.vnodes = item_replicas
	repl_item.replicaInfo.state = target_state
	repl_item.replicaInfo.master = vn

	for _, replica := range item_replicas {
		// update metadata/commit on remote
		repl_item.replicaInfo.depth = real_idx
		err := dt.remoteSetReplicaInfo(replica, repl_item)
		if err != nil {
			fail_count++
			if !returned && len(item_replicas)-fail_count < minAcks {
				done <- fmt.Errorf("insufficient (phase2) active nodes found for requested consistency level (%d)\n", minAcks)
				dt.rollback(vn, item)
				return
			}
			continue
		}
		real_idx++
		repwrite_count++

		// notify client if enough replicas have been written
		if !returned && repwrite_count+1 == minAcks {
			done <- nil
			returned = true
		}
	}
	item.replicaInfo.state = target_state
	item.commited = true
}

// rollback is called on failed set()
func (dt *DTable) rollback(vn *dendrite.Vnode, item *kvItem) {
	if item.replicaInfo != nil {
		for _, replica := range item.replicaInfo.vnodes {
			if replica != nil {
				dt.remoteClearReplica(replica, item, false)
			}
		}
	}
	delete(dt.table[vn.String()], item.keyHashString())
}

// DumpStr dumps dtable keys per vnode on stdout. Mostly used for debugging.
func (dt *DTable) DumpStr() {
	fmt.Println("Dumping DTABLE")
	for vn_id, vn_table := range dt.table {
		fmt.Printf("\tvnode: %s\n", vn_id)
		for key, item := range vn_table {
			fmt.Printf("\t\t%s - %s - %v - commited:%v\n", key, item.Val, item.replicaInfo.state, item.commited)
		}
		rt, _ := dt.rtable[vn_id]
		for key, item := range rt {
			fmt.Printf("\t\t- r%d - %s - %s - %d - commited:%v\n", item.replicaInfo.depth, key, item.Val, item.replicaInfo.state, item.commited)
		}
		for key, item := range dt.demoted_table[vn_id] {
			fmt.Printf("\t\t- d - %s - %s - %v\n", key, item.new_master.String(), item.demoted_ts)
		}
	}
}

// processDemoteKey is called when our successor is demoting key to us.
// We fix replicas for the key and when we're done we make a call to origin
// (old primary for this key) to clear demotedItem there.
func (dt *DTable) processDemoteKey(vnode, origin, old_master *dendrite.Vnode, reqItem *kvItem) {
	// find the key in our primary table
	key_str := reqItem.keyHashString()
	if _, ok := dt.table[vnode.String()][key_str]; ok {
		dt.replicateKey(vnode, reqItem, dt.ring.Replicas())

		// now clear demoted item on origin
		err := dt.remoteClearReplica(origin, reqItem, true)
		if err != nil {
			dt.Logf(LogInfo, "processDemoteKey() - failed while removing demoted key from origin %x for key %s\n", origin.Id, key_str)
		}
	} else {
		dt.Logln(LogInfo, "processDemoteKey failed - key not found:", key_str)
		return
	}
}
