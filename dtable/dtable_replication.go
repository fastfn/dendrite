package dtable

import (
	"bytes"
	"github.com/fastfn/dendrite"
	"log"
	"sync"
	"time"
)

// if node left, maintain consistency by finding local replicas and push them one step further if possible
// if node joined, find all keys in local tables that are < new_pred, copy them to new_pred and strip last replica for them
//                 for other keys, just copy them to all replicas as we might be in deficite
func (dt *DTable) Delegate(localVn, new_pred *dendrite.Vnode, event dendrite.RingEventType, mux sync.Mutex) {
	// get the handler for this vnode
	_, ok := dt.transport.GetVnodeHandler(localVn)
	if !ok {
		// can't do this
		return
	}

	//vn_table, _ := dt.table[localVn.String()]

	switch event {
	case dendrite.EvPredecessorLeft:
		dt.promote(localVn)
	case dendrite.EvPredecessorJoined:
		dt.demote(localVn, new_pred)
		/*
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
		*/
	default:
		return
	}
}

/*
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
*/
// promote() - called when remote predecessor died or left the ring
// because we're only first REMOTE node from original master
// it doesn't mean that we're the actual successor for all the replicated data with depth 0
// if we are, we promote ourselves
// if not, we must find actual successor for each key, and promote that vnode for each key
func (dt *DTable) promote(vnode *dendrite.Vnode) {
	//log.Printf("Node left me: %X for %X now replicating to:\n", localVn.Id, new_pred.Id)
	rtable := dt.rtable[vnode.String()]
	vn_table := dt.table[vnode.String()]
	for key_str, rval := range rtable {
		if rval.depth != 0 {
			continue
		}
		// check if we're real successor for this key
		succs, err := dt.ring.Lookup(1, dendrite.KeyFromString(key_str))
		if err != nil {
			log.Printf("Could not promote key, Lookup() failed: %s\n", err.Error())
			continue
		}
		if bytes.Compare(succs[0].Id, vnode.Id) == 0 {
			// this key should be promoted locally
			new_val := rvalue2value(rval)
			vn_table[key_str] = new_val
			new_val.commited = true
			log.Printf("Promoted local key: %s - running replicator now", key_str)
			dt.replicateKey(vnode, dendrite.KeyFromString(key_str), new_val, dt.ring.Replicas())
			delete(rtable, key_str)
		} else {
			// promote remote vnode
			delete(rtable, key_str)
		}
	}
}

/* demote() - promotes new predecessor with keys from primary table
if new predecessor is local:
	- move all of my replica keys to new vnode
	- replica scheme of remote successors doesn't change here
	  we just need to update metadata on all replica nodes to reflect this change
if new predecessor is remote:
  - for all keys in primary table, that are <= new_pred.Id:
  	1. move key to demoted table and wait there for cleanup call from new master
  	2. call demoteKey() to commit to new_pred's primary table + let that vnode know where existing replicas are
  	3. demoteKey() will callback to cleanup each key from demoted table after it's written new replicas
  - handle replica-0 table such that:
  	1. for each key, check if master vnode is located on same physical node as new_pred
  	- if it is, we don't need to do anything because we're still natural remote successor
  	- if not
  		1. call demoteReplica() to let master know existing replica setup and about newRemoteSucc
  		2. master will reconfigure replicas around and delete unnecessary copies (if any)
*/
func (dt *DTable) demote(vnode, new_pred *dendrite.Vnode) {
	// determine if new_pred is on this node
	isLocal := false
	for _, lvn := range dt.ring.MyVnodes() {
		if lvn.Host == new_pred.Host {
			isLocal = true
		}
	}
	switch isLocal {
	case true:
		// move all replica keys to new vnode
		vn_rtable := dt.rtable[vnode.String()]
		for rkey, rval := range vn_rtable {
			rval.replicaVnodes[rval.depth] = new_pred
			dt.rtable[new_pred.String()][rkey] = rval
			delete(vn_rtable, rkey)

			// update metadata on all replicas
			inSync := replicaComplete
			for idx, replica := range rval.replicaVnodes {
				// skip ourselves
				if idx == rval.depth {
					continue
				}
				meta_rval := &rvalue{
					depth:         idx,
					state:         inSync,
					master:        rval.master,
					replicaVnodes: rval.replicaVnodes,
				}
				err := dt.remoteSetMeta(replica, dendrite.KeyFromString(rkey), meta_rval)
				if err != nil {
					log.Println("Error updating replicaMeta on demote() -", err)
					inSync = replicaIncomplete
					continue
				}
			}
		}
	case false:
		// loop over primary table to find keys that should belong to new predecessor
		vn_table := dt.table[vnode.String()]
		for key_str, val := range vn_table {
			key := dendrite.KeyFromString(key_str)
			if dendrite.Between(vnode.Id, new_pred.Id, key, true) {
				// copy the key to demoted table and remove it from primary one
				dt.demoted_table[vnode.String()][key_str] = value2demotedItem(val, new_pred)
				delete(vn_table, key_str)
				done_c := make(chan error)
				go dt.remoteSet(vnode, new_pred, key, val, dt.ring.Replicas(), true, done_c)
				err := <-done_c
				if err != nil {
					log.Println("Error demoting key to new predecessor -", err)
					continue
				}
			}
		}
	}

}

// changeReplicas() -- callend when replica set changes
//
func (dt *DTable) changeReplicas(vnode *dendrite.Vnode, new_replicas []*dendrite.Vnode) {
	// loop over primary table and get replicaVnodes
	// loop over replicaVnodes and compare with new_replica at that position
	//  - if new_replica at that position is nil or out of range- remove existing replica
	//  - if new_replica at that position is different from existing replica - remove existing and write new
	//  - if there are more new_replicas than existing ones, continue writing to them
	vn_table := dt.table[vnode.String()]
	new_replica_len := len(new_replicas)

KEY_LOOP:
	for key_str, val := range vn_table {
		success_replicas := make([]*dendrite.Vnode, 0)
		last_new_replica := 0

	REPL_LOOP:
		for idx, existing := range val.replicaVnodes {
			// check if new_replica is out of range on this position
			if new_replica_len-1 < idx {
				// remove existing replica
				if err := dt.remoteClearReplica(existing, dendrite.KeyFromString(key_str), false); err != nil {
					log.Printf("changeReplicas() - (1) error removing existing replica %s for key %s", existing.String(), key_str)
				} else {
					val.replicaVnodes[idx] = nil
				}
				continue REPL_LOOP
			}
			if new_replicas[idx] == nil {
				// remove existing replica
				if err := dt.remoteClearReplica(existing, dendrite.KeyFromString(key_str), false); err != nil {
					log.Printf("changeReplicas() - (2) error removing existing replica %s for key %s", existing.String(), key_str)
				} else {
					val.replicaVnodes[idx] = nil
				}
				continue REPL_LOOP
			}
			if new_replicas[idx].String() != existing.String() {
				// remove existing replica
				if err := dt.remoteClearReplica(existing, dendrite.KeyFromString(key_str), false); err != nil {
					log.Printf("changeReplicas() - (3) error removing existing replica %s for key %s", existing.String(), key_str)
				}
				// write new one
				rval := &rvalue{
					Val:       val.Val,
					clean_key: val.clean_key,
					timestamp: val.timestamp,
					depth:     -1,
					state:     replicaIncomplete,
					master:    vnode,
				}
				if err := dt.remoteWriteReplica(new_replicas[idx], dendrite.KeyFromString(key_str), rval); err != nil {
					log.Printf("changeReplicas() - error writing new replica %s for key %s", new_replicas[idx].String(), key_str)
					continue REPL_LOOP
				} else {
					success_replicas = append(success_replicas, new_replicas[idx])
					last_new_replica += 1
					continue REPL_LOOP
				}
			} else {
				// we don't need to do anything here, we'll just update metadata later on
				success_replicas = append(success_replicas)
				last_new_replica += 1
				continue REPL_LOOP
			}
		}
	}

}

func (dt *DTable) replicateKey(vnode *dendrite.Vnode, key []byte, val *value, limit int) {
	handler, _ := dt.transport.GetVnodeHandler(vnode)
	if handler == nil {
		return
	}
	// find remote successors to write replicas to
	remote_succs, err := handler.FindRemoteSuccessors(limit)
	if err != nil {
		return
	}
	// now lets write replicas
	item_replicas := make([]*dendrite.Vnode, 0)

	for _, succ := range remote_succs {
		log.Printf("replicating to: %x\n", succ.Id)
		nval := &value{
			Val:       val.Val,
			timestamp: val.timestamp,
			rstate:    replicaIncomplete,
			isReplica: true,
			commited:  false,
		}
		done_c := make(chan error)
		go dt.remoteSet(vnode, succ, key, nval, 1, false, done_c)
		err = <-done_c
		if err != nil {
			return
		}
		item_replicas = append(item_replicas, succ)
	}

	// replicas have been written, lets now update metadata
	for idx, replica := range item_replicas {
		rval := &rvalue{
			depth:         idx,
			state:         replicaComplete,
			master:        vnode,
			replicaVnodes: item_replicas,
		}
		err := dt.remoteSetMeta(replica, key, rval)
		if err != nil {
			break
		}
	}
}

func rvalue2value(rval *rvalue) *value {
	data := make([]byte, len(rval.Val))
	copy(data, rval.Val)
	return &value{
		Val:       data,
		timestamp: rval.timestamp,
		isReplica: false,
		commited:  false,
		rstate:    replicaIncomplete,
	}
}

func value2demotedItem(val *value, new_master *dendrite.Vnode) *demotedItem {
	data := make([]byte, len(val.Val))
	copy(data, val.Val)
	return &demotedItem{
		val:           data,
		clean_key:     val.clean_key,
		timestamp:     val.timestamp,
		new_master:    new_master,
		replicaVnodes: val.replicaVnodes,
		demoted_ts:    time.Now(),
	}
}

//
