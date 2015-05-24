package dtable

import (
	"bytes"
	"github.com/fastfn/dendrite"
	"time"
)

// promoteKey() -- called when remote wants to promote a key to us
func (dt *DTable) promoteKey(vnode *dendrite.Vnode, reqItem *kvItem) {
	rtable := dt.rtable[vnode.String()]
	vn_table := dt.table[vnode.String()]
	// if we're already primary node for this key, just replicate again because one replica could be deleted
	if _, ok := vn_table[reqItem.keyHashString()]; ok {
		dt.replicateKey(vnode, reqItem, dt.ring.Replicas())
		return
	}
	delete(rtable, reqItem.keyHashString())
	reqItem.lock.Lock()
	vn_table.put(reqItem)
	dt.replicateKey(vnode, reqItem, dt.ring.Replicas())
	reqItem.lock.Unlock()
}

// promote() - called when remote predecessor died or left the ring
// because we're only first REMOTE node from original master
// it doesn't mean that we're the actual successor for all the replicated data with depth 0
// if we are, we promote ourselves
// if not, we must find actual successor for each key, and promote that vnode for each key
func (dt *DTable) promote(vnode *dendrite.Vnode) {
	//log.Printf("Node left me: %X for %X now replicating to:\n", localVn.Id, new_pred.Id)
	rtable := dt.rtable[vnode.String()]
	vn_table := dt.table[vnode.String()]
	for key_str, ritem := range rtable {
		if ritem.replicaInfo.depth != 0 {
			continue
		}
		// check if we're real successor for this key
		succs, err := dt.ring.Lookup(1, ritem.keyHash)
		if err != nil {
			dt.Logf(LogInfo, "Could not promote key, Lookup() failed: %s\n", err.Error())
			continue
		}
		if bytes.Compare(succs[0].Id, vnode.Id) == 0 {
			// this key should be promoted locally
			new_ritem := ritem.dup()
			new_ritem.replicaInfo.vnodes[0] = nil
			new_ritem.commited = true
			new_ritem.lock.Lock()
			vn_table.put(new_ritem)
			dt.Logf(LogDebug, "Promoted local key: %s - running replicator now replicas are %+v \n", key_str, new_ritem.replicaInfo.vnodes)
			delete(rtable, key_str)
			dt.Logf(LogDebug, "Promote calling replicateKey for key %s\n", key_str)
			dt.replicateKey(vnode, new_ritem, dt.ring.Replicas())
			new_ritem.lock.Unlock()
			dt.Logf(LogDebug, "Promote finishing key %s, replicaVnodes are: %+v\n", key_str, new_ritem.replicaInfo.vnodes)
		} else {
			// TODO promote remote vnode
			dt.Logf(LogDebug, "Promoting remote vnode %s for key %s\n", succs[0].String(), key_str)
			delete(rtable, key_str)
			dt.remotePromoteKey(vnode, succs[0], ritem)
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
		for rkey, ritem := range vn_rtable {
			if !ritem.commited {
				continue
			}

			ritem.replicaInfo.vnodes[ritem.replicaInfo.depth] = new_pred
			ritem.lock.Lock()
			dt.rtable[new_pred.String()].put(ritem)
			delete(vn_rtable, rkey)

			// update metadata on all replicas
			new_state := ritem.replicaInfo.state
			for idx, replica := range ritem.replicaInfo.vnodes {
				// skip ourselves
				if idx == ritem.replicaInfo.depth {
					continue
				}
				new_ritem := ritem.dup()
				new_ritem.replicaInfo.depth = idx
				new_ritem.replicaInfo.state = new_state

				err := dt.remoteSetReplicaInfo(replica, new_ritem)
				if err != nil {
					dt.Logf(LogInfo, "Error updating replicaMeta on demote() -", err)
					new_state = replicaIncomplete
					continue
				}
			}
			ritem.lock.Unlock()
		}
	case false:
		// loop over primary table to find keys that should belong to new predecessor
		vn_table := dt.table[vnode.String()]
		for key_str, item := range vn_table {
			if !item.commited {
				continue
			}
			if dendrite.Between(vnode.Id, new_pred.Id, item.keyHash, true) {
				//log.Printf("Analyzed key for demoting %s and pushing to %s\n", key_str, new_pred.String())
				// copy the key to demoted table and remove it from primary one
				dt.demoted_table[vnode.String()][item.keyHashString()] = item.to_demoted(new_pred)
				delete(vn_table, key_str)
				done_c := make(chan error)
				go dt.remoteSet(vnode, new_pred, item, dt.ring.Replicas(), true, done_c)
				err := <-done_c
				if err != nil {
					dt.Logln(LogInfo, "Error demoting key to new predecessor -", err)
					continue
				}
			}
		}
	}

}

// changeReplicas() -- callend when replica set changes
//
func (dt *DTable) changeReplicas(vnode *dendrite.Vnode, new_replicas []*dendrite.Vnode) {
	for _, item := range dt.table[vnode.String()] {
		if !item.commited {
			continue
		}
		item.lock.Lock()
		dt.replicateKey(vnode, item, dt.ring.Replicas())
		item.lock.Unlock()
	}
}

func (dt *DTable) replicateKey(vnode *dendrite.Vnode, reqItem *kvItem, limit int) {
	handler, _ := dt.transport.GetVnodeHandler(vnode)
	if handler == nil {
		return
	}
	// find remote successors to write replicas to
	remote_succs, err := handler.FindRemoteSuccessors(limit)
	if err != nil {
		return
	}

	// first, lets remove existing replicas
	for idx, existing := range reqItem.replicaInfo.vnodes {
		if existing == nil {
			continue
		}
		if err := dt.remoteClearReplica(existing, reqItem, false); err != nil {
			// lets add this replica to orphans
			reqItem.replicaInfo.orphan_vnodes = append(reqItem.replicaInfo.orphan_vnodes, existing)
			reqItem.replicaInfo.vnodes[idx] = nil
			continue
		} else {
			reqItem.replicaInfo.vnodes[idx] = nil
		}
	}

	// we set replicaState to stable if enough remote successors are found
	// otherwise, replicaState is still stable, but partial
	// if any of replica writes fail later on, we'll set the state to Incomplete
	var new_replica_state replicaState
	if len(remote_succs) >= dt.ring.Replicas() {
		new_replica_state = replicaStable
	} else {
		new_replica_state = replicaPartial
	}

	// now lets write replicas
	new_replicas := make([]*dendrite.Vnode, 0)

	for _, succ := range remote_succs {
		if succ == nil {
			continue
		}
		dt.Logf(LogDebug, "replicating to: %x\n", succ.Id)
		new_ritem := reqItem.dup()
		new_ritem.replicaInfo.state = replicaIncomplete
		new_ritem.commited = false

		err := dt.remoteWriteReplica(vnode, succ, new_ritem)
		if err != nil {
			dt.Logf(LogInfo, "Error writing replica to %s for key %s due to error: %s\n", succ.String(), new_ritem.keyHashString(), err.Error())
			new_replica_state = replicaIncomplete
			continue
		}
		new_replicas = append(new_replicas, succ)
	}

	// update metadata on original item
	reqItem.replicaInfo.vnodes = make([]*dendrite.Vnode, limit)
	for idx, new_replica := range new_replicas {
		reqItem.replicaInfo.vnodes[idx] = new_replica
	}
	reqItem.replicaInfo.state = new_replica_state

	// update metadata on successful replicas
	for idx, replica := range reqItem.replicaInfo.vnodes {
		if replica == nil {
			break
		}
		new_ritem := reqItem.dup()
		new_ritem.replicaInfo.depth = idx
		new_ritem.replicaInfo.state = new_replica_state

		err := dt.remoteSetReplicaInfo(replica, new_ritem)
		if err != nil {
			// this should not happen. It means another replica node failed in the meantime
			// need to trigger orphan cleaner, which will restart this process
			reqItem.replicaInfo.state = replicaIncomplete
			reqItem.replicaInfo.vnodes[idx] = nil
			reqItem.replicaInfo.orphan_vnodes = append(reqItem.replicaInfo.orphan_vnodes, replica)
			continue
		}
	}
}

func (dt *DTable) selfCheck() {
	//check for orphaned keys
	for _, vn_table := range dt.table {
	ITEM_LOOP:
		for _, item := range vn_table {
			if len(item.replicaInfo.orphan_vnodes) == 0 {
				continue ITEM_LOOP
			}
			item.lock.Lock()
			new_orphans := make([]*dendrite.Vnode, 0)
			replicate_again := false
			for _, orphan_vnode := range item.replicaInfo.orphan_vnodes {
				if orphan_vnode == nil {
					continue
				}
				// maybe it was fixed already by another process (eg, replicas changed and key was re-replicated)
				fixed := false
				for _, replica := range item.replicaInfo.vnodes {
					if replica == nil {
						continue
					}
					if bytes.Compare(orphan_vnode.Id, replica.Id) == 0 {
						fixed = true
					}
				}
				if !fixed {
					if err := dt.remoteClearReplica(orphan_vnode, item, false); err != nil {
						// attempt to clear orphan'ed item failed
						new_orphans = append(new_orphans, orphan_vnode)
					} else {
						// success, lets re-replicate this key
						replicate_again = true
					}
				}
			}
			item.replicaInfo.orphan_vnodes = new_orphans
			if replicate_again {
				dt.replicateKey(item.replicaInfo.master, item, dt.ring.Replicas())
			}
			item.lock.Unlock()
		}
	}

	//check for demoted keys
	for _, demoted_table := range dt.demoted_table {
		for _, demoted_item := range demoted_table {
			if demoted_item.demoted_ts.Add(time.Minute * 3).Before(time.Now()) {
				// new master did not process this item to the end when we demoted the key
				// lets see if we can lookup the key
				val, err := dt.NewQuery().Get([]byte(demoted_item.item.keyHashString()))
				if err != nil {
					dt.Logf(LogInfo, "selfCheck() tried to check demoted key: %s, but Get() failed: %s\n", demoted_item.item.keyHashString(), err.Error())
					continue
				}
				if val == nil {
					dt.Logf(LogInfo, "selfCheck() found old demoted key: %s. Restoring it now...", demoted_item.item.keyHashString())
					err = dt.NewQuery().Set(demoted_item.item.Key, demoted_item.item.Val)
					if err != nil {
						dt.Logf(LogInfo, "selfCheck() failed while restoring demoted key %s. Err: %s\n", demoted_item.item.keyHashString(), err.Error())
						continue
					}
					delete(demoted_table, demoted_item.item.keyHashString())
					dt.Logf(LogInfo, "selfCheck() restored demoted key: %s\n", demoted_item.item.keyHashString())
				}
			}
		}
	}
}
