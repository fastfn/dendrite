package dtable

import (
	//"fmt"
	"github.com/fastfn/dendrite"
	"time"
)

// delegator() - captures dendrite events as well as internal dtable events
//               and synchronizes data operations
func (dt *DTable) delegator() {
	for {
		select {
		case event := <-dt.event_c:
			switch event.EvType {
			case dendrite.EvPredecessorLeft:
				dt.Logf(LogDebug, "delegator() - predecessor left - promoting ourselves %s, status: ", event.Target.String())
				// don't make the call just yet. Need to verify that peer is ready
				if err := dt.checkPeer(event.Target); err != nil {
					go dt.replayEvent(event)
				} else {
					dt.promote(event.Target)
					dt.Logln(LogDebug, "promote() done on", event.Target.String())
				}
			case dendrite.EvPredecessorJoined:
				dt.Logf(LogDebug, "delegator() - predecessor joined - demoting keys to new predecessor %s, status: ", event.Target.String())
				// don't make the call just yet. Need to verify that peer is ready
				if err := dt.checkPeer(event.PrimaryItem); err != nil {
					// schedule for replay
					go dt.replayEvent(event)
				} else {
					dt.demote(event.Target, event.PrimaryItem)
					dt.Logln(LogDebug, "demoting done on", event.Target.String())
				}
			case dendrite.EvReplicasChanged:
				dt.Logf(LogDebug, "delegator() - replicas changed on %s, status: ", event.Target.String())
				safe := true
				for _, remote := range event.ItemList {
					if remote == nil {
						continue
					}
					if err := dt.checkPeer(remote); err != nil {
						safe = false
						break
					}
				}
				if !safe {
					go dt.replayEvent(event)
				} else {
					dt.changeReplicas(event.Target, event.ItemList)
					dt.Logln(LogDebug, "changeReplica() done on", event.Target.String())
				}
			}
		case event := <-dt.dtable_c:
			// internal event received
			switch event.evType {
			case evPromoteKey:
				dt.Logf(LogDebug, "delegator() - promotekey() event - on %s, for key %s", event.vnode.String(), event.item.keyHashString())
				dt.promoteKey(event.vnode, event.item)
			}
		}
	}

}

func (dt *DTable) checkPeer(remote *dendrite.Vnode) error {
	return dt.remoteStatus(remote)
}

// replayEvent() is called when remote node does not have dtable initialized
func (dt *DTable) replayEvent(event *dendrite.EventCtx) {
	dt.Logln(LogDebug, "- replayEvent scheduled")
	time.Sleep(5 * time.Second)
	dt.EmitEvent(event)
}
