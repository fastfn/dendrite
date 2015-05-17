package dtable

import (
	//"fmt"
	"github.com/fastfn/dendrite"
	"log"
	"time"
)

// delegator() - captures events from different sources
//               and coordinates data delegation processes accordingly
func (dt *DTable) delegator() {
	for {
		select {
		case event := <-dt.event_c:
			switch event.EvType {
			case dendrite.EvPredecessorLeft:
				log.Printf("delegator() - predecessor left - promoting ourselves %s, status: ", event.Target.String())
				// don't make the call just yet. Need to verify that peer is ready
				if err := dt.checkPeer(event.Target); err != nil {
					go dt.replayEvent(event)
				} else {
					dt.promote(event.Target)
					log.Println("promote() done on", event.Target.String())
				}
			case dendrite.EvPredecessorJoined:
				log.Printf("delegator() - predecessor joined - demoting keys to new predecessor %s, status: ", event.Target.String())
				// don't make the call just yet. Need to verify that peer is ready
				if err := dt.checkPeer(event.PrimaryItem); err != nil {
					// schedule for replay
					go dt.replayEvent(event)
				} else {
					dt.demote(event.Target, event.PrimaryItem)
					log.Println("demoting done on", event.Target.String())
				}
			case dendrite.EvReplicasChanged:
				log.Printf("delegator() - replicas changed on %s, status: ", event.Target.String())
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
					log.Println("changeReplica() done on", event.Target.String())
				}
			}

			// TODO: handle case dendrite.EvPredecessorFailed
		}
	}

}

func (dt *DTable) checkPeer(remote *dendrite.Vnode) error {
	return dt.remoteStatus(remote)
}

// replayEvent() is called when remote node does not have dtable initialized
func (dt *DTable) replayEvent(event *dendrite.EventCtx) {
	log.Println("- replayEvent scheduled")
	time.Sleep(5 * time.Second)
	dt.EmitEvent(event)
}
