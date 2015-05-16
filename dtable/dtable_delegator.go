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
	verified_peers := make(map[string]bool)
	for {
		select {
		case event := <-dt.event_c:
			switch event.EvType {
			case dendrite.EvPredecessorLeft:
				log.Printf("delegator() - predecessor left - promoting ourselves %s, status: ", event.Target.String())
				// don't make the call just yet. Need to verify that peer is ready
				if _, ok := verified_peers[event.Target.String()]; ok {
					log.Printf("OK\n")
					dt.promote(event.Target)
				} else {
					if err := dt.checkPeer(event.Target); err != nil {
						// schedule for replay
						log.Printf("DELAYED\n")
						go dt.replayEvent(event)
					} else {
						log.Printf("OK\n")
						verified_peers[event.Target.String()] = true
						dt.promote(event.Target)
					}
				}
			case dendrite.EvPredecessorJoined:
				log.Printf("delegator() - predecessor joined - demoting keys to new predecessor %s, status: ", event.Target.String())
				// don't make the call just yet. Need to verify that peer is ready
				if _, ok := verified_peers[event.PrimaryItem.String()]; ok {
					log.Printf("OK\n")
					dt.demote(event.Target, event.PrimaryItem)
				} else {
					if err := dt.checkPeer(event.PrimaryItem); err != nil {
						// schedule for replay
						log.Printf("DELAYED\n")
						go dt.replayEvent(event)
					} else {
						log.Printf("OK\n")
						verified_peers[event.PrimaryItem.String()] = true
						dt.demote(event.Target, event.PrimaryItem)
					}
				}
			case dendrite.EvReplicasChanged:
				log.Printf("delegator() - replicas changed on %s\n", event.Target.String())
				dt.changeReplicas(event.Target, event.ItemList)

				//case EvPeerError:

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
