package dtable

import (
	//"fmt"
	"github.com/fastfn/dendrite"
	"log"
)

// delegator() - captures events from different sources
//               and coordinates data delegation processes accordingly
func (dt *DTable) delegator() {
	for {
		select {
		case event := <-dt.event_c:
			switch event.EvType {
			case dendrite.EvPredecessorLeft:
				/*
					if predecessor leaves:
						- promote new predecessor vnode with replica-0 records

				*/
				log.Printf("delegator() - predecessor left")
				dt.promote(event.Target)
			case dendrite.EvPredecessorJoined:
				log.Printf("delegator() - predecessor joined")
				dt.demote(event.Target, event.PrimaryItem)
			}
			// TODO: handle case dendrite.EvPredecessorFailed
		}
	}

}
