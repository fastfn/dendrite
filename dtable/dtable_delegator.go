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
				log.Printf("delegator() - predecessor left - promoting ourselves %s\n", event.Target.String())
				dt.promote(event.Target)
			case dendrite.EvPredecessorJoined:
				log.Printf("delegator() - predecessor joined - demoting keys to new predecessor %s\n", event.Target.String())
				dt.demote(event.Target, event.PrimaryItem)
			}
			// TODO: handle case dendrite.EvPredecessorFailed
		}
	}

}
