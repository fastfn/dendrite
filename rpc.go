package dendrite

import (
	"github.com/golang/protobuf/proto"
	"log"
)

func rpc_handle_ping(request *ChordMsg, w chan *ChordMsg) {
	log.Println("handling ping")
	pbPongMsg := &PBProtoPing{
		Version: proto.Int64(1),
	}
	pbPong, _ := proto.Marshal(pbPongMsg)
	pong := &ChordMsg{
		Type: pbPing,
		Data: pbPong,
	}
	w <- pong
}
