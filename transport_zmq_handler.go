package dendrite

import (
	"github.com/golang/protobuf/proto"
	"log"
)

func (transport *ZMQTransport) zmq_ping_handler(request *ChordMsg, w chan *ChordMsg) {
	pingMsg := request.TransportMsg.(PBProtoPing)
	log.Println("handling ping", pingMsg.GetVersion())
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

func (transport *ZMQTransport) zmq_listVnodes_handler(request *ChordMsg, w chan *ChordMsg) {

}
func (transport *ZMQTransport) zmq_join_handler(request *ChordMsg, w chan *ChordMsg) {

}
func (transport *ZMQTransport) zmq_leave_handler(request *ChordMsg, w chan *ChordMsg) {

}
