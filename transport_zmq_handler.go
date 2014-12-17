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
	pblist := new(PBProtoListVnodesResp)
	for _, vnode := range transport.ring.vnodes {
		pblist.Vnodes = append(pblist.Vnodes, &PBProtoVnode{Id: vnode.Id, Host: &vnode.Host})
	}
	pbdata, err := proto.Marshal(pblist)
	if err != nil {
		errorMsg := transport.newErrorMsg("Failed to marshal listVnodes response - " + err.Error())
		w <- errorMsg
		return
	}
	w <- &ChordMsg{
		Type: pbListVnodesResp,
		Data: pbdata,
	}
	return
}

func (transport *ZMQTransport) zmq_join_handler(request *ChordMsg, w chan *ChordMsg) {

}
func (transport *ZMQTransport) zmq_leave_handler(request *ChordMsg, w chan *ChordMsg) {

}
func (transport *ZMQTransport) zmq_error_handler(request *ChordMsg, w chan *ChordMsg) {

}
