package dendrite

import (
	//"bytes"
	//"fmt"
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
	for _, handler := range transport.table {
		h, _ := transport.getVnodeHandler(handler.vn)
		local_vn := h.(*localVnode)
		for _, vnode := range local_vn.ring.vnodes {
			pblist.Vnodes = append(pblist.Vnodes, &PBProtoVnode{Id: vnode.Id, Host: &vnode.Host})
		}
		break
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

func (transport *ZMQTransport) zmq_find_successors_handler(request *ChordMsg, w chan *ChordMsg) {
	pbMsg := request.TransportMsg.(PBProtoFindSuccessors)
	key := pbMsg.GetKey()
	dest := &Vnode{
		Id:   pbMsg.GetDest().GetId(),
		Host: pbMsg.GetDest().GetHost(),
	}
	// make sure destination vnode exists locally
	local_vn, err := transport.getVnodeHandler(dest)
	if err != nil {
		errorMsg := transport.newErrorMsg("Transport::FindSuccessorsHandler - " + err.Error())
		w <- errorMsg
		return
	}

	succs, forward_vn, err := local_vn.FindSuccessors(key, int(pbMsg.GetLimit()))
	if err != nil {
		errorMsg := transport.newErrorMsg("Transport::FindSuccessorsHandler - " + err.Error())
		w <- errorMsg
		return
	}

	// if forward_vn is not set, return the list
	if forward_vn == nil {
		pblist := new(PBProtoListVnodesResp)
		for _, s := range succs {
			pblist.Vnodes = append(pblist.Vnodes,
				&PBProtoVnode{
					Id:   s.Id,
					Host: proto.String(s.Host),
				})

		}
		pbdata, err := proto.Marshal(pblist)
		if err != nil {
			errorMsg := transport.newErrorMsg("Transport::FindSuccessorsHandler - Failed to marshal listVnodes response - " + err.Error())
			w <- errorMsg
			return
		}
		w <- &ChordMsg{
			Type: pbListVnodesResp,
			Data: pbdata,
		}
		return
	}
	// send forward response
	new_remote := &PBProtoVnode{
		Id:   forward_vn.Id,
		Host: proto.String(forward_vn.Host),
	}
	pbfwd := &PBProtoForward{Vnode: new_remote}
	pbdata, err := proto.Marshal(pbfwd)
	if err != nil {
		errorMsg := transport.newErrorMsg("Transport::FindSuccessorsHandler - Failed to marshal forward response - " + err.Error())
		w <- errorMsg
		return
	}
	w <- &ChordMsg{
		Type: pbForward,
		Data: pbdata,
	}
}

func (transport *ZMQTransport) zmq_leave_handler(request *ChordMsg, w chan *ChordMsg) {

}
func (transport *ZMQTransport) zmq_error_handler(request *ChordMsg, w chan *ChordMsg) {

}
