package dendrite

import (
	"bytes"
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

func (transport *ZMQTransport) zmq_find_successors_handler(request *ChordMsg, w chan *ChordMsg) {
	pbMsg := request.TransportMsg.(PBProtoFindSuccessors)
	key := pbMsg.GetKey()
	dest := &Vnode{
		Id:   pbMsg.GetDest().GetId(),
		Host: pbMsg.GetDest().GetHost(),
	}
	// make sure destination vnode exists locally
	var local_vn *localVnode
	for _, vn := range transport.ring.vnodes {
		if bytes.Equal(vn.Id, dest.Id) {
			local_vn = vn
		}
	}
	if local_vn == nil {
		errorMsg := transport.newErrorMsg("Transport::FindSuccessorsHandler - failed to find local destination vnode")
		w <- errorMsg
		return
	}
	// we're good. Now check if we have direct successor for requested key
	if between(local_vn.Id, local_vn.successors[0].Id, key, true) {
		pblist := new(PBProtoListVnodesResp)
		max_vnodes := min(int(pbMsg.GetLimit()), len(local_vn.successors))
		for i := 0; i < max_vnodes; i++ {
			if local_vn.successors[i] == nil {
				continue
			}
			pblist.Vnodes = append(pblist.Vnodes,
				&PBProtoVnode{
					Id:   local_vn.successors[i].Id,
					Host: proto.String(local_vn.successors[i].Host),
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
	// if finger table has been initialized - forward request to closest finger
	// otherwise forward to my successor
	var forward_vn *Vnode
	if local_vn.stabilized.IsZero() {
		forward_vn = local_vn.successors[0]
	} else {
		forward_vn = local_vn.closest_preceeding_finger(key)
	}
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
