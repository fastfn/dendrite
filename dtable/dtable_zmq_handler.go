package dtable

import (
	//"bytes"
	"fmt"
	"github.com/fastfn/dendrite"
	"github.com/golang/protobuf/proto"
	//"log"
)

func (dt *DTable) zmq_status_handler(request *dendrite.ChordMsg, w chan *dendrite.ChordMsg) {
	pbMsg := request.TransportMsg.(PBDTableStatus)

	dest := &dendrite.Vnode{
		Id:   pbMsg.GetDest().GetId(),
		Host: pbMsg.GetDest().GetHost(),
	}

	dest_key_str := fmt.Sprintf("%x", dest.Id)
	zmq_transport := dt.transport.(*dendrite.ZMQTransport)

	// make sure destination vnode exists locally
	_, ok := dt.table[dest_key_str]
	setResp := &PBDTableResponse{}
	if !ok {
		setResp.Ok = proto.Bool(false)
		setResp.Error = proto.String("local dtable vnode not found")
	} else {
		setResp.Ok = proto.Bool(true)
	}

	// encode and send the response
	pbdata, err := proto.Marshal(setResp)
	if err != nil {
		errorMsg := zmq_transport.NewErrorMsg("ZMQ::DTable::StatusHandler - failed to marshal response - " + err.Error())
		w <- errorMsg
		return
	}
	w <- &dendrite.ChordMsg{
		Type: PbDtableResponse,
		Data: pbdata,
	}
	return
}

func (dt *DTable) zmq_get_handler(request *dendrite.ChordMsg, w chan *dendrite.ChordMsg) {
	pbMsg := request.TransportMsg.(PBDTableGetItem)
	keyHash := pbMsg.GetKeyHash()
	dest := &dendrite.Vnode{
		Id:   pbMsg.GetDest().GetId(),
		Host: pbMsg.GetDest().GetHost(),
	}
	dest_key_str := fmt.Sprintf("%x", dest.Id)
	zmq_transport := dt.transport.(*dendrite.ZMQTransport)

	// make sure destination vnode exists locally
	vn_table, ok := dt.table[dest_key_str]
	if !ok {
		errorMsg := zmq_transport.NewErrorMsg("ZMQ::DTable::GetHandler - local vnode table not found")
		w <- errorMsg
		return
	}
	key_str := fmt.Sprintf("%x", keyHash)

	var itemResp *PBDTableItem

	if localItem, ok := vn_table[key_str]; ok {
		itemResp = localItem.to_protobuf()
		itemResp.Found = proto.Bool(true)
	} else {
		itemResp = &PBDTableItem{
			Found: proto.Bool(false),
		}
	}

	// encode and send the response
	pbdata, err := proto.Marshal(itemResp)
	if err != nil {
		errorMsg := zmq_transport.NewErrorMsg("ZMQ::DTable::GetHandler - failed to marshal response - " + err.Error())
		w <- errorMsg
		return
	}
	w <- &dendrite.ChordMsg{
		Type: PbDtableItem,
		Data: pbdata,
	}
	return
}

func (dt *DTable) zmq_set_handler(request *dendrite.ChordMsg, w chan *dendrite.ChordMsg) {
	pbMsg := request.TransportMsg.(PBDTableSetItem)
	reqItem := new(kvItem)
	reqItem.from_protobuf(pbMsg.GetItem())
	demoting := pbMsg.GetDemoting()
	minAcks := int(pbMsg.GetMinAcks())
	dest := &dendrite.Vnode{
		Id:   pbMsg.GetDest().GetId(),
		Host: pbMsg.GetDest().GetHost(),
	}
	origin := &dendrite.Vnode{
		Id:   pbMsg.GetOrigin().GetId(),
		Host: pbMsg.GetOrigin().GetHost(),
	}

	dest_key_str := fmt.Sprintf("%x", dest.Id)
	zmq_transport := dt.transport.(*dendrite.ZMQTransport)

	// make sure destination vnode exists locally
	_, ok := dt.table[dest_key_str]
	if !ok {
		errorMsg := zmq_transport.NewErrorMsg("ZMQ::DTable::SetHandler - local vnode table not found")
		w <- errorMsg
		return
	}
	setResp := &PBDTableResponse{
		Ok: proto.Bool(false),
	}

	if demoting {
		old_master := reqItem.replicaInfo.master
		reqItem.replicaInfo.master = dest
		err := dt.table[dest_key_str].put(reqItem)
		if err != nil {
			errorMsg := zmq_transport.NewErrorMsg("ZMQ::DTable::SetHandler - demote received error on - " + err.Error())
			w <- errorMsg
			return
		}
		go dt.processDemoteKey(dest, origin, old_master, reqItem)
		setResp.Ok = proto.Bool(true)
	} else {
		wait := make(chan error)
		go dt.set(dest, reqItem, minAcks, wait)
		err := <-wait
		if err != nil {
			setResp.Error = proto.String("ZMQ::DTable::SetHandler - error executing transaction - " + err.Error())
		} else {
			setResp.Ok = proto.Bool(true)
		}
	}

	// encode and send the response
	pbdata, err := proto.Marshal(setResp)
	if err != nil {
		errorMsg := zmq_transport.NewErrorMsg("ZMQ::DTable::SetHandler - failed to marshal response - " + err.Error())
		w <- errorMsg
		return
	}
	w <- &dendrite.ChordMsg{
		Type: PbDtableResponse,
		Data: pbdata,
	}

	return
}

func (dt *DTable) zmq_setReplica_handler(request *dendrite.ChordMsg, w chan *dendrite.ChordMsg) {
	pbMsg := request.TransportMsg.(PBDTableSetItem)
	reqItem := new(kvItem)
	reqItem.from_protobuf(pbMsg.GetItem())

	dest := &dendrite.Vnode{
		Id:   pbMsg.GetDest().GetId(),
		Host: pbMsg.GetDest().GetHost(),
	}

	dest_key_str := fmt.Sprintf("%x", dest.Id)
	zmq_transport := dt.transport.(*dendrite.ZMQTransport)
	// make sure destination vnode exists locally
	_, ok := dt.table[dest_key_str]
	if !ok {
		errorMsg := zmq_transport.NewErrorMsg("ZMQ::DTable::SetReplicaHandler - local vnode table not found")
		w <- errorMsg
		return
	}
	setResp := &PBDTableResponse{
		Ok: proto.Bool(true),
	}
	dt.setReplica(dest, reqItem)

	// encode and send the response
	pbdata, err := proto.Marshal(setResp)
	if err != nil {
		errorMsg := zmq_transport.NewErrorMsg("ZMQ::DTable::SetReplicaHandler - failed to marshal response - " + err.Error())
		w <- errorMsg
		return
	}
	w <- &dendrite.ChordMsg{
		Type: PbDtableResponse,
		Data: pbdata,
	}

	return
}

func (dt *DTable) zmq_setReplicaInfo_handler(request *dendrite.ChordMsg, w chan *dendrite.ChordMsg) {
	pbMsg := request.TransportMsg.(PBDTableSetReplicaInfo)
	rInfo := replicaInfo_from_protobuf(pbMsg.GetReplicaInfo())
	keyHash := pbMsg.GetKeyHash()
	dest := &dendrite.Vnode{
		Id:   pbMsg.GetDest().GetId(),
		Host: pbMsg.GetDest().GetHost(),
	}

	dest_key_str := fmt.Sprintf("%x", dest.Id)
	zmq_transport := dt.transport.(*dendrite.ZMQTransport)

	// make sure destination vnode exists locally
	vn_table, ok := dt.rtable[dest_key_str]
	if !ok {
		errorMsg := zmq_transport.NewErrorMsg("ZMQ::DTable::SetMetaHandler - local vnode table not found")
		w <- errorMsg
		return
	}
	key_str := fmt.Sprintf("%x", keyHash)
	item, ok := vn_table[key_str]
	if !ok {
		errorMsg := zmq_transport.NewErrorMsg("ZMQ::DTable::SetMetaHandler - key not found")
		w <- errorMsg
		return
	}
	item.replicaInfo = rInfo

	// encode and send the response
	setResp := &PBDTableResponse{
		Ok: proto.Bool(true),
	}
	pbdata, err := proto.Marshal(setResp)
	if err != nil {
		errorMsg := zmq_transport.NewErrorMsg("ZMQ::DTable::SetMetaHandler - failed to marshal response - " + err.Error())
		w <- errorMsg
		return
	}
	w <- &dendrite.ChordMsg{
		Type: PbDtableResponse,
		Data: pbdata,
	}
	return
}

func (dt *DTable) zmq_clearreplica_handler(request *dendrite.ChordMsg, w chan *dendrite.ChordMsg) {
	pbMsg := request.TransportMsg.(PBDTableClearReplica)
	keyHash := pbMsg.GetKeyHash()
	demoted := pbMsg.GetDemoted()
	dest := &dendrite.Vnode{
		Id:   pbMsg.GetDest().GetId(),
		Host: pbMsg.GetDest().GetHost(),
	}
	dest_key_str := fmt.Sprintf("%x", dest.Id)
	zmq_transport := dt.transport.(*dendrite.ZMQTransport)

	// make sure destination vnode exists locally
	r_table, ok := dt.rtable[dest_key_str]
	if !ok {
		errorMsg := zmq_transport.NewErrorMsg("ZMQ::DTable::ClearReplicaHandler - local vnode table not found")
		w <- errorMsg
		return
	}

	key_str := fmt.Sprintf("%x", keyHash)
	if demoted {
		d_table, _ := dt.demoted_table[dest_key_str]
		if _, ok := d_table[key_str]; ok {
			delete(d_table, key_str)
		} else {
			errorMsg := zmq_transport.NewErrorMsg("ZMQ::DTable::ClearReplicaHandler - key " + key_str + " not found in demoted table")
			w <- errorMsg
			return
		}
	} else {
		if _, ok := r_table[key_str]; ok {
			delete(r_table, key_str)
		} else {
			errorMsg := zmq_transport.NewErrorMsg("ZMQ::DTable::ClearReplicaHandler - key " + key_str + " not found in replica table on vnode " + dest.String())
			w <- errorMsg
			return
		}
	}

	// encode and send the response
	setResp := &PBDTableResponse{
		Ok: proto.Bool(true),
	}
	pbdata, err := proto.Marshal(setResp)
	if err != nil {
		errorMsg := zmq_transport.NewErrorMsg("ZMQ::DTable::ClearReplicaHandler - failed to marshal response - " + err.Error())
		w <- errorMsg
		return
	}
	w <- &dendrite.ChordMsg{
		Type: PbDtableResponse,
		Data: pbdata,
	}
	return
}

func (dt *DTable) zmq_promoteKey_handler(request *dendrite.ChordMsg, w chan *dendrite.ChordMsg) {
	pbMsg := request.TransportMsg.(PBDTablePromoteKey)
	reqItem := new(kvItem)
	reqItem.from_protobuf(pbMsg.GetItem())

	// send out the event to delegator
	ev := &dtableEvent{
		evType: evPromoteKey,
		vnode:  dendrite.VnodeFromProtobuf(pbMsg.GetDest()),
		item:   reqItem,
	}
	dt.dtable_c <- ev

	zmq_transport := dt.transport.(*dendrite.ZMQTransport)

	setResp := &PBDTableResponse{
		Ok: proto.Bool(true),
	}

	// encode and send the response
	pbdata, err := proto.Marshal(setResp)
	if err != nil {
		errorMsg := zmq_transport.NewErrorMsg("ZMQ::DTable::SetReplicaHandler - failed to marshal response - " + err.Error())
		w <- errorMsg
		return
	}
	w <- &dendrite.ChordMsg{
		Type: PbDtableResponse,
		Data: pbdata,
	}
	return
}
