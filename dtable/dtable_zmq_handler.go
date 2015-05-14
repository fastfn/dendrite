package dtable

import (
	//"bytes"
	"fmt"
	"github.com/fastfn/dendrite"
	"github.com/golang/protobuf/proto"
	"time"
	//"log"
)

func (dt *DTable) zmq_get_handler(request *dendrite.ChordMsg, w chan *dendrite.ChordMsg) {
	pbMsg := request.TransportMsg.(PBDTableGet)
	key := pbMsg.GetKey()
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
	key_str := fmt.Sprintf("%x", key)
	valueResp := &PBDTableGetResp{
		Found: proto.Bool(false),
		Value: nil,
	}
	if val, ok := vn_table[key_str]; ok {
		valueResp.Found = proto.Bool(true)
		valueResp.Value = val.Val
	}
	// encode and send the response
	pbdata, err := proto.Marshal(valueResp)
	if err != nil {
		errorMsg := zmq_transport.NewErrorMsg("ZMQ::DTable::GetHandler - failed to marshal response - " + err.Error())
		w <- errorMsg
		return
	}
	w <- &dendrite.ChordMsg{
		Type: PbDtableGetResp,
		Data: pbdata,
	}
	return
}

func (dt *DTable) zmq_set_handler(request *dendrite.ChordMsg, w chan *dendrite.ChordMsg) {
	pbMsg := request.TransportMsg.(PBDTableSet)
	key := pbMsg.GetKey()
	val := pbMsg.GetVal()
	isReplica := pbMsg.GetIsReplica()
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
	success := true

	// make sure destination vnode exists locally
	_, ok := dt.table[dest_key_str]
	if !ok {
		errorMsg := zmq_transport.NewErrorMsg("ZMQ::DTable::SetHandler - local vnode table not found")
		w <- errorMsg
		return
	}
	key_str := fmt.Sprintf("%x", key)
	setResp := &PBDTableSetResp{
		Ok: proto.Bool(false),
	}
	var demote_value *value
	if isReplica {
		new_rval := &rvalue{
			timestamp: time.Now(),
			Val:       val,
			state:     replicaIncomplete,
		}
		dt.setReplica(dest, key_str, new_rval)
		setResp.Ok = proto.Bool(true)
	} else {
		new_val := &value{
			timestamp: time.Now(),
			Val:       val,
			isReplica: false,
			commited:  false,
			rstate:    replicaIncomplete,
		}
		demote_value = new_val
		wait := make(chan error)
		go dt.set(dest, key, new_val, minAcks, wait)
		err := <-wait
		if err != nil {
			setResp.Error = proto.String("ZMQ::DTable::SetHandler - error executing transaction - " + err.Error())
			success = false
		} else {
			setResp.Ok = proto.Bool(true)
		}
	}

	// trigger demote callback if everything's good
	// processDemoteKey will rearange replicas if necessary
	if success && demoting {
		replica_vnodes := make([]*dendrite.Vnode, 0)
		for _, pbrepVnode := range pbMsg.GetReplicaVnodes() {
			replica_vnodes = append(replica_vnodes, &dendrite.Vnode{
				Id:   pbrepVnode.GetId(),
				Host: pbrepVnode.GetHost(),
			})
		}
		go dt.processDemoteKey(dest, origin, key, demote_value, replica_vnodes)
	}
	// encode and send the response
	pbdata, err := proto.Marshal(setResp)
	if err != nil {
		errorMsg := zmq_transport.NewErrorMsg("ZMQ::DTable::SetHandler - failed to marshal response - " + err.Error())
		w <- errorMsg
		return
	}
	w <- &dendrite.ChordMsg{
		Type: PbDtableSetResp,
		Data: pbdata,
	}

	return
}

func (dt *DTable) zmq_setmeta_handler(request *dendrite.ChordMsg, w chan *dendrite.ChordMsg) {
	pbMsg := request.TransportMsg.(PBDTableSetMeta)
	key := pbMsg.GetKey()
	state := pbMsg.GetState()
	master := &dendrite.Vnode{
		Id:   pbMsg.GetMaster().GetId(),
		Host: pbMsg.GetMaster().GetHost(),
	}
	depth := pbMsg.GetDepth()
	replica_vnodes := make([]*dendrite.Vnode, 0)
	for _, pbrepVnode := range pbMsg.GetReplicaVnodes() {
		replica_vnodes = append(replica_vnodes, &dendrite.Vnode{
			Id:   pbrepVnode.GetId(),
			Host: pbrepVnode.GetHost(),
		})
	}

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
	key_str := fmt.Sprintf("%x", key)
	item, ok := vn_table[key_str]
	if !ok {
		errorMsg := zmq_transport.NewErrorMsg("ZMQ::DTable::SetMetaHandler - key not found")
		w <- errorMsg
		return
	}

	item.state = replicaState(state)
	item.master = master
	item.depth = int(depth)
	item.replicaVnodes = replica_vnodes
	vn_table[key_str] = item
	// encode and send the response
	setResp := &PBDTableSetResp{
		Ok: proto.Bool(true),
	}
	pbdata, err := proto.Marshal(setResp)
	if err != nil {
		errorMsg := zmq_transport.NewErrorMsg("ZMQ::DTable::SetMetaHandler - failed to marshal response - " + err.Error())
		w <- errorMsg
		return
	}
	w <- &dendrite.ChordMsg{
		Type: PbDtableSetResp,
		Data: pbdata,
	}
	return
}

func (dt *DTable) zmq_clearreplica_handler(request *dendrite.ChordMsg, w chan *dendrite.ChordMsg) {
	pbMsg := request.TransportMsg.(PBDTableClearReplica)
	key := pbMsg.GetKey()
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

	key_str := fmt.Sprintf("%x", key)
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
			errorMsg := zmq_transport.NewErrorMsg("ZMQ::DTable::ClearReplicaHandler - key " + key_str + " not found in replica table")
			w <- errorMsg
			return
		}
	}

	// encode and send the response
	setResp := &PBDTableSetResp{
		Ok: proto.Bool(true),
	}
	pbdata, err := proto.Marshal(setResp)
	if err != nil {
		errorMsg := zmq_transport.NewErrorMsg("ZMQ::DTable::ClearReplicaHandler - failed to marshal response - " + err.Error())
		w <- errorMsg
		return
	}
	w <- &dendrite.ChordMsg{
		Type: PbDtableSetResp,
		Data: pbdata,
	}
	return
}
