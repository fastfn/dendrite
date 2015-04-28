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
	dest := &dendrite.Vnode{
		Id:   pbMsg.GetDest().GetId(),
		Host: pbMsg.GetDest().GetHost(),
	}
	dest_key_str := fmt.Sprintf("%x", dest.Id)
	zmq_transport := dt.transport.(*dendrite.ZMQTransport)

	// make sure destination vnode exists locally
	vn_table, ok := dt.table[dest_key_str]
	if !ok {
		errorMsg := zmq_transport.NewErrorMsg("ZMQ::DTable::SetHandler - local vnode table not found")
		w <- errorMsg
		return
	}
	key_str := fmt.Sprintf("%x", key)
	new_val := &value{
		timestamp: time.Now(),
		Val:       val,
	}
	setResp := &PBDTableSetResp{
		Ok: proto.Bool(false),
	}
	// see if key exists with older timestamp
	if v, ok := vn_table[key_str]; ok {
		if v.timestamp.UnixNano() <= new_val.timestamp.UnixNano() {
			// key exists but the record is older than new one
			if new_val.Val == nil {
				delete(vn_table, key_str)
			} else {
				vn_table[key_str] = new_val
			}
			setResp.Ok = proto.Bool(true)
		} else {
			setResp.Error = proto.String("new record too old to set for this key")
		}
	} else {
		if new_val.Val == nil {
			delete(vn_table, key_str)
		} else {
			vn_table[key_str] = new_val
		}
		setResp.Ok = proto.Bool(true)
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
