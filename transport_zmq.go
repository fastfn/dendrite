package dendrite

import (
	"github.com/golang/protobuf/proto"
	zmq "github.com/pebbe/zmq4"
	//"log"
	//"sync"
	//"time"
	"bytes"
	"fmt"
)

const (
	// protocol buffer messages (for definitions, see pb_defs/chord.go)
	pbPing MsgType = iota
	pbAck
	pbErr
	pbForward
	pbJoin
	pbLeave
	pbListVnodes
	pbListVnodesResp
)

func (transport *ZMQTransport) newErrorMsg(msg string) *ChordMsg {
	pbmsg := &PBProtoErr{
		Version: proto.Int64(1),
		Error:   proto.String(msg),
	}
	pbdata, _ := proto.Marshal(pbmsg)
	return &ChordMsg{
		Type: pbErr,
		Data: pbdata,
	}

}
func (transport *ZMQTransport) Encode(mt MsgType, data []byte) []byte {
	buf := new(bytes.Buffer)
	buf.WriteByte(byte(mt))
	buf.Write(data)
	return buf.Bytes()
}

func (transport *ZMQTransport) Decode(data []byte) (*ChordMsg, error) {
	if len(data) < 2 {
		return nil, fmt.Errorf("data too short")
	}
	cm := &ChordMsg{
		Type: MsgType(data[0]),
		Data: data[1:],
	}

	// parse the data and set the handler
	switch cm.Type {
	case pbPing:
		var pingMsg PBProtoPing
		err := proto.Unmarshal(cm.Data, &pingMsg)
		if err != nil {
			return nil, fmt.Errorf("error decoding PBProtoPing message - %s", err)
		}
		cm.TransportMsg = pingMsg
		cm.TransportHandler = transport.zmq_ping_handler
	case pbJoin:
		var joinMsg PBProtoJoin
		err := proto.Unmarshal(cm.Data, &joinMsg)
		if err != nil {
			return nil, fmt.Errorf("error decoding PBProtoJoin message - %s", err)
		}
		cm.TransportMsg = joinMsg
		cm.TransportHandler = transport.zmq_join_handler
	case pbLeave:
		var leaveMsg PBProtoLeave
		err := proto.Unmarshal(cm.Data, &leaveMsg)
		if err != nil {
			return nil, fmt.Errorf("error decoding PBProtoLeave message - %s", err)
		}
		cm.TransportMsg = leaveMsg
		cm.TransportHandler = transport.zmq_leave_handler
	case pbListVnodes:
		var listVnodesMsg PBProtoListVnodes
		err := proto.Unmarshal(cm.Data, &listVnodesMsg)
		if err != nil {
			return nil, fmt.Errorf("error decoding PBProtoListVnodes message - %s", err)
		}
		cm.TransportMsg = listVnodesMsg
		cm.TransportHandler = transport.zmq_listVnodes_handler
	default:
		return nil, fmt.Errorf("error decoding message - unknown request type")
	}

	return cm, nil
}

func (transport *ZMQTransport) ListVnodes(host string) ([]*Vnode, error) {
	req_sock, err := transport.zmq_context.NewSocket(zmq.REQ)
	if err != nil {
		return nil, err
	}
	err = req_sock.Connect("tcp://" + host)
	if err != nil {
		return nil, err
	}

	return nil, nil
}

func (transport *ZMQTransport) Ping(remote_vn *Vnode) (bool, error) {
	req_sock, err := transport.zmq_context.NewSocket(zmq.REQ)
	if err != nil {
		return false, err
	}
	err = req_sock.Connect("tcp://" + remote_vn.Host)
	if err != nil {
		return false, err
	}
	pbPingMsg := &PBProtoPing{
		Version: proto.Int64(1),
	}
	pbPingData, _ := proto.Marshal(pbPingMsg)
	encoded := transport.Encode(pbPing, pbPingData)
	req_sock.SendBytes(encoded, 0)
	resp, _ := req_sock.RecvBytes(0)
	decoded, err := transport.Decode(resp)
	if err != nil {
		return false, err
	}
	pongMsg := new(PBProtoPing)
	err = proto.Unmarshal(decoded.Data, pongMsg)
	if err != nil {
		return false, err
	}
	fmt.Println("Got pong with version:", pongMsg.GetVersion())
	return true, nil
}
