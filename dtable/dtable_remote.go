package dtable

import (
	"fmt"
	"github.com/fastfn/dendrite"
	"github.com/golang/protobuf/proto"
	zmq "github.com/pebbe/zmq4"
	"time"
)

// Client Request: Get value for a key from remote host
func (dt *DTable) remoteGet(remote *dendrite.Vnode, reqItem *kvItem) (*kvItem, bool, error) {
	error_c := make(chan error, 1)
	resp_c := make(chan *kvItem, 1)
	notfound_c := make(chan bool, 1)
	zmq_transport := dt.transport.(*dendrite.ZMQTransport)
	go func() {

		req_sock, err := zmq_transport.ZMQContext.NewSocket(zmq.REQ)
		if err != nil {
			error_c <- fmt.Errorf("ZMQ:DTable:remoteGet - newsocket error - %s", err)
			return
		}
		req_sock.SetRcvtimeo(2 * time.Second)
		req_sock.SetSndtimeo(2 * time.Second)

		defer req_sock.Close()
		err = req_sock.Connect("tcp://" + remote.Host)
		if err != nil {
			error_c <- fmt.Errorf("ZMQ:DTable:remoteGet - connect error - %s", err)
			return
		}
		// Build request protobuf
		dest := &dendrite.PBProtoVnode{
			Host: proto.String(remote.Host),
			Id:   remote.Id,
		}
		req := &PBDTableGetItem{
			Dest:    dest,
			KeyHash: reqItem.keyHash,
		}

		reqData, _ := proto.Marshal(req)
		encoded := dt.transport.Encode(PbDtableGetItem, reqData)
		_, err = req_sock.SendBytes(encoded, 0)
		if err != nil {
			error_c <- fmt.Errorf("ZMQ:DTable:remoteGet - error while sending request - %s", err)
			return
		}

		// read response and decode it
		resp, err := req_sock.RecvBytes(0)
		if err != nil {
			error_c <- fmt.Errorf("ZMQ:DTable:remoteGet - error while reading response - %s", err)
			return
		}
		decoded, err := dt.transport.Decode(resp)
		if err != nil {
			error_c <- fmt.Errorf("ZMQ:DTable:remoteGet - error while decoding response - %s", err)
			return
		}

		switch decoded.Type {
		case dendrite.PbErr:
			pbMsg := decoded.TransportMsg.(dendrite.PBProtoErr)
			error_c <- fmt.Errorf("ZMQ:DTable:remoteGet - got error response - %s", pbMsg.GetError())
		case PbDtableItem:
			pbMsg := decoded.TransportMsg.(PBDTableItem)
			if found := pbMsg.GetFound(); !found {
				notfound_c <- true
				return
			}
			item := new(kvItem)
			copy(item.Key, reqItem.Key)
			copy(item.keyHash, reqItem.keyHash)
			item.Val = pbMsg.GetVal()
			resp_c <- item
			return
		default:
			// unexpected response
			error_c <- fmt.Errorf("ZMQ:DTable:remoteGet - unexpected response")
			return
		}
	}()

	select {
	case <-time.After(zmq_transport.ClientTimeout):
		return nil, false, fmt.Errorf("ZMQ:DTable:remoteGet - command timed out!")
	case err := <-error_c:
		return nil, false, err
	case _ = <-notfound_c:
		return nil, false, nil
	case item := <-resp_c:
		return item, true, nil
	}
}

// Client Request: set value for a key to remote host
func (dt *DTable) remoteSet(origin, remote *dendrite.Vnode, reqItem *kvItem, minAcks int, demoting bool, done chan error) {
	error_c := make(chan error, 1)
	resp_c := make(chan bool, 1)
	zmq_transport := dt.transport.(*dendrite.ZMQTransport)

	go func() {
		req_sock, err := zmq_transport.ZMQContext.NewSocket(zmq.REQ)
		if err != nil {
			error_c <- fmt.Errorf("ZMQ:DTable:remoteSet - newsocket error - %s", err)
			return
		}
		req_sock.SetRcvtimeo(5 * time.Second)
		req_sock.SetSndtimeo(5 * time.Second)

		defer req_sock.Close()
		err = req_sock.Connect("tcp://" + remote.Host)
		if err != nil {
			error_c <- fmt.Errorf("ZMQ:DTable:remoteSet - connect error - %s", err)
			return
		}
		// Build request protobuf
		dest := &dendrite.PBProtoVnode{
			Host: proto.String(remote.Host),
			Id:   remote.Id,
		}
		origin := &dendrite.PBProtoVnode{
			Host: proto.String(origin.Host),
			Id:   origin.Id,
		}

		req := &PBDTableSetItem{
			Origin:   origin,
			Dest:     dest,
			Item:     reqItem.to_protobuf(),
			MinAcks:  proto.Int32(int32(minAcks)),
			Demoting: proto.Bool(demoting),
		}

		reqData, _ := proto.Marshal(req)
		encoded := dt.transport.Encode(PbDtableSetItem, reqData)
		_, err = req_sock.SendBytes(encoded, 0)
		if err != nil {
			error_c <- fmt.Errorf("ZMQ:DTable:remoteSet - error while sending request - %s", err)
			return
		}

		// read response and decode it
		resp, err := req_sock.RecvBytes(0)
		if err != nil {
			error_c <- fmt.Errorf("ZMQ:DTable:remoteSet - error while reading response - %s", err)
			return
		}
		decoded, err := dt.transport.Decode(resp)
		if err != nil {
			error_c <- fmt.Errorf("ZMQ:DTable:remoteSet - error while decoding response - %s", err)
			return
		}

		switch decoded.Type {
		case dendrite.PbErr:
			pbMsg := decoded.TransportMsg.(dendrite.PBProtoErr)
			error_c <- fmt.Errorf("ZMQ:DTable:remoteSet - got error response - %s", pbMsg.GetError())
		case PbDtableResponse:
			pbMsg := decoded.TransportMsg.(PBDTableResponse)
			success := pbMsg.GetOk()
			if success {
				resp_c <- true
				return
			}
			error_c <- fmt.Errorf("ZMQ:DTable:remoteSet - write error - %s", pbMsg.GetError())
			return
		default:
			// unexpected response
			error_c <- fmt.Errorf("ZMQ:DTable:remoteSet - unexpected response")
			return
		}
	}()

	select {
	case <-time.After(zmq_transport.ClientTimeout):
		done <- fmt.Errorf("ZMQ:DTable:remoteSet - command timed out!")
	case err := <-error_c:
		done <- err
	case _ = <-resp_c:
		done <- nil
	}
}

// Client Request: set replicaInfo for replicated item to remote host
func (dt *DTable) remoteSetReplicaInfo(remote *dendrite.Vnode, reqItem *kvItem) error {
	error_c := make(chan error, 1)
	resp_c := make(chan bool, 1)
	zmq_transport := dt.transport.(*dendrite.ZMQTransport)

	go func() {
		req_sock, err := zmq_transport.ZMQContext.NewSocket(zmq.REQ)
		if err != nil {
			error_c <- fmt.Errorf("ZMQ:DTable:remoteSetReplicaInfo - newsocket error - %s", err)
			return
		}
		req_sock.SetRcvtimeo(5 * time.Second)
		req_sock.SetSndtimeo(5 * time.Second)

		defer req_sock.Close()
		err = req_sock.Connect("tcp://" + remote.Host)
		if err != nil {
			error_c <- fmt.Errorf("ZMQ:DTable:remoteSetReplicaInfo - connect error - %s", err)
			return
		}
		// Build request protobuf
		dest := &dendrite.PBProtoVnode{
			Host: proto.String(remote.Host),
			Id:   remote.Id,
		}
		pb_reqItem := reqItem.to_protobuf()

		req := &PBDTableSetReplicaInfo{
			Dest:        dest,
			KeyHash:     reqItem.keyHash,
			ReplicaInfo: pb_reqItem.GetReplicaInfo(),
		}

		reqData, _ := proto.Marshal(req)
		encoded := dt.transport.Encode(PbDtableSetReplicaInfo, reqData)
		_, err = req_sock.SendBytes(encoded, 0)
		if err != nil {
			error_c <- fmt.Errorf("ZMQ:DTable:remoteSetReplicaInfo - error while sending request - %s", err)
			return
		}

		// read response and decode it
		resp, err := req_sock.RecvBytes(0)
		if err != nil {
			error_c <- fmt.Errorf("ZMQ:DTable:remoteSetReplicaInfo - error while reading response - %s", err)
			return
		}
		decoded, err := dt.transport.Decode(resp)
		if err != nil {
			error_c <- fmt.Errorf("ZMQ:DTable:remoteSetReplicaInfo - error while decoding response - %s", err)
			return
		}

		switch decoded.Type {
		case dendrite.PbErr:
			pbMsg := decoded.TransportMsg.(dendrite.PBProtoErr)
			error_c <- fmt.Errorf("ZMQ:DTable:remoteSetReplicaInfo - got error response - %s", pbMsg.GetError())
		case PbDtableResponse:
			pbMsg := decoded.TransportMsg.(PBDTableResponse)
			success := pbMsg.GetOk()
			if success {
				resp_c <- true
				return
			}
			error_c <- fmt.Errorf("ZMQ:DTable:remoteSetReplicaInfo - write error - %s", pbMsg.GetError())
			return
		default:
			// unexpected response
			error_c <- fmt.Errorf("ZMQ:DTable:remoteSetReplicaInfo - unexpected response")
			return
		}
	}()

	select {
	case <-time.After(zmq_transport.ClientTimeout):
		return fmt.Errorf("ZMQ:DTable:remoteSetReplicaInfo - command timed out!")
	case err := <-error_c:
		return err
	case _ = <-resp_c:
		return nil
	}
}

// Client Request: remove replica
func (dt *DTable) remoteClearReplica(remote *dendrite.Vnode, reqItem *kvItem, demoted bool) error {
	error_c := make(chan error, 1)
	resp_c := make(chan bool, 1)
	zmq_transport := dt.transport.(*dendrite.ZMQTransport)

	go func() {
		req_sock, err := zmq_transport.ZMQContext.NewSocket(zmq.REQ)
		if err != nil {
			error_c <- fmt.Errorf("ZMQ:DTable:remoteClearReplica - newsocket error - %s", err)
			return
		}
		req_sock.SetRcvtimeo(5 * time.Second)
		req_sock.SetSndtimeo(5 * time.Second)

		defer req_sock.Close()
		err = req_sock.Connect("tcp://" + remote.Host)
		if err != nil {
			error_c <- fmt.Errorf("ZMQ:DTable:remoteClearReplica - connect error - %s", err)
			return
		}
		// Build request protobuf
		dest := &dendrite.PBProtoVnode{
			Host: proto.String(remote.Host),
			Id:   remote.Id,
		}

		req := &PBDTableClearReplica{
			Dest:    dest,
			KeyHash: reqItem.keyHash,
			Demoted: proto.Bool(demoted),
		}

		reqData, _ := proto.Marshal(req)
		encoded := dt.transport.Encode(PbDtableClearReplica, reqData)
		_, err = req_sock.SendBytes(encoded, 0)
		if err != nil {
			error_c <- fmt.Errorf("ZMQ:DTable:remoteClearReplica - error while sending request - %s", err)
			return
		}

		// read response and decode it
		resp, err := req_sock.RecvBytes(0)
		if err != nil {
			error_c <- fmt.Errorf("ZMQ:DTable:remoteClearReplica - error while reading response - %s", err)
			return
		}
		decoded, err := dt.transport.Decode(resp)
		if err != nil {
			error_c <- fmt.Errorf("ZMQ:DTable:remoteClearReplica - error while decoding response - %s", err)
			return
		}

		switch decoded.Type {
		case dendrite.PbErr:
			pbMsg := decoded.TransportMsg.(dendrite.PBProtoErr)
			error_c <- fmt.Errorf("ZMQ:DTable:remoteClearReplica - got error response - %s", pbMsg.GetError())
		case PbDtableResponse:
			pbMsg := decoded.TransportMsg.(PBDTableResponse)
			success := pbMsg.GetOk()
			if success {
				resp_c <- true
				return
			}
			error_c <- fmt.Errorf("ZMQ:DTable:remoteClearReplica - error - %s", pbMsg.GetError())
			return
		default:
			// unexpected response
			error_c <- fmt.Errorf("ZMQ:DTable:remoteClearReplica - unexpected response")
			return
		}
	}()

	select {
	case <-time.After(zmq_transport.ClientTimeout):
		return fmt.Errorf("ZMQ:DTable:remoteClearReplica - command timed out!")
	case err := <-error_c:
		return err
	case _ = <-resp_c:
		return nil
	}
}

// Client Request: take a rvalue and write replica to another host
func (dt *DTable) remoteWriteReplica(origin, remote *dendrite.Vnode, reqItem *kvItem) error {
	error_c := make(chan error, 1)
	resp_c := make(chan bool, 1)
	zmq_transport := dt.transport.(*dendrite.ZMQTransport)

	go func() {
		req_sock, err := zmq_transport.ZMQContext.NewSocket(zmq.REQ)
		if err != nil {
			error_c <- fmt.Errorf("ZMQ:DTable:remoteWriteReplica - newsocket error - %s", err)
			return
		}
		req_sock.SetRcvtimeo(5 * time.Second)
		req_sock.SetSndtimeo(5 * time.Second)

		defer req_sock.Close()
		err = req_sock.Connect("tcp://" + remote.Host)
		if err != nil {
			error_c <- fmt.Errorf("ZMQ:DTable:remoteWriteReplica - connect error - %s", err)
			return
		}
		// Build request protobuf
		dest := &dendrite.PBProtoVnode{
			Host: proto.String(remote.Host),
			Id:   remote.Id,
		}
		origin := &dendrite.PBProtoVnode{
			Host: proto.String(origin.Host),
			Id:   origin.Id,
		}
		req := &PBDTableSetItem{
			Origin: origin,
			Dest:   dest,
			Item:   reqItem.to_protobuf(),
		}

		reqData, _ := proto.Marshal(req)
		encoded := dt.transport.Encode(PbDtableSetReplica, reqData)
		_, err = req_sock.SendBytes(encoded, 0)
		if err != nil {
			error_c <- fmt.Errorf("ZMQ:DTable:remoteWriteReplica - error while sending request - %s", err)
			return
		}

		// read response and decode it
		resp, err := req_sock.RecvBytes(0)
		if err != nil {
			error_c <- fmt.Errorf("ZMQ:DTable:remoteWriteReplica - error while reading response - %s", err)
			return
		}
		decoded, err := dt.transport.Decode(resp)
		if err != nil {
			error_c <- fmt.Errorf("ZMQ:DTable:remoteWriteReplica - error while decoding response - %s", err)
			return
		}

		switch decoded.Type {
		case dendrite.PbErr:
			pbMsg := decoded.TransportMsg.(dendrite.PBProtoErr)
			error_c <- fmt.Errorf("ZMQ:DTable:remoteWriteReplica - got error response - %s", pbMsg.GetError())
		case PbDtableResponse:
			pbMsg := decoded.TransportMsg.(PBDTableResponse)
			success := pbMsg.GetOk()
			if success {
				resp_c <- true
				return
			}
			error_c <- fmt.Errorf("ZMQ:DTable:remoteWriteReplica - write error - %s", pbMsg.GetError())
			return
		default:
			// unexpected response
			error_c <- fmt.Errorf("ZMQ:DTable:remoteWriteReplica - unexpected response")
			return
		}
	}()

	select {
	case <-time.After(zmq_transport.ClientTimeout):
		return fmt.Errorf("ZMQ:DTable:remoteWriteReplica - command timed out!")
	case err := <-error_c:
		return err
	case _ = <-resp_c:
		return nil
	}
}

// Client Request: get dtable status of remote vnode
func (dt *DTable) remoteStatus(remote *dendrite.Vnode) error {
	error_c := make(chan error, 1)
	resp_c := make(chan bool, 1)
	zmq_transport := dt.transport.(*dendrite.ZMQTransport)

	go func() {
		req_sock, err := zmq_transport.ZMQContext.NewSocket(zmq.REQ)
		if err != nil {
			error_c <- fmt.Errorf("ZMQ:DTable:remoteStatus - newsocket error - %s", err)
			return
		}
		req_sock.SetRcvtimeo(5 * time.Second)
		req_sock.SetSndtimeo(5 * time.Second)

		defer req_sock.Close()
		err = req_sock.Connect("tcp://" + remote.Host)
		if err != nil {
			error_c <- fmt.Errorf("ZMQ:DTable:remoteStatus - connect error - %s", err)
			return
		}
		// Build request protobuf
		dest := &dendrite.PBProtoVnode{
			Host: proto.String(remote.Host),
			Id:   remote.Id,
		}

		req := &PBDTableStatus{
			Dest: dest,
		}

		reqData, _ := proto.Marshal(req)
		encoded := dt.transport.Encode(PbDtableStatus, reqData)
		_, err = req_sock.SendBytes(encoded, 0)
		if err != nil {
			error_c <- fmt.Errorf("ZMQ:DTable:remoteStatus - error while sending request - %s", err)
			return
		}

		// read response and decode it
		resp, err := req_sock.RecvBytes(0)
		if err != nil {
			error_c <- fmt.Errorf("ZMQ:DTable:remoteStatus - error while reading response - %s", err)
			return
		}
		decoded, err := dt.transport.Decode(resp)
		if err != nil {
			error_c <- fmt.Errorf("ZMQ:DTable:remoteStatus - error while decoding response - %s", err)
			return
		}

		switch decoded.Type {
		case dendrite.PbErr:
			pbMsg := decoded.TransportMsg.(dendrite.PBProtoErr)
			error_c <- fmt.Errorf("ZMQ:DTable:remoteStatus - got error response - %s", pbMsg.GetError())
		case PbDtableResponse:
			pbMsg := decoded.TransportMsg.(PBDTableResponse)
			success := pbMsg.GetOk()
			if success {
				resp_c <- true
				return
			}
			error_c <- fmt.Errorf("ZMQ:DTable:remoteStatus - error - %s", pbMsg.GetError())
			return
		default:
			// unexpected response
			error_c <- fmt.Errorf("ZMQ:DTable:remoteStatus - unexpected response")
			return
		}
	}()

	select {
	case <-time.After(zmq_transport.ClientTimeout):
		return fmt.Errorf("ZMQ:DTable:remoteStatus - command timed out!")
	case err := <-error_c:
		return err
	case _ = <-resp_c:
		return nil
	}
}
