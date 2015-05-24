package dtable

import (
	"fmt"
	"github.com/fastfn/dendrite"
	"github.com/golang/protobuf/proto"
	"log"
	"time"
)

func (item *kvItem) keyHashString() string {
	if item.keyHash == nil {
		item.keyHash = dendrite.HashKey(item.Key)
	}
	return fmt.Sprintf("%x", item.keyHash)
}

func (rinfo *kvReplicaInfo) to_protobuf() *PBDTableReplicaInfo {
	pb_master := rinfo.master.ToProtobuf()
	pb_vnodes := make([]*dendrite.PBProtoVnode, 0)
	for _, rvn := range rinfo.vnodes {
		if rvn == nil {
			continue
		}
		pb_vnodes = append(pb_vnodes, rvn.ToProtobuf())
	}
	pb_orphanVnodes := make([]*dendrite.PBProtoVnode, 0)
	for _, ovn := range rinfo.orphan_vnodes {
		pb_orphanVnodes = append(pb_orphanVnodes, ovn.ToProtobuf())
	}
	return &PBDTableReplicaInfo{
		Master:       pb_master,
		Vnodes:       pb_vnodes,
		OrphanVnodes: pb_orphanVnodes,
		State:        proto.Int32(int32(rinfo.state)),
		Depth:        proto.Int32(int32(rinfo.depth)),
	}
}

func replicaInfo_from_protobuf(pb *PBDTableReplicaInfo) *kvReplicaInfo {
	if pb == nil {
		return nil
	}
	rInfo := new(kvReplicaInfo)
	rInfo.master = dendrite.VnodeFromProtobuf(pb.GetMaster())
	rInfo.vnodes = make([]*dendrite.Vnode, 0)
	for _, pb_vnode := range pb.GetVnodes() {
		rInfo.vnodes = append(rInfo.vnodes, dendrite.VnodeFromProtobuf(pb_vnode))
	}
	rInfo.orphan_vnodes = make([]*dendrite.Vnode, 0)
	for _, pb_orphanVnode := range pb.GetOrphanVnodes() {
		rInfo.orphan_vnodes = append(rInfo.orphan_vnodes, dendrite.VnodeFromProtobuf(pb_orphanVnode))
	}
	rInfo.state = replicaState(int(pb.GetState()))
	rInfo.depth = int(pb.GetDepth())
	return rInfo
}

func (item *kvItem) to_protobuf() *PBDTableItem {
	rv := &PBDTableItem{
		Key:       item.Key,
		Val:       item.Val,
		Timestamp: proto.Int64(item.timestamp.UnixNano()),
		KeyHash:   item.keyHash,
		Commited:  proto.Bool(item.commited),
	}
	if item.replicaInfo != nil {
		rv.ReplicaInfo = item.replicaInfo.to_protobuf()
	}
	return rv
}

func (item *kvItem) from_protobuf(pb *PBDTableItem) {
	item.Key = pb.GetKey()
	item.Val = pb.GetVal()
	item.timestamp = time.Unix(0, pb.GetTimestamp())
	item.keyHash = pb.GetKeyHash()
	item.commited = pb.GetCommited()
	item.replicaInfo = replicaInfo_from_protobuf(pb.GetReplicaInfo())
}

func (item *kvItem) to_demoted(new_master *dendrite.Vnode) *demotedKvItem {
	rv := new(demotedKvItem)
	rv.item = item.dup()
	rv.new_master = new_master
	rv.demoted_ts = time.Now()
	return rv
}

func (item *kvItem) numActiveReplicas() int {
	if item.replicaInfo == nil {
		return 0
	}
	rv := 0
	for _, r := range item.replicaInfo.vnodes {
		if r != nil {
			rv++
		}
	}
	return rv
}

func (item *kvItem) dup() *kvItem {
	new_item := new(kvItem)
	new_item.timestamp = item.timestamp
	new_item.commited = item.commited
	new_item.lock = item.lock

	new_item.Key = make([]byte, len(item.Key))
	copy(new_item.Key, item.Key)

	new_item.Val = make([]byte, len(item.Val))
	copy(new_item.Val, item.Val)

	new_item.keyHash = make([]byte, len(item.keyHash))
	copy(new_item.keyHash, item.keyHash)

	if item.replicaInfo != nil {
		new_item.replicaInfo = new(kvReplicaInfo)
		new_item.replicaInfo.master = item.replicaInfo.master
		new_item.replicaInfo.vnodes = make([]*dendrite.Vnode, len(item.replicaInfo.vnodes))
		new_item.replicaInfo.orphan_vnodes = make([]*dendrite.Vnode, len(item.replicaInfo.orphan_vnodes))
		copy(new_item.replicaInfo.vnodes, item.replicaInfo.vnodes)
		copy(new_item.replicaInfo.orphan_vnodes, item.replicaInfo.orphan_vnodes)
		new_item.replicaInfo.state = item.replicaInfo.state
		new_item.replicaInfo.depth = item.replicaInfo.depth
	} else {
		new_item.replicaInfo = nil
	}
	return new_item
}

type LogLevel int

const (
	LogNull  LogLevel = 0
	LogInfo  LogLevel = 1
	LogDebug LogLevel = 2
)

func (dt *DTable) Logf(level LogLevel, format string, v ...interface{}) {
	if level == LogNull {
		return
	}
	var new_format string
	if level == LogInfo {
		new_format = "[DTABLE][INFO] " + format
	} else if level == LogDebug {
		new_format = "[DTABLE][DEBUG] " + format
	}

	if dt.confLogLevel == LogDebug {
		log.Printf(new_format, v...)
	} else if dt.confLogLevel == LogInfo && level == LogInfo {
		log.Printf(new_format, v...)
	}
}

func (dt *DTable) Logln(level LogLevel, v ...interface{}) {
	if level == LogNull {
		return
	}

	var new_format string
	if level == LogInfo {
		new_format = "[DTABLE][INFO]"
	} else if level == LogDebug {
		new_format = "[DTABLE][DEBUG]"
	}
	if dt.confLogLevel == LogDebug {
		v = append([]interface{}{new_format}, v...)
		log.Println(v...)
	} else if dt.confLogLevel == LogInfo && level == LogInfo {
		v = append([]interface{}{new_format}, v...)
		log.Println(v...)
	}
}
