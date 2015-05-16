package dtable

import (
	"fmt"
	"github.com/fastfn/dendrite"
	"github.com/golang/protobuf/proto"
	"time"
)

func (item *kvItem) keyHashString() string {
	if item.keyHash == nil {
		item.keyHash = dendrite.HashKey(item.Key)
	}
	return fmt.Sprintf("%x", item.keyHash)
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
		pb_master := &dendrite.PBProtoVnode{
			Host: proto.String(item.replicaInfo.master.Host),
			Id:   item.replicaInfo.master.Id,
		}
		pb_vnodes := make([]*dendrite.PBProtoVnode, 0)
		for _, rvn := range item.replicaInfo.vnodes {
			pb_vnodes = append(pb_vnodes, &dendrite.PBProtoVnode{
				Host: proto.String(rvn.Host),
				Id:   rvn.Id,
			})
		}
		pb_orphanVnodes := make([]*dendrite.PBProtoVnode, 0)
		for _, ovn := range item.replicaInfo.orphan_vnodes {
			pb_orphanVnodes = append(pb_orphanVnodes, &dendrite.PBProtoVnode{
				Host: proto.String(ovn.Host),
				Id:   ovn.Id,
			})
		}
		pb_replicaInfo := &PBDTableReplicaInfo{
			Master:       pb_master,
			Vnodes:       pb_vnodes,
			OrphanVnodes: pb_orphanVnodes,
			State:        proto.Int32(int32(item.replicaInfo.state)),
			Depth:        proto.Int32(int32(item.replicaInfo.depth)),
		}
		rv.ReplicaInfo = pb_replicaInfo
	}
	return rv
}

func (item *kvItem) from_protobuf(pb *PBDTableItem) {
	item.Key = pb.GetKey()
	item.Val = pb.GetVal()
	item.timestamp = time.Unix(0, pb.GetTimestamp())
	item.keyHash = pb.GetKeyHash()
	item.commited = pb.GetCommited()
	pb_replicaInfo := pb.GetReplicaInfo()
	if pb_replicaInfo != nil {
		item.replicaInfo = new(kvReplicaInfo)
		item.replicaInfo.master = &dendrite.Vnode{
			Id:   pb_replicaInfo.GetMaster().GetId(),
			Host: pb_replicaInfo.GetMaster().GetHost(),
		}
		item.replicaInfo.vnodes = make([]*dendrite.Vnode, 0)
		for _, pb_vnode := range pb_replicaInfo.GetVnodes() {
			item.replicaInfo.vnodes = append(item.replicaInfo.vnodes, &dendrite.Vnode{
				Id:   pb_vnode.GetId(),
				Host: pb_vnode.GetHost(),
			})
		}
		item.replicaInfo.orphan_vnodes = make([]*dendrite.Vnode, 0)
		for _, pb_orphanVnode := range pb_replicaInfo.GetOrphanVnodes() {
			item.replicaInfo.orphan_vnodes = append(item.replicaInfo.orphan_vnodes, &dendrite.Vnode{
				Id:   pb_orphanVnode.GetId(),
				Host: pb_orphanVnode.GetHost(),
			})
		}
		item.replicaInfo.state = replicaState(int(pb_replicaInfo.GetState()))
		item.replicaInfo.depth = int(pb_replicaInfo.GetDepth())
	}
}

func (item *kvItem) to_demoted(new_master *dendrite.Vnode) *demotedKvItem {
	rv := new(demotedKvItem)
	rv.item = item.dup()
	rv.new_master = new_master
	rv.demoted_ts = time.Now()
	return rv
}
func (item *kvItem) dup() *kvItem {
	new_item := new(kvItem)
	copy(new_item.Key, item.Key)
	copy(new_item.Val, item.Val)
	new_item.timestamp = item.timestamp
	copy(new_item.keyHash, item.keyHash)
	new_item.commited = item.commited
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
