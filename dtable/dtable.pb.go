package dtable

import proto "code.google.com/p/goprotobuf/proto"
import math "math"
import dendrite "github.com/fastfn/dendrite"

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = math.Inf

// PBDTableResponse is a generic response structure with error indication.
type PBDTableResponse struct {
	Ok               *bool   `protobuf:"varint,1,req,name=ok" json:"ok,omitempty"`
	Error            *string `protobuf:"bytes,2,opt,name=error" json:"error,omitempty"`
	XXX_unrecognized []byte  `json:"-"`
}

func (m *PBDTableResponse) Reset()         { *m = PBDTableResponse{} }
func (m *PBDTableResponse) String() string { return proto.CompactTextString(m) }
func (*PBDTableResponse) ProtoMessage()    {}

func (m *PBDTableResponse) GetOk() bool {
	if m != nil && m.Ok != nil {
		return *m.Ok
	}
	return false
}

func (m *PBDTableResponse) GetError() string {
	if m != nil && m.Error != nil {
		return *m.Error
	}
	return ""
}

// PBDTableStatus is a message to request the status of remote vnode.
type PBDTableStatus struct {
	Dest             *dendrite.PBProtoVnode `protobuf:"bytes,1,req,name=dest" json:"dest,omitempty"`
	XXX_unrecognized []byte                 `json:"-"`
}

func (m *PBDTableStatus) Reset()         { *m = PBDTableStatus{} }
func (m *PBDTableStatus) String() string { return proto.CompactTextString(m) }
func (*PBDTableStatus) ProtoMessage()    {}

func (m *PBDTableStatus) GetDest() *dendrite.PBProtoVnode {
	if m != nil {
		return m.Dest
	}
	return nil
}

// PBDTableReplicaInfo message represents kvItem's replicaInfo structure.
type PBDTableReplicaInfo struct {
	Master           *dendrite.PBProtoVnode   `protobuf:"bytes,1,opt,name=master" json:"master,omitempty"`
	Vnodes           []*dendrite.PBProtoVnode `protobuf:"bytes,2,rep,name=vnodes" json:"vnodes,omitempty"`
	OrphanVnodes     []*dendrite.PBProtoVnode `protobuf:"bytes,3,rep,name=orphanVnodes" json:"orphanVnodes,omitempty"`
	State            *int32                   `protobuf:"varint,4,opt,name=state" json:"state,omitempty"`
	Depth            *int32                   `protobuf:"varint,5,opt,name=depth" json:"depth,omitempty"`
	XXX_unrecognized []byte                   `json:"-"`
}

func (m *PBDTableReplicaInfo) Reset()         { *m = PBDTableReplicaInfo{} }
func (m *PBDTableReplicaInfo) String() string { return proto.CompactTextString(m) }
func (*PBDTableReplicaInfo) ProtoMessage()    {}

func (m *PBDTableReplicaInfo) GetMaster() *dendrite.PBProtoVnode {
	if m != nil {
		return m.Master
	}
	return nil
}

func (m *PBDTableReplicaInfo) GetVnodes() []*dendrite.PBProtoVnode {
	if m != nil {
		return m.Vnodes
	}
	return nil
}

func (m *PBDTableReplicaInfo) GetOrphanVnodes() []*dendrite.PBProtoVnode {
	if m != nil {
		return m.OrphanVnodes
	}
	return nil
}

func (m *PBDTableReplicaInfo) GetState() int32 {
	if m != nil && m.State != nil {
		return *m.State
	}
	return 0
}

func (m *PBDTableReplicaInfo) GetDepth() int32 {
	if m != nil && m.Depth != nil {
		return *m.Depth
	}
	return 0
}

// PBDTableItem message represents kvItem's structure.
type PBDTableItem struct {
	Key              []byte                 `protobuf:"bytes,1,opt,name=key" json:"key,omitempty"`
	Val              []byte                 `protobuf:"bytes,2,opt,name=val" json:"val,omitempty"`
	Timestamp        *int64                 `protobuf:"varint,3,opt,name=timestamp" json:"timestamp,omitempty"`
	Commited         *bool                  `protobuf:"varint,4,opt,name=commited" json:"commited,omitempty"`
	KeyHash          []byte                 `protobuf:"bytes,5,opt,name=keyHash" json:"keyHash,omitempty"`
	ReplicaInfo      *PBDTableReplicaInfo   `protobuf:"bytes,6,opt,name=replicaInfo" json:"replicaInfo,omitempty"`
	Origin           *dendrite.PBProtoVnode `protobuf:"bytes,7,opt,name=origin" json:"origin,omitempty"`
	Found            *bool                  `protobuf:"varint,8,opt,name=found" json:"found,omitempty"`
	XXX_unrecognized []byte                 `json:"-"`
}

func (m *PBDTableItem) Reset()         { *m = PBDTableItem{} }
func (m *PBDTableItem) String() string { return proto.CompactTextString(m) }
func (*PBDTableItem) ProtoMessage()    {}

func (m *PBDTableItem) GetKey() []byte {
	if m != nil {
		return m.Key
	}
	return nil
}

func (m *PBDTableItem) GetVal() []byte {
	if m != nil {
		return m.Val
	}
	return nil
}

func (m *PBDTableItem) GetTimestamp() int64 {
	if m != nil && m.Timestamp != nil {
		return *m.Timestamp
	}
	return 0
}

func (m *PBDTableItem) GetCommited() bool {
	if m != nil && m.Commited != nil {
		return *m.Commited
	}
	return false
}

func (m *PBDTableItem) GetKeyHash() []byte {
	if m != nil {
		return m.KeyHash
	}
	return nil
}

func (m *PBDTableItem) GetReplicaInfo() *PBDTableReplicaInfo {
	if m != nil {
		return m.ReplicaInfo
	}
	return nil
}

func (m *PBDTableItem) GetOrigin() *dendrite.PBProtoVnode {
	if m != nil {
		return m.Origin
	}
	return nil
}

func (m *PBDTableItem) GetFound() bool {
	if m != nil && m.Found != nil {
		return *m.Found
	}
	return false
}

// PBDTableDemotedItem message represents demotedItem's structure.
type PBDTableDemotedItem struct {
	Dest             *dendrite.PBProtoVnode `protobuf:"bytes,1,req,name=dest" json:"dest,omitempty"`
	Item             *PBDTableItem          `protobuf:"bytes,2,req,name=item" json:"item,omitempty"`
	Origin           *dendrite.PBProtoVnode `protobuf:"bytes,3,opt,name=origin" json:"origin,omitempty"`
	XXX_unrecognized []byte                 `json:"-"`
}

func (m *PBDTableDemotedItem) Reset()         { *m = PBDTableDemotedItem{} }
func (m *PBDTableDemotedItem) String() string { return proto.CompactTextString(m) }
func (*PBDTableDemotedItem) ProtoMessage()    {}

func (m *PBDTableDemotedItem) GetDest() *dendrite.PBProtoVnode {
	if m != nil {
		return m.Dest
	}
	return nil
}

func (m *PBDTableDemotedItem) GetItem() *PBDTableItem {
	if m != nil {
		return m.Item
	}
	return nil
}

func (m *PBDTableDemotedItem) GetOrigin() *dendrite.PBProtoVnode {
	if m != nil {
		return m.Origin
	}
	return nil
}

// PBDTableMultiItemResponse is a response message used to send multiple kvItems to the caller.
type PBDTableMultiItemResponse struct {
	Origin           *dendrite.PBProtoVnode `protobuf:"bytes,1,opt,name=origin" json:"origin,omitempty"`
	Items            []*PBDTableItem        `protobuf:"bytes,2,rep,name=items" json:"items,omitempty"`
	XXX_unrecognized []byte                 `json:"-"`
}

func (m *PBDTableMultiItemResponse) Reset()         { *m = PBDTableMultiItemResponse{} }
func (m *PBDTableMultiItemResponse) String() string { return proto.CompactTextString(m) }
func (*PBDTableMultiItemResponse) ProtoMessage()    {}

func (m *PBDTableMultiItemResponse) GetOrigin() *dendrite.PBProtoVnode {
	if m != nil {
		return m.Origin
	}
	return nil
}

func (m *PBDTableMultiItemResponse) GetItems() []*PBDTableItem {
	if m != nil {
		return m.Items
	}
	return nil
}

// PBDTableGetItem is a request message used to get an item from remote vnode.
type PBDTableGetItem struct {
	Dest             *dendrite.PBProtoVnode `protobuf:"bytes,1,req,name=dest" json:"dest,omitempty"`
	KeyHash          []byte                 `protobuf:"bytes,2,req,name=keyHash" json:"keyHash,omitempty"`
	Origin           *dendrite.PBProtoVnode `protobuf:"bytes,3,opt,name=origin" json:"origin,omitempty"`
	XXX_unrecognized []byte                 `json:"-"`
}

func (m *PBDTableGetItem) Reset()         { *m = PBDTableGetItem{} }
func (m *PBDTableGetItem) String() string { return proto.CompactTextString(m) }
func (*PBDTableGetItem) ProtoMessage()    {}

func (m *PBDTableGetItem) GetDest() *dendrite.PBProtoVnode {
	if m != nil {
		return m.Dest
	}
	return nil
}

func (m *PBDTableGetItem) GetKeyHash() []byte {
	if m != nil {
		return m.KeyHash
	}
	return nil
}

func (m *PBDTableGetItem) GetOrigin() *dendrite.PBProtoVnode {
	if m != nil {
		return m.Origin
	}
	return nil
}

// PBDTableSetItem is a request message used to set an item to remote vnode.
type PBDTableSetItem struct {
	Dest             *dendrite.PBProtoVnode `protobuf:"bytes,1,req,name=dest" json:"dest,omitempty"`
	Item             *PBDTableItem          `protobuf:"bytes,2,req,name=item" json:"item,omitempty"`
	Origin           *dendrite.PBProtoVnode `protobuf:"bytes,3,opt,name=origin" json:"origin,omitempty"`
	Demoting         *bool                  `protobuf:"varint,4,opt,name=demoting" json:"demoting,omitempty"`
	MinAcks          *int32                 `protobuf:"varint,5,opt,name=minAcks" json:"minAcks,omitempty"`
	XXX_unrecognized []byte                 `json:"-"`
}

func (m *PBDTableSetItem) Reset()         { *m = PBDTableSetItem{} }
func (m *PBDTableSetItem) String() string { return proto.CompactTextString(m) }
func (*PBDTableSetItem) ProtoMessage()    {}

func (m *PBDTableSetItem) GetDest() *dendrite.PBProtoVnode {
	if m != nil {
		return m.Dest
	}
	return nil
}

func (m *PBDTableSetItem) GetItem() *PBDTableItem {
	if m != nil {
		return m.Item
	}
	return nil
}

func (m *PBDTableSetItem) GetOrigin() *dendrite.PBProtoVnode {
	if m != nil {
		return m.Origin
	}
	return nil
}

func (m *PBDTableSetItem) GetDemoting() bool {
	if m != nil && m.Demoting != nil {
		return *m.Demoting
	}
	return false
}

func (m *PBDTableSetItem) GetMinAcks() int32 {
	if m != nil && m.MinAcks != nil {
		return *m.MinAcks
	}
	return 0
}

// PBDTableSetMultiItem is a request message used to set multiple items on remote vnode.
type PBDTableSetMultiItem struct {
	Dest             *dendrite.PBProtoVnode `protobuf:"bytes,1,req,name=dest" json:"dest,omitempty"`
	Origin           *dendrite.PBProtoVnode `protobuf:"bytes,2,opt,name=origin" json:"origin,omitempty"`
	Items            []*PBDTableItem        `protobuf:"bytes,3,rep,name=items" json:"items,omitempty"`
	XXX_unrecognized []byte                 `json:"-"`
}

func (m *PBDTableSetMultiItem) Reset()         { *m = PBDTableSetMultiItem{} }
func (m *PBDTableSetMultiItem) String() string { return proto.CompactTextString(m) }
func (*PBDTableSetMultiItem) ProtoMessage()    {}

func (m *PBDTableSetMultiItem) GetDest() *dendrite.PBProtoVnode {
	if m != nil {
		return m.Dest
	}
	return nil
}

func (m *PBDTableSetMultiItem) GetOrigin() *dendrite.PBProtoVnode {
	if m != nil {
		return m.Origin
	}
	return nil
}

func (m *PBDTableSetMultiItem) GetItems() []*PBDTableItem {
	if m != nil {
		return m.Items
	}
	return nil
}

// PBDTableClearReplica is a request message used to remove replicated item from remote vnode.
type PBDTableClearReplica struct {
	Dest             *dendrite.PBProtoVnode `protobuf:"bytes,1,req,name=dest" json:"dest,omitempty"`
	KeyHash          []byte                 `protobuf:"bytes,2,req,name=keyHash" json:"keyHash,omitempty"`
	Demoted          *bool                  `protobuf:"varint,3,req,name=demoted" json:"demoted,omitempty"`
	Origin           *dendrite.PBProtoVnode `protobuf:"bytes,4,opt,name=origin" json:"origin,omitempty"`
	XXX_unrecognized []byte                 `json:"-"`
}

func (m *PBDTableClearReplica) Reset()         { *m = PBDTableClearReplica{} }
func (m *PBDTableClearReplica) String() string { return proto.CompactTextString(m) }
func (*PBDTableClearReplica) ProtoMessage()    {}

func (m *PBDTableClearReplica) GetDest() *dendrite.PBProtoVnode {
	if m != nil {
		return m.Dest
	}
	return nil
}

func (m *PBDTableClearReplica) GetKeyHash() []byte {
	if m != nil {
		return m.KeyHash
	}
	return nil
}

func (m *PBDTableClearReplica) GetDemoted() bool {
	if m != nil && m.Demoted != nil {
		return *m.Demoted
	}
	return false
}

func (m *PBDTableClearReplica) GetOrigin() *dendrite.PBProtoVnode {
	if m != nil {
		return m.Origin
	}
	return nil
}

// PBDTableSetReplicaInfo is a request message used to update metadata for replicated item on remote vnode.
type PBDTableSetReplicaInfo struct {
	Dest             *dendrite.PBProtoVnode `protobuf:"bytes,1,req,name=dest" json:"dest,omitempty"`
	KeyHash          []byte                 `protobuf:"bytes,2,req,name=keyHash" json:"keyHash,omitempty"`
	ReplicaInfo      *PBDTableReplicaInfo   `protobuf:"bytes,3,req,name=replicaInfo" json:"replicaInfo,omitempty"`
	Origin           *dendrite.PBProtoVnode `protobuf:"bytes,4,opt,name=origin" json:"origin,omitempty"`
	XXX_unrecognized []byte                 `json:"-"`
}

func (m *PBDTableSetReplicaInfo) Reset()         { *m = PBDTableSetReplicaInfo{} }
func (m *PBDTableSetReplicaInfo) String() string { return proto.CompactTextString(m) }
func (*PBDTableSetReplicaInfo) ProtoMessage()    {}

func (m *PBDTableSetReplicaInfo) GetDest() *dendrite.PBProtoVnode {
	if m != nil {
		return m.Dest
	}
	return nil
}

func (m *PBDTableSetReplicaInfo) GetKeyHash() []byte {
	if m != nil {
		return m.KeyHash
	}
	return nil
}

func (m *PBDTableSetReplicaInfo) GetReplicaInfo() *PBDTableReplicaInfo {
	if m != nil {
		return m.ReplicaInfo
	}
	return nil
}

func (m *PBDTableSetReplicaInfo) GetOrigin() *dendrite.PBProtoVnode {
	if m != nil {
		return m.Origin
	}
	return nil
}

// PBDTablePromoteKey is a request message used to request a promotion of a key on the remote vnode.
type PBDTablePromoteKey struct {
	Dest             *dendrite.PBProtoVnode `protobuf:"bytes,1,req,name=dest" json:"dest,omitempty"`
	Item             *PBDTableItem          `protobuf:"bytes,2,req,name=item" json:"item,omitempty"`
	Origin           *dendrite.PBProtoVnode `protobuf:"bytes,3,opt,name=origin" json:"origin,omitempty"`
	XXX_unrecognized []byte                 `json:"-"`
}

func (m *PBDTablePromoteKey) Reset()         { *m = PBDTablePromoteKey{} }
func (m *PBDTablePromoteKey) String() string { return proto.CompactTextString(m) }
func (*PBDTablePromoteKey) ProtoMessage()    {}

func (m *PBDTablePromoteKey) GetDest() *dendrite.PBProtoVnode {
	if m != nil {
		return m.Dest
	}
	return nil
}

func (m *PBDTablePromoteKey) GetItem() *PBDTableItem {
	if m != nil {
		return m.Item
	}
	return nil
}

func (m *PBDTablePromoteKey) GetOrigin() *dendrite.PBProtoVnode {
	if m != nil {
		return m.Origin
	}
	return nil
}

func init() {
}
