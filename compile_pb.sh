#!/bin/bash
cd pb_defs
protoc --go_out=../ chord.proto
cd ../dtable/pb_defs
protoc --proto_path "../../pb_defs:." --go_out=../ dtable.proto
sed -i 's/import dendrite \"chord.pb/import dendrite \"github.com\/fastfn\/dendrite/' ../dtable.pb.go

