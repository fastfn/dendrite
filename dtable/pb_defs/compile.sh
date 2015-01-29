#!/bin/bash
protoc --proto_path "../../pb_defs:." --go_out=. dtable.proto
