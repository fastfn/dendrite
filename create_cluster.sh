#!/bin/bash

for port in 5001 5002 5003 5004
do
  dendrite-node -host "127.0.0.1:$port" -nodes "127.0.0.1:5000" &
done


