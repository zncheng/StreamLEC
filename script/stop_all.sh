#!/bin/bash

node_name = {"node1", "node2", "node3", "node", "node5"}
streamlec_path=/home/ncsgroup/CodeStream

for name in ${node_name[*]}
do
ssh $name  "cd  ${streamlec_path}/script && ./stop.sh"
done
