#!/bin/bash

node_name = {"node1", "node2", "node3", "node4", "node5"} 
user=ncsgroup
streamlec_path=/home/ncsgroup/StreamLEC

for name in ${node_name[*]}
do
scp -r $streamlec_path $name:~
done

