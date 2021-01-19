#!/bin/bash

node_name = {"testbed-node41", "testbed-node42", "testbed-node43", "testbed-node44", "testbed-node45"}
codestream_path=/home/ncsgroup/CodeStream

for name in ${node_name[*]}
do
ssh $name  "cd  ${codestream_path}/script && ./stop.sh"
done
