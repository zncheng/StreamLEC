#!/bin/bash

node_name = {"testbed-node41", "testbed-node42", "testbed-node43", "testbed-node44", "testbed-node45"} 
user=ncsgroup
codestream_path=/home/ncsgroup/CodeStream

for name in ${node_name[*]}
do
scp -r $codestream_path $name:~
done

