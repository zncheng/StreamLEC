#!/bin/bash

node_name = {"testbed-node41", "testbed-node42", "testbed-node43", "testbed-node44", "testbed-node45"}
user=ncsgroup
codestream_path=/home/ncsgroup/CodeStream


ssh ${node_name[0]}  "cd  ${codestream_path}/script && ./run.sh Encoder ${codestream_path}/apps/sample/smple.ini Encoder"
count=1
for i in {1..4}
do
ssh ${node_name[$i]}  "cd  ${codestream_path}/script && ./run.sh Processor ${codestream_path}/apps/sample/smple.ini Processor$count"
let count=count+1
do
done


ssh ${node_name[4]}  "cd  ${codestream_path}/script && ./run.sh Decoder ${codestream_path}/apps/sample/smple.ini Decoder"

