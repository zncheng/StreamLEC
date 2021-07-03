#!/bin/bash

node_name = {"node1", "node2", "node3", "node4", "node5"}
user=streamlec
streamlec_path=/home/StreamLEC


ssh ${node_name[0]}  "cd  ${streamlec_path}/script && ./run.sh Encoder ${streamlec_path}/apps/sample/smple.ini Encoder"
count=1
for i in {1..4}
do
ssh ${node_name[$i]}  "cd  ${streamlec_path}/script && ./run.sh Processor ${streamlec_path}/apps/sample/smple.ini Processor$count"
let count=count+1
do
done


ssh ${node_name[4]}  "cd  ${streamlec_path}/script && ./run.sh Decoder ${streamlec_path}/apps/sample/smple.ini Decoder"

