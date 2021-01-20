#!/bin/bash

if [ "$#" -le "2" ]
then
	echo "Require parameters./run \$1 \$2 \$3 [\$4]"
	echo "\$1 represents applications location"
	echo "\$2 represents worker type"
	echo "\$3 represents output label"
	echo "\$4 represents worker id, optional!"
	exit 1
fi

if [ "$2" == "Processor" ]
then
	nohup $1/processor ./$1/sample.ini Processor$4 0 > ./output/$3_local$4.txt 2>&1 &
elif [ "$2" == "Encoder" ]
then
	nohup $1/sink ./$1/sample.ini Encoder > ./output/$3_encoder.txt 2>&1 &
elif [ "$2" == "Decoder" ]
then
	nohup $1/source ./$1/sample.ini Decoder > ./output/$3_decoder.txt 2>&1 &
fi
