#!/bin/bash

cd $CURATOR_DIRECTORY/dist
./bin/curator-local.sh --annotators configs/annotators-example.xml --port 9010 --threads 10 >& logs/curator.log &