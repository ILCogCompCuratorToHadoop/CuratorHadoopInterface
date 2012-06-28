#!/bin/bash

# TODO: Decide how we define the CURATOR_DIRECTORY and CURRENT_TOOL variables

cd $CURATOR_DIRECTORY/dist
# Launch the Curator in local mode, using an annotator config file that 
# indicates we have just one tool running.
./bin/curator-local.sh --annotators configs/annotators-local-$CURRENT_TOOL.xml --port 9010 --threads 10 >& logs/curator.log &