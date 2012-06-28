#!/bin/bash

# TODO: Get CURATOR_DIRECTORY, CURRENT_TOOL_SCRIPT, and PORT variables

cd $CURATOR_DIRECTORY/dist
# Launch the annotation tool
./bin/$CURRENT_TOOL_SCRIPT.sh -p $PORT >& logs/$CURRENT_TOOL_SCRIPT.log &

# Remember what process ID it gives back (for later killing)?