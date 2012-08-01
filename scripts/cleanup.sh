#!/bin/bash

# This is a script designed to clean up the shared
# Curator directories after a Hadoop job.
# It is really, really specific to the implementation we're
# using on the Altocumulus cluster at UIUC.

# For each Curator directory . . .
# Delete the "Curator lock" file
CURATOR_LOCK="CURATOR_IS_IN_USE"
BASE=/project/cogcomp/curator-0.6.9_
for i in {1..32}
do
    CMD="rm $BASE$i/$CURATOR_LOCK"
    echo "Running command $CMD"
done
