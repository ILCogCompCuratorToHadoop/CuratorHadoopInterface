#!/bin/bash

# This is a script designed to clean up the shared
# Curator directories after a Hadoop job.
# It is really, really specific to the implementation we're
# using on the Altocumulus cluster at UIUC.

cd /project/cogcomp

# For each Curator directory . . .
# Delete the "Curator lock" file
CURATOR_LOCK="CURATOR_IS_IN_USE"
cd curator-0.6.9_0
rm $CURATOR_LOCK
cd ../curator-0.6.9_1
rm $CURATOR_LOCK
cd ../curator-0.6.9_2
rm $CURATOR_LOCK
