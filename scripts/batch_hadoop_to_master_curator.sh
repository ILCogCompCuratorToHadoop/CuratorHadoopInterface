#!/bin/bash

# Usage: ./script_name ANNOTATION_TOOL_TO_RUN path/to/your_input_directory
# Example: ./batch_master_curator_to_hadoop TOKENIZER /shared/gargamel/undergrad/tyoun/curator-0.6.9/dist/client/job123

echo ""
echo "In order to use this script, you must open it in a text"
echo "editor and configure the location of your local Curator"
echo "and your Hadoop directory."
echo ""
echo "Note also that when you run this script, the Hadoop name node"
echo "(i.e., the thing that controls the Hadoop cluster) must "
echo "already be running."
echo ""


#       Change these variables to the appropriate *absolute paths*      #
#                Should match exactly the same variables in             #
#                   batch_master_curator_to_hadoop.sh                   #
#########################################################################

CURATOR_DIRECTORY=/shared/gargamel/undergrad/tyoun/curator-0.6.9
#HADOOP_DIRECTORY=/shared/gargamel/undergrad/tyoun/hadoop-1.0.3
#INTERMEDIATE_OUTPUT=$HADOOP_DIRECTORY/serialized
OUTPUT=/shared/gargamel/undergrad/tyoun/hadoop-1.0.3
#ANNOTATION_TOOL_TO_RUN=$1       # The 1st parameter from the command line
#INPUT_PATH=$2                   # The 2nd parameter from the command line
TESTING=$3

#########################################################################
#                       No need to edit below here                      #

set +e # Do *not* exit the script if a command fails (so we can give
       # useful suggestions to the user)

# When the MapReduce job finishes, copy the data back to local disk
# TODO: Make this a distributed Hadoop job
echo "Copying the results of the MapReduce job back to the local machine"
COMMAND="./bin/hadoop fs -copyToLocal serialized_output $OUTPUT"
$COMMAND
# If the copy to local failed . . . 
if [ "$?"-ne 0]; then echo "Copying to local failed. Try fixing the error, then executing: $COMMAND"; exit 1; fi 

set -e

# Have Master Curator read in the updated Records and update the database accordingly
cd $CURATOR_DIRECTORY/dist/client
./runclient.sh -host localhost -port 9010 -in $OUTPUT/serialized_output -mode POST $TESTING 

# TODO New Hadoop job:
#       Have the Hadoop nodes kill the running annotator, Curator, and 
#       Curator Client processes

exit 0