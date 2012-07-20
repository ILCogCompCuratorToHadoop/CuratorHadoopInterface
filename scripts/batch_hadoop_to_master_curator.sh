#!/bin/bash

# Usage (after configuring the directory variables below):
#    ./batch_job_master_script ANNOTATION_TOOL_TO_RUN path/to/your_input_directory raw
# Example: ./batch_hadoop_to_master_curator.sh TOKENIZER /shared/gargamel/undergrad/tyoun/curator-0.6.9/dist/client/job123 raw
# Example: ./batch_job_master_script.sh POS /shared/gargamel/undergrad/tyoun/hadoop-1.0.3/serialized_output/ serial

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

MSG_COLOR='\e[0;36m'     # Cyan. Might also try dark gray (1;30), green
                         # (0;32), or light green (1;32).
DEFAULT_COLOR='\e[0m'    # Reset to normal
ERROR_COLOR='\e[0;31m'

#########################################################################
#                       No need to edit below here                      #

set +e # Do *not* exit the script if a command fails (so we can give
       # useful suggestions to the user)

# When the MapReduce job finishes, copy the data back to local disk
# TODO: Make this a distributed Hadoop job
echo -e "$MSG_COLOR Copying the results of the MapReduce job back to the local machine$DEFAULT_COLOR"
./bin/hadoop fs -copyToLocal serialized_output $OUTPUT
# If the copy to local failed . . . 
if [ "$?"-ne 0 ]
then
   echo -e "$MSG_COLOR\nCopying to local failed. Try fixing the error, then executing: $COMMAND$DEFAULT_COLOR"
   exit 1
fi 
echo -e "$MSG_COLOR\nCopying to local succeeded. $DEFAULT_COLOR"

set -e # Exit the script if a command fails


# Have Master Curator read in the updated Records and update the database accordingly
echo -e "$MSG_COLOR\n\n\nRe-launching the master Curator. $DEFAULT_COLOR"
cd $CURATOR_DIRECTORY/dist
./bin/curator.sh --annotators configs/annotators-empty.xml --port 9010 --threads 10 >& logs/curator.log & >/dev/null
sleep 3s
cd client
./runclient.sh -host localhost -port 9010 -in $OUTPUT/serialized_output -mode POST $TESTING 
echo -e "$MSG_COLOR\n\nShutting down the master Curator. $DEFAULT_COLOR"
# Kill the Curator server
jps -l | grep edu.illinois.cs.cogcomp.curator.CuratorServer | cut -d ' ' -f 1 | xargs -n1 kill

# TODO New Hadoop job:
#       Have the Hadoop nodes kill the running annotator, Curator, and 
#       Curator Client processes
# In order to do this, launch the HadoopInterface with parameter '-cleanup'

exit 0