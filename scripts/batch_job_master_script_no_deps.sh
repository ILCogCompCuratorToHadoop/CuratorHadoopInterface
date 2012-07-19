#!/bin/bash

# Usage: ./batch_job_master_script ANNOTATION_TOOL_TO_RUN path/to/your_input_directory
# Example: ./batch_job_master_script.sh TOKENIZER /shared/gargamel/undergrad/tyoun/curator-0.6.9/dist/client/job123

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
#########################################################################

CURATOR_DIRECTORY=/shared/gargamel/undergrad/tyoun/curator-0.6.9
HADOOP_DIRECTORY=/shared/gargamel/undergrad/tyoun/hadoop-1.0.3
INTERMEDIATE_OUTPUT=$HADOOP_DIRECTORY/serialized
# In the output directory, we will place a dir called "serialized" which
# will store the job's output records
OUTPUT=/shared/gargamel/undergrad/tyoun/hadoop-1.0.3
ANNOTATION_TOOL_TO_RUN=$1       # The 1st parameter from the command line
INPUT_PATH=$2                   # The 2nd parameter from the command line
TESTING=$3                      # 3rd parameter from CL should be "-test"
                                #   (no quotes) to run in test mode.

MSG_COLOR='\e[0;36m'     # Cyan. Might also try dark gray (1;30), green
                         # (0;32), or light green (1;32).
DEFAULT_COLOR='\e[0m'    # Reset to normal

#########################################################################
#                       No need to edit below here                      #


BASEDIR=$(dirname $0) # location of this script file
KILL_CURATOR_COMMAND="jps -l | grep edu.illinois.cs.cogcomp.curator.CuratorServer | cut -d ' ' -f 1 | xargs -n1 kill"
START_CURATOR_COMMAND="cd $CURATOR_DIRECTORY/dist ; ./bin/curator.sh --annotators configs/annotators-empty.xml --port 9010 --threads 10 >& logs/curator.log & >/dev/null"

set -e # Exit the script if any command fails

echo -e "$MSG_COLOR\n\n\nYou said your copy of Curator is located here: $CURATOR_DIRECTORY"
echo "You requested we run the annotation tool $ANNOTATION_TOOL_TO_RUN on your input"
echo "You requested we annotate the input text files located here: $INPUT_PATH"
echo -e "\t(That input directory should be an *absolute* path.) $DEFAULT_COLOR"

# Launch the Master Curator
echo -e "$MSG_COLOR\n\n\nLaunching the master curator. $DEFAULT_COLOR"
# First make sure we have the "empty" annotators list
cd $CURATOR_DIRECTORY/dist/configs
if [ -f annotators-empty.xml ] # Does the file exist?
then
    echo "Annotator list exists for the Curator."
else
    echo "<?xml version=\"1.0\" encoding=\"utf-8\" ?>\n<curator-annotators> </curator-annotators>" > annotators-empty.xml
fi
$START_CURATOR_COMMAND


# Launch the Master Curator Client, asking it to serialize the
# records from the text in the input directory
echo -e "$MSG_COLOR\n\n\nLaunching the master curator client:$DEFAULT_COLOR"
cd client
./runclient.sh -host localhost -port 9010 -in $INPUT_PATH -out $INTERMEDIATE_OUTPUT -mode PRE $TESTING 

echo -e "$MSG_COLOR\n\nShutting down locally running Curator. $DEFAULT_COLOR"
$KILL_CURATOR_COMMAND

# Copy the serialized records to the Hadoop Distributed File System (HDFS)
echo -e "$MSG_COLOR\n\n\nCopying the serialized records to HDFS: $DEFAULT_COLOR"
cd $HADOOP_DIRECTORY
set +e # Do *not* exit the script if any command fails
./bin/hadoop dfs -rmr serialized
./bin/hadoop dfs -mkdir serialized
set -e # Exit the script if any command fails
./bin/hadoop dfs -copyFromLocal $INTERMEDIATE_OUTPUT/* serialized
echo -e "$MSG_COLOR Copied successfully. $DEFAULT_COLOR"

# Launch MapReduce job on Hadoop cluster
echo -e "$MSG_COLOR\n\n\nLaunching the mapreduce job on the Hadoop cluster: $DEFAULT_COLOR"
./bin/hadoop jar curator.jar -d serialized -m $ANNOTATION_TOOL_TO_RUN -out serialized_output
echo -e "$MSG_COLOR\n\n\nJob finished!\n\n$DEFAULT_COLOR"


set +e # Do *not* exit the script if a command fails (so we can give
       # useful suggestions to the user)

# When the MapReduce job finishes, copy the data back to local disk
# TODO: Make this a distributed Hadoop job
echo -e "$MSG_COLOR Copying the results of the MapReduce job back to the local machine$DEFAULT_COLOR"
./bin/hadoop fs -copyToLocal serialized_output $OUTPUT
# If the copy to local failed . . . 
if [ "$?"-ne 0 ]
then
   echo -e "$MSG_COLOR Copying to local failed. Try fixing the error, then executing: $COMMAND$DEFAULT_COLOR"
   exit 1
fi 
echo -e "$MSG_COLOR Copying to local succeeded. $DEFAULT_COLOR"

set -e

# Have Master Curator read in the updated Records and update the database accordingly
echo -e "$MSG_COLOR\n\n\nRe-launching the master curator. $DEFAULT_COLOR"
$START_CURATOR_COMMAND
cd client
./runclient.sh -host localhost -port 9010 -in $OUTPUT/serialized_output -mode POST $TESTING 
$KILL_CURATOR_COMMAND



# New Hadoop job:
#       Have the Hadoop nodes kill the running annotator, Curator, and 
#       Curator Client processes

exit 0
