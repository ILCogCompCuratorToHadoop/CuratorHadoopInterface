#!/bin/bash

# Usage (after configuring the directory variables below):
#    ./batch_job_master_script ANNOTATION_TOOL_TO_RUN path/to/your_input_directory raw
# Example: ./batch_job_master_script.sh TOKENIZER /shared/gargamel/undergrad/tyoun/curator-0.6.9/dist/client/job123 raw
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
#########################################################################

CURATOR_DIRECTORY=/shared/gargamel/undergrad/tyoun/curator-0.6.9
HADOOP_DIRECTORY=/shared/gargamel/undergrad/tyoun/hadoop-1.0.3
INTERMEDIATE_OUTPUT=$HADOOP_DIRECTORY/serialized
# In the output directory, we will place a dir called "serialized" which
# will store the job's output records
OUTPUT=/shared/gargamel/undergrad/tyoun/hadoop-1.0.3
ANNOTATION_TOOL_TO_RUN=$1       # The 1st parameter from the command line
INPUT_PATH=$2                   # The 2nd parameter from the command line
MODE=$3                         # 3rd parameter from CL should be "serial" 
                                #   or "raw"
TESTING=$4                      # 4th parameter from CL should be "-test"
                                #   (no quotes) to run in test mode.



MSG_COLOR='\e[0;36m'     # Cyan. Might also try dark gray (1;30), green
                         # (0;32), or light green (1;32).
DEFAULT_COLOR='\e[0m'    # Reset to normal
ERROR_COLOR='\e[0;31m'

#########################################################################
#                       No need to edit below here                      #

BASEDIR=$(dirname $0) # location of this script file

set -e # Exit the script if any command fails

echo -e "$MSG_COLOR\n\n\nYou said your copy of Curator is located here: $CURATOR_DIRECTORY"
echo "You requested we run the annotation tool $ANNOTATION_TOOL_TO_RUN on your input"
echo "You requested we annotate the input text files located here: $INPUT_PATH"
echo -e "\t(That input directory should be an *absolute* path.) $DEFAULT_COLOR"

# Make sure a proper mode was given
if [ "$MODE" != "serial" ]; then
    if [ "$MODE" != "raw" ]; then
	echo -e "$ERROR_COLOR\n\n\n\tPlease launch this script with the "
	echo -e "\tproper mode (either 'raw', for new documents, or 'serial'"
	echo -e "\tfor documents already in serialized Record form, as you"
	echo -e "\tmight get from a previous Hadoop job)."
	exit 1;
    fi
fi


if [ "$MODE" != "serial" ]; then # if we're working with raw text files...
    # Launch the Master Curator
    echo -e "$MSG_COLOR\n\n\nLaunching the master curator. $DEFAULT_COLOR"
    # First make sure we have the "empty" annotators list
    cd $CURATOR_DIRECTORY/dist/configs
    if [ ! -f annotators-empty.xml ]; then # Does the file exist?
	touch annotators-empty.xml
	echo "<?xml version=\"1.0\" encoding=\"utf-8\" ?>" >> annotators-empty.xml
	echo "<curator-annotators><annotator>" >> annotators-empty.xml
	echo "<type>multilabeler</type>" >> annotators-empty.xml
	echo "<field>sentences</field>" >> annotators-empty.xml
	echo "<field>tokens</field>" >> annotators-empty.xml
	echo "<local>edu.illinois.cs.cogcomp.annotation.handler.IllinoisTokenizerHandler</local>" >> annotators-empty.xml
	echo "</annotator></curator-annotators>" >> annotators-empty.xml
    fi
    cd $CURATOR_DIRECTORY/dist
    ./bin/curator.sh --annotators configs/annotators-empty.xml --port 9010 --threads 10 >& logs/curator.log & >/dev/null
    sleep 3s


    # Launch the Master Curator Client, asking it to serialize the
    # records from the text in the input directory
    echo -e "$MSG_COLOR\n\n\nLaunching the master curator client:$DEFAULT_COLOR"
    cd client
    ./runclient.sh -host localhost -port 9010 -in $INPUT_PATH -out $INTERMEDIATE_OUTPUT -mode PRE $TESTING 

    echo -e "$MSG_COLOR\n\nShutting down locally running Curator. $DEFAULT_COLOR"
    # Get list of currently running Java procs | find the Curator server | split on spaces (?) | send the first thing (i.e., the process ID) to the kill command
    jps -l | grep edu.illinois.cs.cogcomp.curator.CuratorServer | cut -d ' ' -f 1 | xargs -n1 kill

    # Copy the serialized records to the Hadoop Distributed File System (HDFS)
    echo -e "$MSG_COLOR\n\n\nCopying the serialized records to HDFS: $DEFAULT_COLOR"
    cd $HADOOP_DIRECTORY
    set +e # Do *not* exit the script if any command fails
    ./bin/hadoop dfs -rmr serialized
    ./bin/hadoop dfs -mkdir serialized
    set -e # Exit the script if any command fails
    ./bin/hadoop dfs -copyFromLocal $INTERMEDIATE_OUTPUT/* serialized
    echo -e "$MSG_COLOR\nCopied successfully. $DEFAULT_COLOR"


# Else we are working with already serialized records, not raw text:
else
    # Copy the input directory, which we assume to have only serialized
    # records, to the cluster
    echo -e "$MSG_COLOR\n\n\nCopying the serialized records to HDFS: $DEFAULT_COLOR"
    cd $HADOOP_DIRECTORY
    set +e # Do *not* exit the script if any command fails
    ./bin/hadoop dfs -rmr serialized
    ./bin/hadoop dfs -mkdir serialized
    
    # If this came from a MapReduce job, let's not copy the MR output back
    if [ -f $INPUT_PATH/mapreduce_out ]; then
	rm -r $INPUT_PATH/mapreduce_out
    fi

    set -e # Exit the script if any command fails
    ./bin/hadoop dfs -copyFromLocal $INPUT_PATH/* serialized
    echo -e "$MSG_COLOR\nCopied successfully. $DEFAULT_COLOR"
fi


# Launch MapReduce job on Hadoop cluster
echo -e "$MSG_COLOR\n\n\nLaunching the mapreduce job on the Hadoop cluster: $DEFAULT_COLOR"
HADOOP_OUTPUT=$ANNOTATION_TOOL_TO_RUN"_output"
./bin/hadoop jar curator.jar edu.illinois.cs.cogcomp.hadoopinterface.HadoopInterface -d serialized -m $ANNOTATION_TOOL_TO_RUN -out $HADOOP_OUTPUT
echo -e "$MSG_COLOR\n\n\nJob finished!\n\n$DEFAULT_COLOR"


set +e # Do *not* exit the script if a command fails (so we can give
       # useful suggestions to the user)

# When the MapReduce job finishes, copy the data back to local disk
# TODO: Make this a distributed Hadoop job
echo -e "$MSG_COLOR Copying the results of the MapReduce job back to the local machine$DEFAULT_COLOR"
./bin/hadoop fs -copyToLocal $HADOOP_OUTPUT $OUTPUT
# If the copy to local failed . . . 
if [[ $? -ne 0 ]] ; then
   echo -e "$MSG_COLOR\nCopying to local failed. Try fixing the error, then executing: ./bin/hadoop fs -copyToLocal serialized_output $OUTPUT $DEFAULT_COLOR"
   exit 1
fi 
echo -e "$MSG_COLOR\nCopying to local succeeded. $DEFAULT_COLOR"

set -e

# Have Master Curator read in the updated Records and update the database accordingly
echo -e "$MSG_COLOR\n\n\nRe-launching the master Curator. $DEFAULT_COLOR"
cd $CURATOR_DIRECTORY/dist
./bin/curator.sh --annotators configs/annotators-empty.xml --port 9010 --threads 10 >& logs/curator.log & >/dev/null
sleep 3s
cd client
./runclient.sh -host localhost -port 9010 -in $OUTPUT/$HADOOP_OUTPUT -mode POST $TESTING 
echo -e "$MSG_COLOR\n\nShutting down the master Curator. $DEFAULT_COLOR"
jps -l | grep edu.illinois.cs.cogcomp.curator.CuratorServer | cut -d ' ' -f 1 | xargs -n1 kill



# New Hadoop job:
#       Have the Hadoop nodes kill the running annotator, Curator, and 
#       Curator Client processes

exit 0
