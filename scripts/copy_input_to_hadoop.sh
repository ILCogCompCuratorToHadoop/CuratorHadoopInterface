#!/bin/bash

# TODO outdated!
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

CURATOR_DIRECTORY=/project/cogcomp/curator-0.6.9_1
HADOOP_DIRECTORY=/hadoop
# This is where we will store the serialized forms of our records before
# sending them to Hadoop.
STAGING_DIRECTORY=/scratch/tyoun
INPUT_PATH=$1                   # The 1st parameter from the command line 
                                #   (must be a local path to be copied to 
                                #   HDFS)
MODE=$2                         # 2nd parameter from CL should be "serial" 
                                #   or "raw"
DESTINATION_FOR_INPUT_IN_HADOOP=$3 # The directory in HDFS in which we 
                                   # will put our job's input Records

# If you need to specify more fully the location in HDFS to which we 
# copy our input, do so here. By default, we copy the directory named
# $DESTINATION_FOR_INPUT_IN_HADOOP to the Hadoop working directory
# (which should be, but might not be, /home/[your user name]/ in HDFS).
PREFIX_TO_HADOOP_DIR=/home/tyoun


# If you're logging the output of this script to a file (instead of 
# just reading it on the command line), you might want
# to comment out these colors for a more readable plain text file.
MSG_COLOR='\e[0;36m'     # Cyan. Might also try dark gray (1;30), green
                         # (0;32), or light green (1;32).
DEFAULT_COLOR='\e[0m'    # Reset to normal
ERROR_COLOR='\e[0;31m'

#########################################################################
#                       No need to edit below here                      #

BASEDIR=$(dirname $0) # location of this script file

set -e # Exit the script if any command fails

# Fix the DESTINATION_FOR_INPUT_IN_HADOOP variable if user wants a prefix
# If the prefix is not null (empty)
if [ -n "$PREFIX_TO_HADOOP_DIR" ]; then
    DESTINATION_FOR_INPUT_IN_HADOOP=$PREFIX_TO_HADOOP_DIR/$DESTINATION_FOR_INPUT_IN_HADOOP
fi

echo -e "$MSG_COLOR\n\n\nYou said your copy of Curator is located here: $CURATOR_DIRECTORY"
echo "You requested we run the annotation tool $ANNOTATION_TOOL_TO_RUN on your input"
echo "You requested we annotate the input text files located here: $INPUT_PATH"
echo -e "\t(That input directory should be an *absolute* path.) $DEFAULT_COLOR"

echo -e "\nYou launched this script like this: copy_input_to_hadoop.sh $*"

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
    ./runclient.sh -host localhost -port 9010 -in $INPUT_PATH -out $STAGING_DIRECTORY -mode PRE
    chmod -R 777 $STAGING_DIRECTORY

    echo -e "$MSG_COLOR\n\nShutting down locally running Curator. $DEFAULT_COLOR"
    # Get list of currently running Java procs | find the Curator server | split on spaces (?) | send the first thing (i.e., the process ID) to the kill command
    jps -l | grep edu.illinois.cs.cogcomp.curator.CuratorServer | cut -d ' ' -f 1 | xargs -n1 kill

    # Copy the serialized records to the Hadoop Distributed File System (HDFS)
    echo -e "$MSG_COLOR\n\n\nCopying the serialized records to HDFS: $DEFAULT_COLOR"
    cd $HADOOP_DIRECTORY
    set +e # Do *not* exit the script if any command fails
    ./bin/hadoop dfs -rmr $DESTINATION_FOR_INPUT_IN_HADOOP
    ./bin/hadoop dfs -mkdir $DESTINATION_FOR_INPUT_IN_HADOOP
    set -e # Exit the script if any command fails
    ./bin/hadoop dfs -copyFromLocal $STAGING_DIRECTORY/* $DESTINATION_FOR_INPUT_IN_HADOOP
    echo -e "$MSG_COLOR\nCopied successfully. $DEFAULT_COLOR"


# Else we are working with already serialized records, not raw text:
else
    # Copy the input directory, which we assume to have only serialized
    # records, to the cluster
    echo -e "$MSG_COLOR\n\n\nCopying the serialized records to HDFS: $DEFAULT_COLOR"
    cd $HADOOP_DIRECTORY
    set +e # Do *not* exit the script if any command fails
    ./bin/hadoop dfs -rmr $DESTINATION_FOR_INPUT_IN_HADOOP
    ./bin/hadoop dfs -mkdir $DESTINATION_FOR_INPUT_IN_HADOOP
    
    # If this came from a MapReduce job, let's not copy the MR output back
    if [ -f $INPUT_PATH/mapreduce_out ]; then
	rm -r $INPUT_PATH/mapreduce_out
    fi

    set -e # Exit the script if any command fails
    ./bin/hadoop dfs -copyFromLocal $INPUT_PATH/* $DESTINATION_FOR_INPUT_IN_HADOOP
    echo -e "$MSG_COLOR\nCopied successfully. $DEFAULT_COLOR"
fi

exit 0
