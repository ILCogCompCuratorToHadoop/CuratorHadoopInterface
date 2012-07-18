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

#########################################################################
#                       No need to edit below here                      #

set -e # Exit the script if any command fails

echo -e "\n\n\nYou said your copy of Curator is located here: $CURATOR_DIRECTORY"
echo "You requested we run the annotation tool $ANNOTATION_TOOL_TO_RUN on your input"
echo "You requested we annotate the input text files located here: $INPUT_PATH"
echo -e "\t(That input directory should be an *absolute* path.)"

# Launch the Master Curator
echo -e "\n\n\nLaunching the master curator."
cd $CURATOR_DIRECTORY/dist
./bin/curator-local.sh --annotators configs/annotators-local.xml --port 9010 --threads 10 >& logs/curator.log & >/dev/null

# Launch the Master Curator Client, asking it to serialize the
# records from the text in the input directory
echo -e "\n\n\nLaunching the master curator client:"
cd client
./runclient.sh -host localhost -port 9010 -in $INPUT_PATH -out $INTERMEDIATE_OUTPUT -mode PRE $TESTING 

# Copy the serialized records to the Hadoop Distributed File System (HDFS)
echo -e "\n\n\nCopying the serialized records to HDFS:"
cd $HADOOP_DIRECTORY
set +e # Do *not* exit the script if any command fails
./bin/hadoop dfs -rmr serialized
./bin/hadoop dfs -mkdir serialized
set -e # Exit the script if any command fails
./bin/hadoop dfs -copyFromLocal $INTERMEDIATE_OUTPUT/* serialized
echo -e "Copied successfully."


# Launch MapReduce job on Hadoop cluster
echo -e "\n\n\nLaunching the mapreduce job on the Hadoop cluster:"
./bin/hadoop jar curator.jar -d serialized -m $ANNOTATION_TOOL_TO_RUN -out serialized_output
echo -e "\n\n\nJob finished!\n\n"


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



# New Hadoop job:
#       Have the Hadoop nodes kill the running annotator, Curator, and 
#       Curator Client processes

exit 0
