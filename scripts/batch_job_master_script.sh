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

CURATOR_DIRECTORY=/shared/gargamel/undergrad/tyoun/curator-0.6.9/
HADOOP_DIRECTORY=/shared/gargamel/undergrad/tyoun/hadoop-1.0.3/
INTERMEDIATE_OUTPUT=$HADOOP_DIRECTORY/serialized/
OUTPUT=/shared/gargamel/undergrad/tyoun/hadoop-1.0.3/job_output/
ANNOTATION_TOOL_TO_RUN=$1       # The 1st parameter from the command line
INPUT_PATH=$2                   # The 2nd parameter from the command line

#########################################################################
#                       No need to edit below here                      #


echo -e "\n\n\nYou said your copy of Curator is located here: $CURATOR_DIRECTORY"
echo "You requested we run the annotation tool $ANNOTATION_TOOL_TO_RUN on your input"
echo "You requested we annotate the input text files located here: $INPUT_PATH"
echo -e "\t(That input directory should be an *absolute* path.)"

# Launch the Master Curator
echo -e "\n\n\nLaunching the master curator:"
cd $CURATOR_DIRECTORY/dist
./bin/curator-local.sh --annotators configs/annotators-local.xml --port 9010 --threads 10 >& logs/curator.log & >/dev/null

# Launch the Master Curator Client, asking it to serialize the
# records from the text in the input directory
echo -e "\n\n\nLaunching the master curator client:"
cd client
./runclient.sh localhost 9010 $INPUT_PATH $INTERMEDIATE_OUTPUT 

# Copy the serialized records to the Hadoop Distributed File System (HDFS)
echo -e "\n\n\nCopying the serialized records to HDFS:"
cd $HADOOP_DIRECTORY
./bin/hadoop dfs -copyFromLocal $INTERMEDIATE_OUTPUT/* serialized

# Launch MapReduce job on Hadoop cluster
echo -e "\n\n\nLaunching the mapreduce job on the Hadoop cluster:"
./bin/hadoop jar curator.jar serialized $ANNOTATION_TOOL_TO_RUN

# When the MapReduce job finishes, copy the data back to local disk
echo -e "\n\n\nCopying the data back from the Hadoop cluster:"
./bin/hadoop fs -copyToLocal serialized/* $OUTPUT

# Have Master Curator read in the updated Records and update the database accordingly


# New Hadoop job:
#       Have the Hadoop nodes kill the running annotator, Curator, and 
#       Curator Client processes

exit 0
