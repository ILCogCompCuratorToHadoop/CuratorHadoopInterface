#!/bin/bash

# Usage: ./batch_job_master_script some/path/to/curator_directory ANNOTATION_TOOL_TO_RUN path/to/your_input_directory
# Example (from a directory containing both hadoop-1.0.3 and curator-0.6.9):
#       ./batch_job_master_script.sh curator-0.6.9 TOKENIZER job123 


CURATOR_DIRECTORY=$1
CURRENT_TOOL=$2
INPUT_PATH_FROM_CLIENT_DIR=$3
HADOOP_DIR_FROM_CLIENT=../../hadoop-1.0.3/
INTERMEDIATE_OUTPUT=$HADOOP_DIR_FROM_CLIENT/serialized/

echo -e "\nYou said your copy of Curator is located here: $1"
echo "You requested we run the annotation tool $2 on your input"
echo -e "You requested we annotate the input text files located here: $3\n"

# Launch the Master Curator
cd $CURATOR_DIRECTORY/dist
./bin/curator-local.sh --annotators configs/annotators-local.xml --port 9010 --threads 10 >& logs/curator.log &

# Launch the Master Curator Client, asking it to serialize the
# records from the text in the input directory
cd client
./runclient.sh localhost 9010 $INPUT_PATH_FROM_CLIENT_DIR $INTERMEDIATE_OUTPUT 

# Copy the serialized records to the Hadoop Distributed File System (HDFS)
cd $HADOOP_DIR_FROM_CLIENT
./bin/hadoop dfs -copyFromLocal serialized serialized

# Launch MapReduce job on Hadoop cluster
./bin/hadoop jar curator.jar serialized $CURRENT_TOOL

# When the MapReduce job finishes, copy the data back to local disk
./bin/hadoop fs -copyToLocal serialized extended_serialized

# Have Master Curator read in the updated Records and update the database accordingly


# New Hadoop job:
#       Have the Hadoop nodes kill the running annotator, Curator, and 
#       Curator Client processes

exit 0
