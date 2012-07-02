#!/bin/bash

# Usage: ./batch_job_master_script some/path/to/curator_directory SOME_ANNOTATION_MODE path/to/your_input_directory


CURATOR_DIRECTORY = $1
CURRENT_TOOL = $2
INPUT_PATH = $3

echo "You said your copy of Curator is located here: $1"
echo "You requested we run the annotation tool $2 on your input"
echo "You requested we annotate the input text files located here: $3"

# Launch the Master Curator Client, asking it to serialize the
# records from the text in the input directory

# Copy the serialized records to the Hadoop Distributed File System (HDFS)

# Launch MapReduce job on Hadoop cluster

# When the MapReduce job finishes, copy the data back to local disk 

# Have Master Curator read in the updated Records and update the database accordingly


# New Hadoop job:
#       Have the Hadoop nodes kill the running annotator, Curator, and 
#       Curator Client processes

exit 0
