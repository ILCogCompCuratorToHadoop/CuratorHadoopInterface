#!/bin/bash

# Usage (after configuring the directory variables below):
#    ./batch_job_master_script ANNOTATION_TOOL_TO_RUN path/to/your_input_directory raw
# Example: ./batch_job_master_script.sh TOKENIZER /shared/gargamel/undergrad/tyoun/curator-0.6.9/dist/client/job123 raw
# Example: ./batch_job_master_script.sh POS /shared/gargamel/undergrad/tyoun/hadoop-1.0.3/serialized_output/ serial


#       Change these variables to the appropriate *absolute paths*      #
#########################################################################

# The location of the Hadoop installation (where we launch jobs from)
HADOOP_DIRECTORY=/hadoop
# Where we find the Thrift libraries on the Hadoop nodes (this should
# probably be local to each machine in your Hadoop cluster)
#LIB_DIR_ON_HADOOP_NODES=/shared/grandpa/opt/lib
CURATOR_DIR_ON_HADOOP_NODES=/project/cogcomp/curator-0.6.9

# In the output directory, we will place a dir called "serialized" which
# will store the job's output records
OUTPUT=/shared/gargamel/undergrad/tyoun/hadoop-1.0.3
ANNOTATION_TOOL_TO_RUN=$1       # The 1st parameter from the command line:
                                #   the annotator to use on the input docs
INPUT_DIR_IN_HDFS=$2            # The 2nd parameter from the command line:
                                #   the location to take input from
OUTPUT_DIR_IN_HDFS=$3           # The location we should write output to

# If you need to specify more fully the location in HDFS in which we 
# do I/O, do so here. By default, we assume the INPUT and OUTPUT 
# directories to within the Hadoop working directory
# (which should be, but might not be, /home/[your user name]/ in HDFS).
PREFIX_TO_HADOOP_DIRS=/home/tyoun

# If you're logging the output of this script to a file (instead of 
# just reading it on the command line), you might want
# to comment out these colors for a more readable plain text file.
MSG_COLOR='\e[0;36m'     # Cyan. Might also try dark gray (1;30), green
                         # (0;32), or light green (1;32).
DEFAULT_COLOR='\e[0m'    # Reset to normal
ERROR_COLOR='\e[0;31m'

#########################################################################
#                       No need to edit below here                      #


set -e # Exit the script if any command fails

# Fix the INPUT_DIR_IN_HDFS and OUTPUT_DIR_IN_HDFS variables if user 
# wants a prefix
# If the prefix is not null (empty)
if [ -n "$PREFIX_TO_HADOOP_DIRS" ]; then
    INPUT_DIR_IN_HDFS=$PREFIX_TO_HADOOP_DIRS/$INPUT_DIR_IN_HDFS
    OUTPUT_DIR_IN_HDFS=$PREFIX_TO_HADOOP_DIRS/$OUTPUT_DIR_IN_HDFS
fi

# Go to the Hadoop directory
cd $HADOOP_DIRECTORY

# Launch MapReduce job on Hadoop cluster
echo -e "$MSG_COLOR\n\n\nLaunching the mapreduce job on the Hadoop cluster $DEFAULT_COLOR"
CMD="bin/hadoop jar /project/cogcomp/HadoopInterface/HadoopInterface.jar edu.illinois.cs.cogcomp.hadoopinterface.HadoopInterface -d $INPUT_DIR_IN_HDFS -m $ANNOTATION_TOOL_TO_RUN -out $OUTPUT_DIR_IN_HDFS -reduces 8 -curator $CURATOR_DIR_ON_HADOOP_NODES" #-lib $LIB_DIR_ON_HADOOP_NODES
echo -e "using command $CMD"
./$CMD

echo -e "$MSG_COLOR\n\n\n$ANNOTATION_TOOL_TO_RUN job finished!\n$DEFAULT_COLOR"


# Have the Hadoop nodes kill the running annotator, Curator, and Curator Client processes
# In order to do this, launch the HadoopInterface with parameter '-cleanup'
#echo -e "$MSG_COLOR\n\nRunning cleanup . . . $DEFAULT_COLOR"
#echo "(Note that this is another MapReduce job which we run in order "
#echo "to shut down the previously used annotators.)"

#./bin/hadoop jar curator.jar edu.illinois.cs.cogcomp.hadoopinterface.HadoopInterface -d $INPUT_DIR_IN_HDFS -m $ANNOTATION_TOOL_TO_RUN -out $OUTPUT_DIR_IN_HDFS -lib $LIB_DIR_ON_HADOOP_NODES -reduces 1 -cleanup

#echo -e "$MSG_COLOR\n\n\nCleanup finished!\n$DEFAULT_COLOR"