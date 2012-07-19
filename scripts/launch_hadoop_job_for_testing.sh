#!/bin/bash

# Launches the Hadoop job.
# You probably want to call this using the batch job's master script
# (batch_job_master_script.sh).

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

echo -e "$MSG_COLOR\n\n\nLaunching the mapreduce job on the Hadoop cluster:$DEFAULT_COLOR"
cd $HADOOP_DIRECTORY
./bin/hadoop jar curator.jar edu/illinois/cs/cogcomp/HadoopInterface -d serialized -m $ANNOTATION_TOOL_TO_RUN -out serialized_output
echo -e "$MSG_COLOR\n\n\nJob finished!\n\n$DEFAULT_COLOR"
