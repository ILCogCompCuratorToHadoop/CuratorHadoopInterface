#!/bin/bash

# TODO outdated!
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
#########################################################################

CURATOR_DIRECTORY=/project/cogcomp/curator-0.6.9
HADOOP_DIRECTORY=/hadoop
PATH_IN_HADOOP=$1           # The 1st parameter from the command line:
                            # the location of the output serialized 
                            # records in HDFS
DESTINATION_IN_LOCAL=$2     # The place to which we should copy the 
                            # Hadoop job's output. Should be an absolute 
                            # path on the local disk.

# If you're logging the output of this script to a file (instead of 
# just reading it on the command line), you might want
# to comment out these colors for a more readable plain text file.
MSG_COLOR='\e[0;36m'     # Cyan. Might also try dark gray (1;30), green
                         # (0;32), or light green (1;32).
DEFAULT_COLOR='\e[0m'    # Reset to normal
ERROR_COLOR='\e[0;31m'

#########################################################################
#                       No need to edit below here                      #


# Move to the Hadoop directory
cd $HADOOP_DIRECTORY


set +e # Do *not* exit the script if a command fails (so we can give
       # useful suggestions to the user)

# When the MapReduce job finishes, copy the data back to local disk
echo -e "$MSG_COLOR\nCopying the results of the MapReduce job back to the local machine $DEFAULT_COLOR"

# Does the directory we want to copy to already exist? Throw an error if so.
if [ -e $DESTINATION_IN_LOCAL ]; then
    echo -e "$ERROR_COLOR\nOutput directory $DESTINATION_IN_LOCAL already exists.$DEFAULT_COLOR"
    echo -e "\tMoving your old version up one directory."
    mv $DESTINATION_IN_LOCAL $DESTINATION_IN_LOCAL/../old_output
fi

echo -e "$MSG_COLOR\nCreating output directory $DESTINATION_IN_LOCAL $DEFAULT_COLOR"
mkdir $DESTINATION_IN_LOCAL
chmod 777 $DESTINATION_IN_LOCAL

# Do the actual copy operation
# TODO: Make this a distributed Hadoop job
./bin/hadoop fs -copyToLocal $PATH_IN_HADOOP $DESTINATION_IN_LOCAL

# If the copy to local failed . . . 
if [[ $? -ne 0 ]] ; then
   echo -e "$MSG_COLOR\nCopying to local failed. Try fixing the error, then executing:"
   echo -e "scripts/copy_output_from_hadoop.sh $1 $2 $3 $4"
   exit 1
fi 
echo -e "$MSG_COLOR\nCopying to local (at $DESTINATION_IN_LOCAL) succeeded.\n$DEFAULT_COLOR"

chmod -R 777 $DESTINATION_IN_LOCAL

set -e







# Have Master Curator read in the updated Records and update the database accordingly
echo -e "$MSG_COLOR\n\n\nRe-launching the master Curator and asking it"
echo -e "to reconstruct the serialized records we got from Hadoop. $DEFAULT_COLOR"
cd $CURATOR_DIRECTORY/dist
./bin/curator.sh --annotators configs/annotators-empty.xml --port 9010 --threads 10 >& logs/curator.log & >/dev/null
sleep 3s
cd client
# NOTE: From Hadoop, we copy some directory (named, e.g., "TOKEN").
#       That directory goes into DESTINATION_IN_LOCAL, so the actual
#       directory we need to work with is $DESTINATION_IN_LOCAL/$PATH_IN_HADOOP.
./runclient.sh -host localhost -port 9010 -in $DESTINATION_IN_LOCAL/$PATH_IN_HADOOP -mode POST

# Kill the Curator server
echo -e "$MSG_COLOR\n\nShutting down the master Curator. $DEFAULT_COLOR"
# Get list of currently running Java procs | 
# find the Curator server | 
# split on spaces (?) | 
# send the first thing (i.e., the process ID) to the kill command
jps -l | grep edu.illinois.cs.cogcomp.curator.CuratorServer | cut -d ' ' -f 1 | xargs -n1 kill

exit 0