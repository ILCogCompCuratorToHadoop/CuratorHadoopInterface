#!/bin/bash

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
#                Should match exactly the same variables in             #
#                   batch_master_curator_to_hadoop.sh                   #
#########################################################################

CURATOR_DIRECTORY=/shared/gargamel/undergrad/tyoun/curator-0.6.9
HADOOP_DIRECTORY=/shared/gargamel/undergrad/tyoun/hadoop-1.0.3
INTERMEDIATE_OUTPUT=$HADOOP_DIRECTORY/serialized
OUTPUT=/shared/gargamel/undergrad/tyoun/hadoop-1.0.3
PATH_IN_HADOOP=$1                   # The 2nd parameter from the command line
DESTINATION_IN_LOCAL=$2             # Should be an absolute path on the
                                    # local disk

# If you're running logging the output of this to a file, you might want
# to comment out these colors for a more readable plain text file.
MSG_COLOR='\e[0;36m'     # Cyan. Might also try dark gray (1;30), green
                         # (0;32), or light green (1;32).
DEFAULT_COLOR='\e[0m'    # Reset to normal
ERROR_COLOR='\e[0;31m'

#########################################################################
#                       No need to edit below here                      #


# Move to the Hadoop directory
cd /shared/gargamel/undergrad/tyoun/hadoop-1.0.3






set +e # Do *not* exit the script if a command fails (so we can give
       # useful suggestions to the user)

# When the MapReduce job finishes, copy the data back to local disk
echo -e "$MSG_COLOR\nCopying the results of the MapReduce job back to the local machine $DEFAULT_COLOR"

# Does the directory we want to copy to already exist? Throw an error if so.
if [ -e $DESTINATION_IN_LOCAL ]; then
    echo -e "$ERROR_COLOROutput directory $OUTPUT/$DESTINATION_IN_LOCAL already exists.$DEFAULT_COLOR"
    echo "Should we delete it and replace?"
    echo "(If no, we will have to exit the job, let you move things around, then"
    echo "let you relaunch the job or something.)"
    echo "Delete exising $OUTPUT ? (y/n)"
    read ANSWER
    if [ $ANSWER = 'y' ]; then
	chmod -R 777 $DESTINATION_IN_LOCAL
	rm -r $DESTINATION_IN_LOCAL
    else
        exit 1
    fi
fi

echo -e "$MSG_COLOR\nCreating output directory $DESTINATION_IN_LOCAL $DEFAULT_COLOR"
mkdir $DESTINATION_IN_LOCAL

# Do the actual copy operation
# TODO: Make this a distributed Hadoop job
./bin/hadoop fs -copyToLocal $DESTINATION_IN_LOCAL $OUTPUT

# If the copy to local failed . . . 
if [[ $? -ne 0 ]] ; then
   echo -e "$MSG_COLOR\nCopying to local failed. Try fixing the error, then executing:"
   echo -e "./bin/hadoop fs -copyToLocal $DESTINATION_IN_LOCAL $OUTPUT\n$DEFAULT_COLOR"
   exit 1
fi 
echo -e "$MSG_COLOR\nCopying to local (at $DESTINATION_IN_LOCAL) succeeded.\n$DEFAULT_COLOR"

chmod -R 777 $DESTINATION_IN_LOCAL

set -e







# Have Master Curator read in the updated Records and update the database accordingly
echo -e "$MSG_COLOR\n\n\nRe-launching the master Curator. $DEFAULT_COLOR"
cd $CURATOR_DIRECTORY/dist
./bin/curator.sh --annotators configs/annotators-empty.xml --port 9010 --threads 10 >& logs/curator.log & >/dev/null
sleep 3s
cd client
./runclient.sh -host localhost -port 9010 -in $OUTPUT/serialized_output -mode POST $TESTING 

# Kill the Curator server
echo -e "$MSG_COLOR\n\nShutting down the master Curator. $DEFAULT_COLOR"
# Get list of currently running Java procs | 
# find the Curator server | 
# split on spaces (?) | 
# send the first thing (i.e., the process ID) to the kill command
jps -l | grep edu.illinois.cs.cogcomp.curator.CuratorServer | cut -d ' ' -f 1 | xargs -n1 kill

# Have the Hadoop nodes kill the running annotator, Curator, and Curator Client processes
# In order to do this, launch the HadoopInterface with parameter '-cleanup'
./bin/hadoop jar HadoopInterface.jar -d $INPUT_PATH -m $ANNOTATION_TOOL_TO_RUN -cleanup


exit 0