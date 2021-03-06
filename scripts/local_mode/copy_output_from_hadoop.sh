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
PATH_IN_HADOOP=$1                   # The 2nd parameter from the command line
DESTINATION_IN_LOCAL=$2             # Should be an absolute path on the
                                    # local disk
TESTING=$3 # If the 3rd param from the CL is "-test", we will have the
           # locally-running Curator verify that the annotations it has
           # received are the same ones it gets itself.
           # NOTE: Unless you modify the script below, we will launch
           # *all* annotators when launching in test mode. *Only* do
           # this on a machine with lots of RAM (24 GB or more?).

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
    for i in {1..100}; do
	POTENTIAL_MV="$DESTINATION_IN_LOCAL/../old_output$i"
	if [ ! -e $POTENTIAL_MV ]; then
	    mv $DESTINATION_IN_LOCAL $POTENTIAL_MV
	    break
	fi
    done
fi

echo -e "$MSG_COLOR\nCreating output directory $DESTINATION_IN_LOCAL $DEFAULT_COLOR"
mkdir $DESTINATION_IN_LOCAL

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

# Set up variables used to launch the CuratorClient
LIBDIR=$CURATOR_DIRECTORY/dist/lib
COMPONENTDIR=$CURATOR_BASE/components
COMPONENT_CLASSPATH=.:$COMPONENTDIR/curator-interfaces.jar:$COMPONENTDIR/curator-server.jar:CuratorClientDependencies.jar
LIB_CLASSPATH=$LIBDIR/libthrift.jar:$LIBDIR/logback-classic-0.9.17.jar:$LIBDIR/logback-core-0.9.17.jar:$LIBDIR/slf4j-api-1.5.8.jar:$LIBDIR/curator-client-0.6.jar:$LIBDIR/curator-interfaces.jar
OUR_CLASSPATH=$COMPONENT_CLASSPATH:$LIB_CLASSPATH

CMD=""
# If the user specified the "-test" flag, we launch all annotators on the local
# machine. The CuratorClient will have the Curator verify all annotations.
if [ "$TESTING" == "-test" ]; then
    CMD="startServers.sh"
    echo "Launching Curator with command $CMD"
    ./$CMD
    echo "Since we're in testing mode, we're going to wait 3 minutes for the servers to start."
    sleep 1m
    echo "Waiting 2 more minutes..."
    sleep 1m
    #echo "Waiting 1 more minute..."
    #sleep 1m
else
    CMD="bin/curator.sh --annotators configs/annotators-empty.xml --port 9010 --threads 10 >& logs/curator.log & >/dev/null"
    echo "Launching Curator with command $CMD"
    ./$CMD
    sleep 3s
fi


cd client
# If this isn't working, make sure you have a CuratorClient.class file in the 
# edu/illinois/cs/cogcomp/hadoopinterface/ directory
# Note: argument -Xmx[number] used to specify the maximum size, in bytes, 
#     of the memory allocation pool. 
# NOTE: From Hadoop, we copy some directory (named, e.g., "TOKEN").
#       That directory goes into DESTINATION_IN_LOCAL, so the actual
#       directory we need to work with is $DESTINATION_IN_LOCAL/$PATH_IN_HADOOP.
ARGS="-cp $OUR_CLASSPATH -Xmx512m edu.illinois.cs.cogcomp.hadoopinterface.CuratorClient -host localhost -port 9010 -in $DESTINATION_IN_LOCAL/$PATH_IN_HADOOP -mode POST"
if [ "$TESTING" == "-test" ]; then
    ARGS="$ARGS -test"
fi
echo "Launching Curator Client with command: java $ARGS"
java $ARGS


# Kill the Curator server
echo -e "$MSG_COLOR\n\nShutting down the master Curator. $DEFAULT_COLOR"
# Get list of currently running Java procs | 
# find the Curator server | 
# split on spaces (?) | 
# send the first thing (i.e., the process ID) to the kill command
jps -l | grep edu.illinois.cs.cogcomp.curator.CuratorServer | cut -d ' ' -f 1 | xargs -n1 kill

exit 0