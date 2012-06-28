#!/bin/bash

# Get list of documents to proces

# Launch Master Curator

# Get serialized records for all documents from Master Curator

# Copy the records to the Hadoop Distributed File System (HDFS)

# Launch MapReduce job on Hadoop cluster (probably ssh in to the cluster...?)

# When the MapReduce job finishes, copy the data back to local disk 

# Have Master Curator read in the updated Records and update the database accordingly


# New Hadoop job:
#       Have the Hadoop nodes kill the running annotator, Curator, and 
#       Curator Client processes