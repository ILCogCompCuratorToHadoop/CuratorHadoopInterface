<!-- -*-Markdown-*- -->

About the Project
=================

This is a tool for interfacing the [Curator](http://cogcomp.cs.illinois.edu/trac/wiki/Curator) with a [Hadoop](http://hadoop.apache.org/) cluster. To contribute to the project, contact @s3cur3.

The overall goal of the project is to provide an architecture for efficiently processing large text corpora using Hadoop/MapReduce. This architecture consists of the following:

* A set of changes to the Curator which allow it two new "modes" in which to operate:

	1. local mode, which runs on each node in the Hadoop cluster and
	waits for input from the Hadoop process manager, and
    2. master mode, which sets up the inputs for the Hadoop cluster
    and sends batch jobs to the cluster.
	
* An interface to Hadoop, callable from the Master Curator.

* A set of scripts to start the Curator in local mode on all nodes in the Hadoop cluster.

The advantages to this system are as follows:

* It allows for (potentially massive) data parallelism, which scales linearly
  with the size of the Hadoop cluster, in a tool set which has been
  written to operate in a strictly linear fashion.
  
* It allows for the pre-processing of a large corpora (for instance,
  after one annotation tool has been upgraded) with little user
  intervention, freeing up more time to solve research problems.



