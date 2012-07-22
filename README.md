<!-- -*-Markdown-*- -->

The Curator-to-Hadoop Interface
========================================

About the Project
----------------------------------------

This is a tool for interfacing the [Curator](http://cogcomp.cs.illinois.edu/trac/wiki/Curator) with a [Hadoop](http://hadoop.apache.org/) cluster. To contribute to the project, contact @s3cur3.

The overall goal of the project is to provide an architecture for efficiently processing large text corpora using Hadoop/MapReduce. This architecture consists of the following:

* A set of changes to the Curator which allow it two new "modes" in
  which to operate:  
    1. local mode, which runs on each node in the Hadoop cluster and waits for input from the Hadoop process manager, and
    2. master mode, which sets up the inputs for the Hadoop cluster and sends batch jobs to the cluster.
	
* An interface to Hadoop, callable from the Master Curator.

* A set of scripts to start the Curator in local mode on all nodes in the Hadoop cluster.

The advantages to this system are as follows:

* It allows for (potentially massive) data parallelism, which scales linearly with the size of the Hadoop cluster, in a tool set which has been written to operate in a strictly linear fashion.
  
* It allows for the pre-processing of a large corpora (for instance, after one annotation tool has been upgraded) with little user intervention, freeing up more time to solve research problems.

Use notes
----------------------------------------

Make sure you have all the files present in the [[Manifest]] section.

Manifest
----------------------------------------

Ensure that, in addition to your stock, working Hadoop and Curator
installations, following files are present in your directories.

* `hadoop-1.0.3/`

    * `curator.jar`
    * `lib/`

        * `curator-interfaces.jar`
        * `curator-server.jar`
        * `libthrift.jar` (currently tested on Thrift v0.4)

* `curator-0.6.9/`

    * `bin/`

         * `curator-local.sh` modified to add stuff to the class path. Full class path line: `COMPONENT_CLASSPATH=$CURATOR_BASE:$COMPONENTDIR/curator-server.jar:$COMPONENTDIR/illinois-tokenizer-server.jar:$COMPONENTDIR/illinois-pos-server.jar:$COMPONENTDIR/illinois-chunker-server.jar:$COMPONENTDIR/illinois-coref-server.jar:$COMPONENTDIR/stanford-parser-server.jar:$COMPONENTDIR/curator-interfaces.jar:$COMPONENTDIR/illinois-ner-extended-server.jar`

