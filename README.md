<!-- -*-Markdown-*- -->

The Curator-to-Hadoop Interface
========================================

Overview (About the Project)
----------------------------------------

This is a tool for interfacing the [Curator](http://cogcomp.cs.illinois.edu/trac/wiki/Curator) with a [Hadoop](http://hadoop.apache.org/) cluster. To contribute to the project, contact @s3cur3.

The overall goal of the project is to provide an architecture for efficiently processing large text corpora using Hadoop/MapReduce. This architecture consists of the following:

* New classes, inheriting from the original Curator Client, which include:  
    1. a local client that runs on each node in the Hadoop cluster and waits for input from the Hadoop process manager.
    2. a master client that sets up the inputs for the Hadoop cluster and sends batch jobs to the cluster.
	
* An interface to Hadoop, called from the Master Curator.

* A set of scripts to start the Curator in local mode on all nodes in the Hadoop cluster.

Advantages to this system include:

* (Potentially massive) data parallelism, which scales linearly with the size of the Hadoop cluster, in a tool set which has been written to operate in a strictly linear fashion.

* Pre-processing of large corpora (for instance, after one annotation tool has been upgraded) with little user intervention, freeing up more time to solve research problems.

Getting Started
----------------------------------------

Make sure you have all the files present in the [[Manifest]] section.

TODO: Step-by-step guide to go from downloading the package through compiling, setting up a job, getting the output, etc.

### Examining the Log Files ###

Although we have attempted to log as much as possible to standard text files, your best source of log information on a Hadoop/MapReduce job will come from Hadoop's logs. (This is due to an oddity in the way Hadoop handles both standard output and shared loggers.) 

To access the Hadoop logs:

1. Navigate to your Hadoop JobTracker's web interface. The address may look something like this: http://somejobtracker.cs.illinois.edu:50030/jobtracker.jsp

2. Your jobs will show up there, under the list of Running, Completed, or Failed jobs. Click on the job ID of the job you're interested in.

3. Click on the **reduce** link. (All the interesting work our program does occurs in the Reduce phase, so that's the only place to check for logs.)

4. Click on a Task link. There should be one task per document that you passed in.

5. On the far right of the screen are Task Log links. You will probably want to click the "All" link.

6. Scroll through the log you've found!

Manifest
----------------------------------------

Ensure that, in addition to your standard Hadoop and Curator installations, the following files are present in your directories:

* `hadoop-1.0.3/`

    * `curator.jar`
    * `lib/`

        * `curator-interfaces.jar`
        * `curator-server.jar`
        * `libthrift.jar` (currently tested on Thrift v0.4)

* `curator-0.6.9/`

    * `bin/`

         * `curator-local.sh` modified to add stuff to the class path. Full class path line: `COMPONENT_CLASSPATH=$CURATOR_BASE:$COMPONENTDIR/curator-server.jar:$COMPONENTDIR/illinois-tokenizer-server.jar:$COMPONENTDIR/illinois-pos-server.jar:$COMPONENTDIR/illinois-chunker-server.jar:$COMPONENTDIR/illinois-coref-server.jar:$COMPONENTDIR/stanford-parser-server.jar:$COMPONENTDIR/curator-interfaces.jar:$COMPONENTDIR/illinois-ner-extended-server.jar`

Troubleshooting
----------------------------------------

* Make sure that you can launch the annotators on the Hadoop node using the same commands you find in the Hadoop logs. For instance, for tools that do not run in local mode (all tools except the tokenizer, POS, Stanford parser, and chunker), each time a Hadoop node annotates its first document using a given annotation, it will print the command it used to launch the annotator. If you cannot use that same command to launch the annotator manually on the Hadoop node, there will be problems.

* If you have issues running the annotation tools in Hadoop (especially the Charniak parser), try passing an additional argument when you launch the job on Hadoop. (To do so, you may have to modify the script that gives the job to the Hadoop Job Handler.) Add the argument `-lib some\_library\_path` to the call to Hadoop. That library path should be an absolute path (i.e., one beginning with `/`), and inside that directory should be the Thrift library files (e.g., `libthrift.so.0`).

Modifications to the Curator
----------------------------------------

This package currently requires a custom-built version of the Curator which makes the following modifications:

* In `[Curator home]/curator-interfaces/curator.thrift`: added `getTimeOfLastAnnotation()` method.
* In `[Curator home]/curator-server/CuratorHandler.java`: added `getTimeOfLastAnnotation()` and modified `performAnnotation()` to update a new private field `lastAnnotationTime`.
* In `[Curator home]/curator-server/CuratorHandler.java`: added the private `InactiveCuratorKiller` class, which periodically queries the `CuratorHandler`'s last annotation time and calls `Runtime.exit()` if the last annotation took place too long ago. Also modified the `runServer()` method's signature to take a
  Curator.Iface (the CuratorHandler), and had it spawn a thread of the `InactiveCuratorKiller` right before calling `server.serve()`.
* In `[Curator home]/curator-annotators/`: each [annotation type]Server/[annotation type]Handler pair modified to monitor and kill the handler after a period of inactivity, almost identical to the modifications made in 'CuratorServer/CuratorHandler'.
 
