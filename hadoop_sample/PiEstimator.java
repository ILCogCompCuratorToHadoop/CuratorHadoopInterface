/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.gmail.s3cur3.hadoop_test;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Iterator;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * A Map-reduce program to estimate the value of Pi
 * using quasi-Monte Carlo method.
 *
 * Mapper:
 *   Generate points in a unit square
 *   and then count points inside/outside of the inscribed circle of the square.
 *
 * Reducer:
 *   Accumulate points inside/outside results from the mappers.
 *
 * Let numTotal = numInside + numOutside.
 * The fraction numInside/numTotal is a rational approximation of
 * the value (Area of the circle)/(Area of the square),
 * where the area of the inscribed circle is Pi/4
 * and the area of unit square is 1.
 * Then, Pi is estimated value to be 4(numInside/numTotal).  
 */
public class PiEstimator extends Configured implements Tool {
  /** tmp directory for input/output */
  static private final Path TMP_DIR = new Path( PiEstimator.class.getSimpleName()
          + "_TMP_3_141592654");

  /**
   * Run a map/reduce job for estimating Pi.
   *
   * @return the estimated value of Pi
   */
  public static BigDecimal estimate(int numMaps, long numPoints, JobConf jobConf )
          throws IOException {
    //setup job conf
    jobConf.setJobName(PiEstimator.class.getSimpleName());

    jobConf.setInputFormat(SequenceFileInputFormat.class);

    jobConf.setOutputKeyClass(BooleanWritable.class);
    jobConf.setOutputValueClass(LongWritable.class);
    jobConf.setOutputFormat(SequenceFileOutputFormat.class);

    jobConf.setMapperClass(PiMapper.class);
    jobConf.setNumMapTasks(numMaps);

    jobConf.setReducerClass(PiReducer.class);
    jobConf.setNumReduceTasks(1);

    // turn off speculative execution, because DFS doesn't handle
    // multiple writers to the same file.
    jobConf.setSpeculativeExecution(false);

    //setup input/output directories
    final Path inDir = new Path(TMP_DIR, "in");
    final Path outDir = new Path(TMP_DIR, "out");
    FileInputFormat.setInputPaths(jobConf, inDir);
    FileOutputFormat.setOutputPath(jobConf, outDir);

    final FileSystem fs = FileSystem.get(jobConf);
    if (fs.exists(TMP_DIR)) {
      throw new IOException("Tmp directory " + fs.makeQualified(TMP_DIR)
          + " already exists.  Please remove it first.");
    }
    if (!fs.mkdirs(inDir)) {
      throw new IOException("Cannot create input directory " + inDir);
    }

    try {
      //generate an input file for each map task
      for(int i=0; i < numMaps; ++i) {
        final Path file = new Path(inDir, "part"+i);
        final LongWritable offset = new LongWritable(i * numPoints);
        final LongWritable size = new LongWritable(numPoints);
        final SequenceFile.Writer writer = SequenceFile.createWriter(
            fs, jobConf, file,
            LongWritable.class, LongWritable.class, CompressionType.NONE);
        try {
          writer.append(offset, size);
        } finally {
          writer.close();
        }
        System.out.println("Wrote input for Map #"+i);
      }

      //start a map/reduce job
      System.out.println("Starting Job");
      final long startTime = System.currentTimeMillis();
      JobClient.runJob(jobConf); // Mapper and reducer set at beginning of this fn
      final double duration = (System.currentTimeMillis() - startTime)/1000.0;
      System.out.println("Job Finished in " + duration + " seconds");

      //read outputs
      Path inFile = new Path(outDir, "reduce-out");
      LongWritable numInside = new LongWritable();
      LongWritable numOutside = new LongWritable();
      SequenceFile.Reader reader = new SequenceFile.Reader(fs, inFile, jobConf);
      try {
        reader.next(numInside, numOutside);
      } finally {
        reader.close();
      }

      //compute estimated value
      return BigDecimal.valueOf(4).setScale(20)
          .multiply(BigDecimal.valueOf(numInside.get()))
          .divide(BigDecimal.valueOf(numMaps))
          .divide(BigDecimal.valueOf(numPoints));
    } finally {
      fs.delete(TMP_DIR, true);
    }
  }

  /**
   * Parse arguments and then runs a map/reduce job.
   * Print output in standard out.
   * 
   * @return a non-zero if there is an error.  Otherwise, return 0.  
   */
  public int run(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: "+getClass().getName()+" <nMaps> <nSamples>");
      ToolRunner.printGenericCommandUsage(System.err);
      return -1;
    }
    
    final int nMaps = Integer.parseInt(args[0]);
    final long nSamples = Long.parseLong(args[1]);
        
    System.out.println("Number of Maps  = " + nMaps);
    System.out.println("Samples per Map = " + nSamples);
        
    final JobConf jobConf = new JobConf(getConf(), getClass());
    System.out.println("Estimated value of Pi is "
        + estimate(nMaps, nSamples, jobConf));
    return 0;
  }

  /**
   * main method for running it as a stand alone command. 
   */
  public static void main(String[] argv) throws Exception {
    System.exit(ToolRunner.run(null, new PiEstimator(), argv));
  }
}
