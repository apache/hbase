/**
 * Copyright 2010 The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase.mapreduce;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.mortbay.log.Log;

/**
 * MR based tool to split HBase logs in a distributed fashion. 
 
 * The tool first creates a list of directories and puts them into 
 * a file in the HDFS root directory. This file is used as the input 
 * to the mappers, there are no reducers. Each mapper processes the 
 * directory it receives as input.
 */
@Deprecated
public class HLogSplitter {
  final static String NAME = "splitlogs";
  
  static class HLogSpliterMap 
  extends Mapper<LongWritable, Text, Text, Text> {

    /**
     * Takes an entire HLog directory of a region server as the input 
     * and splits the logs.
     *
     * @param key     The mapper input key
     * @param value   A log directory to split
     * @param context
     */
    @Override
    public void map(LongWritable key, Text value, Context context)
    throws IOException {
      try {
        Configuration conf = context.getConfiguration();
        String logsInputDir = value.toString();
        Path logInputPath = new Path(logsInputDir);
        Path baseDir = new Path(conf.get(HConstants.HBASE_DIR));
        Path logOutputPath = new Path(baseDir, HConstants.HREGION_OLDLOGDIR_NAME);
        Log.info("HLogs input dir      : " + logInputPath.toString());
        Log.info("Split log output dir : " + logOutputPath.toString());
        
        FileSystem fs = FileSystem.get(conf);
        if (!fs.exists(logInputPath)) {
          throw new FileNotFoundException(logInputPath.toString());
        }
        if (!fs.getFileStatus(logInputPath).isDir()) {
          throw new IOException(logInputPath + " is not a directory");
        }
        HLog.splitLog(baseDir, logInputPath, logOutputPath, fs, conf);

        context.write(new Text(value), new Text("Processed"));
      } catch (InterruptedException e) {
        throw new IOException(e);
      }
    }
  }


  /*
   * @param errorMsg Error message.  Can be null.
   */
  private static void usage(final String errorMsg) {
    if (errorMsg != null && errorMsg.length() > 0) {
      System.err.println("ERROR: " + errorMsg);
    }
    System.err.println("Usage: HLogSplitter <HLog directory>\n");
    System.err.println("Example:");
    System.err.println("  HLogSplitter hdfs://<namenode>:9000/TABLE-HBASE/.logs");
  }

  /**
   * Main entry point.
   *
   * @param args  The command line parameters.
   * @throws Exception When running the job fails.
   */
  public static void main(String[] args) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length < 1) {
      usage("Wrong number of arguments: " + otherArgs.length);
      System.exit(-1);
    }

    // this is the .logs directory that we want to split
    Path logsDirPath = new Path(otherArgs[0]);
    FileSystem fs = FileSystem.get(conf);

    if (!fs.exists(logsDirPath) || !fs.getFileStatus(logsDirPath).isDir()) {
      usage("Directory does not exist: " + logsDirPath);
      System.exit(-1);
    }
    
    // list all the directories in the .logs directory. Typically this there is 
    // one directory per regionserver.
    FileStatus[] logFolders = fs.listStatus(logsDirPath);
    if (logFolders == null || logFolders.length == 0) {
      usage("No log files to split in " + logsDirPath);
      System.exit(-1);
    }
    
    // write the list of RS directories to a temp file in HDFS. This will be the
    // input to the mapper.
    String jobInputFile = "/" + NAME + "_" + System.currentTimeMillis();
    String jobOutputDir = jobInputFile + "-output";
    Path jobInputPath = new Path(jobInputFile);
    Path jobOutputPath = new Path(jobOutputDir);
    FSDataOutputStream dos = fs.create(jobInputPath);
    PrintWriter out = new PrintWriter(dos);
    for (FileStatus status : logFolders) {
      out.println(status.getPath().toString());
    }
    out.close();
    dos.close();
    
    // create the job that will do the distributed log splitting
    Job job = new Job(conf, NAME + "_" + logsDirPath);
    job.setJobName(NAME + "_" + logsDirPath);
    job.setJarByClass(HLogSpliterMap.class);
    job.setMapperClass(HLogSpliterMap.class);
    job.setNumReduceTasks(0);
    job.setInputFormatClass(TextInputFormat.class);
    FileInputFormat.setInputPaths(job, jobInputFile);
    FileOutputFormat.setOutputPath(job, jobOutputPath);
    
    // submit the job
    boolean status = job.waitForCompletion(true);

    // delete jobInputFile and the output directory once we are done with the 
    // job
//    fs.delete(jobInputPath);

    // return the appropriate return code
    System.exit(status?0:1);
  }
}
