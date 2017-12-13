/**
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

import java.io.IOException;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Triple;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Export an HBase table.
 * Writes content to sequence files up in HDFS.  Use {@link Import} to read it
 * back in again.
 */
@InterfaceAudience.Public
public class Export extends Configured implements Tool {
  static final String NAME = "export";
  static final String JOB_NAME_CONF_KEY = "mapreduce.job.name";

  /**
   * Sets up the actual job.
   *
   * @param conf  The current configuration.
   * @param args  The command line parameters.
   * @return The newly created job.
   * @throws IOException When setting up the job fails.
   */
  public static Job createSubmittableJob(Configuration conf, String[] args)
  throws IOException {
    Triple<TableName, Scan, Path> arguments = ExportUtils.getArgumentsFromCommandLine(conf, args);
    String tableName = arguments.getFirst().getNameAsString();
    Path outputDir = arguments.getThird();
    Job job = Job.getInstance(conf, conf.get(JOB_NAME_CONF_KEY, NAME + "_" + tableName));
    job.setJobName(NAME + "_" + tableName);
    job.setJarByClass(Export.class);
    // Set optional scan parameters
    Scan s = arguments.getSecond();
    IdentityTableMapper.initJob(tableName, s, IdentityTableMapper.class, job);
    // No reducers.  Just write straight to output files.
    job.setNumReduceTasks(0);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    job.setOutputKeyClass(ImmutableBytesWritable.class);
    job.setOutputValueClass(Result.class);
    FileOutputFormat.setOutputPath(job, outputDir); // job conf doesn't contain the conf so doesn't have a default fs.
    return job;
  }

  @Override
  public int run(String[] args) throws Exception {
    if (!ExportUtils.isValidArguements(args)) {
      ExportUtils.usage("Wrong number of arguments: " + ArrayUtils.getLength(args));
      System.err.println("   -D " + JOB_NAME_CONF_KEY
              + "=jobName - use the specified mapreduce job name for the export");
      System.err.println("For MR performance consider the following properties:");
      System.err.println("   -D mapreduce.map.speculative=false");
      System.err.println("   -D mapreduce.reduce.speculative=false");
      return -1;
    }
    Job job = createSubmittableJob(getConf(), args);
    return (job.waitForCompletion(true) ? 0 : 1);
  }

  /**
   * Main entry point.
   * @param args The command line parameters.
   * @throws Exception When running the job fails.
   */
  public static void main(String[] args) throws Exception {
    int errCode = ToolRunner.run(HBaseConfiguration.create(), new Export(), args);
    System.exit(errCode);
  }
}
