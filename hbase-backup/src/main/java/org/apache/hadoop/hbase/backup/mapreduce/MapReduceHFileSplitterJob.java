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
package org.apache.hadoop.hbase.backup.mapreduce;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.TableName;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileInputFormat;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.KeyValueSortReducer;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * A tool to split HFiles into new region boundaries as a MapReduce job. The tool generates HFiles
 * for later bulk importing.
 */
@InterfaceAudience.Private
public class MapReduceHFileSplitterJob extends Configured implements Tool {
  private static final Log LOG = LogFactory.getLog(MapReduceHFileSplitterJob.class);
  final static String NAME = "HFileSplitterJob";
  public final static String BULK_OUTPUT_CONF_KEY = "hfile.bulk.output";
  public final static String TABLES_KEY = "hfile.input.tables";
  public final static String TABLE_MAP_KEY = "hfile.input.tablesmap";
  private final static String JOB_NAME_CONF_KEY = "mapreduce.job.name";

  public MapReduceHFileSplitterJob() {
  }

  protected MapReduceHFileSplitterJob(final Configuration c) {
    super(c);
  }

  /**
   * A mapper that just writes out cells. This one can be used together with
   * {@link KeyValueSortReducer}
   */
  static class HFileCellMapper extends
      Mapper<NullWritable, KeyValue, ImmutableBytesWritable, KeyValue> {

    @Override
    public void map(NullWritable key, KeyValue value, Context context) throws IOException,
        InterruptedException {
      // Convert value to KeyValue if subclass
      if (!value.getClass().equals(KeyValue.class)) {
        value =
            new KeyValue(value.getRowArray(), value.getRowOffset(), value.getRowLength(),
                value.getFamilyArray(), value.getFamilyOffset(), value.getFamilyLength(),
                value.getQualifierArray(), value.getQualifierOffset(), value.getQualifierLength(),
                value.getTimestamp(), Type.codeToType(value.getTypeByte()), value.getValueArray(),
                value.getValueOffset(), value.getValueLength());
      }
      context.write(new ImmutableBytesWritable(CellUtil.cloneRow(value)), value);
    }

    @Override
    public void setup(Context context) throws IOException {
      // do nothing
    }
  }

  /**
   * Sets up the actual job.
   * @param args The command line parameters.
   * @return The newly created job.
   * @throws IOException When setting up the job fails.
   */
  public Job createSubmittableJob(String[] args) throws IOException {
    Configuration conf = getConf();
    String inputDirs = args[0];
    String tabName = args[1];
    conf.setStrings(TABLES_KEY, tabName);
    conf.set(FileInputFormat.INPUT_DIR, inputDirs);
    Job job =
        Job.getInstance(conf,
          conf.get(JOB_NAME_CONF_KEY, NAME + "_" + EnvironmentEdgeManager.currentTime()));
    job.setJarByClass(MapReduceHFileSplitterJob.class);
    job.setInputFormatClass(HFileInputFormat.class);
    job.setMapOutputKeyClass(ImmutableBytesWritable.class);
    String hfileOutPath = conf.get(BULK_OUTPUT_CONF_KEY);
    if (hfileOutPath != null) {
      LOG.debug("add incremental job :" + hfileOutPath + " from " + inputDirs);
      TableName tableName = TableName.valueOf(tabName);
      job.setMapperClass(HFileCellMapper.class);
      job.setReducerClass(KeyValueSortReducer.class);
      Path outputDir = new Path(hfileOutPath);
      FileOutputFormat.setOutputPath(job, outputDir);
      job.setMapOutputValueClass(KeyValue.class);
      try (Connection conn = ConnectionFactory.createConnection(conf);
          Table table = conn.getTable(tableName);
          RegionLocator regionLocator = conn.getRegionLocator(tableName)) {
        HFileOutputFormat2.configureIncrementalLoad(job, table.getDescriptor(), regionLocator);
      }
      LOG.debug("success configuring load incremental job");

      TableMapReduceUtil.addDependencyJars(job.getConfiguration(),
        org.apache.hadoop.hbase.shaded.com.google.common.base.Preconditions.class);
    } else {
      throw new IOException("No bulk output directory specified");
    }
    return job;
  }

  /**
   * Print usage
   * @param errorMsg Error message. Can be null.
   */
  private void usage(final String errorMsg) {
    if (errorMsg != null && errorMsg.length() > 0) {
      System.err.println("ERROR: " + errorMsg);
    }
    System.err.println("Usage: " + NAME + " [options] <HFile inputdir(s)> <table>");
    System.err.println("Read all HFile's for <table> and split them to <table> region boundaries.");
    System.err.println("<table>  table to load.\n");
    System.err.println("To generate HFiles for a bulk data load, pass the option:");
    System.err.println("  -D" + BULK_OUTPUT_CONF_KEY + "=/path/for/output");
    System.err.println("Other options:");
    System.err.println("   -D " + JOB_NAME_CONF_KEY
        + "=jobName - use the specified mapreduce job name for the HFile splitter");
    System.err.println("For performance also consider the following options:\n"
        + "  -Dmapreduce.map.speculative=false\n" + "  -Dmapreduce.reduce.speculative=false");
  }

  /**
   * Main entry point.
   * @param args The command line parameters.
   * @throws Exception When running the job fails.
   */
  public static void main(String[] args) throws Exception {
    int ret = ToolRunner.run(new MapReduceHFileSplitterJob(HBaseConfiguration.create()), args);
    System.exit(ret);
  }

  @Override
  public int run(String[] args) throws Exception {
    if (args.length < 2) {
      usage("Wrong number of arguments: " + args.length);
      System.exit(-1);
    }
    Job job = createSubmittableJob(args);
    int result = job.waitForCompletion(true) ? 0 : 1;
    return result;
  }
}
