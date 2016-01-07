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
package org.apache.hadoop.hbase.mapred;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.UserProvider;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.zookeeper.ClusterId;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.security.token.Token;
import org.apache.zookeeper.KeeperException;

/**
 * Utility for {@link TableMap} and {@link TableReduce}
 */
@Deprecated
@SuppressWarnings("unchecked")
public class TableMapReduceUtil {

  /**
   * Use this before submitting a TableMap job. It will
   * appropriately set up the JobConf.
   *
   * @param table  The table name to read from.
   * @param columns  The columns to scan.
   * @param mapper  The mapper class to use.
   * @param outputKeyClass  The class of the output key.
   * @param outputValueClass  The class of the output value.
   * @param job  The current job configuration to adjust.
   */
  public static void initTableMapJob(String table, String columns,
    Class<? extends TableMap> mapper,
    Class<? extends WritableComparable> outputKeyClass,
    Class<? extends Writable> outputValueClass, JobConf job) {
    initTableMapJob(table, columns, mapper, outputKeyClass, outputValueClass, job, true);
  }

  /**
   * Use this before submitting a TableMap job. It will
   * appropriately set up the JobConf.
   *
   * @param table  The table name to read from.
   * @param columns  The columns to scan.
   * @param mapper  The mapper class to use.
   * @param outputKeyClass  The class of the output key.
   * @param outputValueClass  The class of the output value.
   * @param job  The current job configuration to adjust.
   * @param addDependencyJars upload HBase jars and jars for any of the configured
   *           job classes via the distributed cache (tmpjars).
   */
  public static void initTableMapJob(String table, String columns,
    Class<? extends TableMap> mapper,
    Class<? extends WritableComparable> outputKeyClass,
    Class<? extends Writable> outputValueClass, JobConf job, boolean addDependencyJars) {

    job.setInputFormat(TableInputFormat.class);
    job.setMapOutputValueClass(outputValueClass);
    job.setMapOutputKeyClass(outputKeyClass);
    job.setMapperClass(mapper);
    FileInputFormat.addInputPaths(job, table);
    job.set(TableInputFormat.COLUMN_LIST, columns);
    if (addDependencyJars) {
      try {
        addDependencyJars(job);
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    try {
      initCredentials(job);
    } catch (IOException ioe) {
      // just spit out the stack trace?  really?
      ioe.printStackTrace();
    }
  }

  /**
   * Use this before submitting a TableReduce job. It will
   * appropriately set up the JobConf.
   *
   * @param table  The output table.
   * @param reducer  The reducer class to use.
   * @param job  The current job configuration to adjust.
   * @throws IOException When determining the region count fails.
   */
  public static void initTableReduceJob(String table,
    Class<? extends TableReduce> reducer, JobConf job)
  throws IOException {
    initTableReduceJob(table, reducer, job, null);
  }

  /**
   * Use this before submitting a TableReduce job. It will
   * appropriately set up the JobConf.
   *
   * @param table  The output table.
   * @param reducer  The reducer class to use.
   * @param job  The current job configuration to adjust.
   * @param partitioner  Partitioner to use. Pass <code>null</code> to use
   * default partitioner.
   * @throws IOException When determining the region count fails.
   */
  public static void initTableReduceJob(String table,
    Class<? extends TableReduce> reducer, JobConf job, Class partitioner)
  throws IOException {
    initTableReduceJob(table, reducer, job, partitioner, true);
  }

  /**
   * Use this before submitting a TableReduce job. It will
   * appropriately set up the JobConf.
   *
   * @param table  The output table.
   * @param reducer  The reducer class to use.
   * @param job  The current job configuration to adjust.
   * @param partitioner  Partitioner to use. Pass <code>null</code> to use
   * default partitioner.
   * @param addDependencyJars upload HBase jars and jars for any of the configured
   *           job classes via the distributed cache (tmpjars).
   * @throws IOException When determining the region count fails.
   */
  public static void initTableReduceJob(String table,
    Class<? extends TableReduce> reducer, JobConf job, Class partitioner,
    boolean addDependencyJars) throws IOException {
    job.setOutputFormat(TableOutputFormat.class);
    job.setReducerClass(reducer);
    job.set(TableOutputFormat.OUTPUT_TABLE, table);
    job.setOutputKeyClass(ImmutableBytesWritable.class);
    job.setOutputValueClass(Put.class);
    if (partitioner == HRegionPartitioner.class) {
      job.setPartitionerClass(HRegionPartitioner.class);
      HTable outputTable = new HTable(HBaseConfiguration.create(job), table);
      int regions = outputTable.getRegionsInfo().size();
      if (job.getNumReduceTasks() > regions) {
        job.setNumReduceTasks(outputTable.getRegionsInfo().size());
      }
    } else if (partitioner != null) {
      job.setPartitionerClass(partitioner);
    }
    if (addDependencyJars) {
      addDependencyJars(job);
    }
    initCredentials(job);
  }

  public static void initCredentials(JobConf job) throws IOException {
    UserProvider provider = UserProvider.instantiate(job);

    if (provider.isHadoopSecurityEnabled()) {
      // propagate delegation related props from launcher job to MR job
      if (System.getenv("HADOOP_TOKEN_FILE_LOCATION") != null) {
        job.set("mapreduce.job.credentials.binary",
                System.getenv("HADOOP_TOKEN_FILE_LOCATION"));
      }
    }

    if (provider.isHBaseSecurityEnabled()) {
      try {
        User user = provider.getCurrent();
        Token<?> authToken = getAuthToken(job, user);
        if (authToken == null) {
          user.obtainAuthTokenForJob(job);
        } else {
          job.getCredentials().addToken(authToken.getService(), authToken);
        }
      } catch (InterruptedException ie) {
        ie.printStackTrace();
        Thread.interrupted();
      }
    }
  }

  /**
   * Get the authentication token of the user for the cluster specified in the configuration
   * @return null if the user does not have the token, otherwise the auth token for the cluster.
   */
  private static Token<?> getAuthToken(Configuration conf, User user)
      throws IOException, InterruptedException {
    ZooKeeperWatcher zkw = new ZooKeeperWatcher(conf, "mr-init-credentials", null);
    try {
      String clusterId = ClusterId.readClusterIdZNode(zkw);
      return user.getToken("HBASE_AUTH_TOKEN", clusterId);
    } catch (KeeperException e) {
      throw new IOException(e);
    } finally {
      zkw.close();
    }
  }

  /**
   * Ensures that the given number of reduce tasks for the given job
   * configuration does not exceed the number of regions for the given table.
   *
   * @param table  The table to get the region count for.
   * @param job  The current job configuration to adjust.
   * @throws IOException When retrieving the table details fails.
   */
  public static void limitNumReduceTasks(String table, JobConf job)
  throws IOException {
    HTable outputTable = new HTable(HBaseConfiguration.create(job), table);
    int regions = outputTable.getRegionsInfo().size();
    if (job.getNumReduceTasks() > regions)
      job.setNumReduceTasks(regions);
  }

  /**
   * Ensures that the given number of map tasks for the given job
   * configuration does not exceed the number of regions for the given table.
   *
   * @param table  The table to get the region count for.
   * @param job  The current job configuration to adjust.
   * @throws IOException When retrieving the table details fails.
   */
  public static void limitNumMapTasks(String table, JobConf job)
  throws IOException {
    HTable outputTable = new HTable(HBaseConfiguration.create(job), table);
    int regions = outputTable.getRegionsInfo().size();
    if (job.getNumMapTasks() > regions)
      job.setNumMapTasks(regions);
  }

  /**
   * Sets the number of reduce tasks for the given job configuration to the
   * number of regions the given table has.
   *
   * @param table  The table to get the region count for.
   * @param job  The current job configuration to adjust.
   * @throws IOException When retrieving the table details fails.
   */
  public static void setNumReduceTasks(String table, JobConf job)
  throws IOException {
    HTable outputTable = new HTable(HBaseConfiguration.create(job), table);
    int regions = outputTable.getRegionsInfo().size();
    job.setNumReduceTasks(regions);
  }

  /**
   * Sets the number of map tasks for the given job configuration to the
   * number of regions the given table has.
   *
   * @param table  The table to get the region count for.
   * @param job  The current job configuration to adjust.
   * @throws IOException When retrieving the table details fails.
   */
  public static void setNumMapTasks(String table, JobConf job)
  throws IOException {
    HTable outputTable = new HTable(HBaseConfiguration.create(job), table);
    int regions = outputTable.getRegionsInfo().size();
    job.setNumMapTasks(regions);
  }

  /**
   * Sets the number of rows to return and cache with each scanner iteration.
   * Higher caching values will enable faster mapreduce jobs at the expense of
   * requiring more heap to contain the cached rows.
   *
   * @param job The current job configuration to adjust.
   * @param batchSize The number of rows to return in batch with each scanner
   * iteration.
   */
  public static void setScannerCaching(JobConf job, int batchSize) {
    job.setInt("hbase.client.scanner.caching", batchSize);
  }

  /**
   * @see org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil#addDependencyJars(org.apache.hadoop.mapreduce.Job)
   */
  public static void addDependencyJars(JobConf job) throws IOException {
    org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil.addHBaseDependencyJars(job);
    org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil.addDependencyJars(
      job,
      // when making changes here, consider also mapreduce.TableMapReduceUtil
      job.getMapOutputKeyClass(),
      job.getMapOutputValueClass(),
      job.getOutputKeyClass(),
      job.getOutputValueClass(),
      job.getPartitionerClass(),
      job.getClass("mapred.input.format.class", TextInputFormat.class, InputFormat.class),
      job.getClass("mapred.output.format.class", TextOutputFormat.class, OutputFormat.class),
      job.getCombinerClass());
  }
}
