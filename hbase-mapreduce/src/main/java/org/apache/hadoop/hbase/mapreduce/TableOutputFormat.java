/*
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
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotEnabledException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Convert Map/Reduce output and write it to an HBase table. The KEY is ignored while the output
 * value <u>must</u> be either a {@link Put} or a {@link Delete} instance.
 */
@InterfaceAudience.Public
public class TableOutputFormat<KEY> extends OutputFormat<KEY, Mutation> implements Configurable {

  private static final Logger LOG = LoggerFactory.getLogger(TableOutputFormat.class);

  /** Job parameter that specifies the output table. */
  public static final String OUTPUT_TABLE = "hbase.mapred.outputtable";

  /** Property value to use write-ahead logging */
  public static final boolean WAL_ON = true;

  /** Property value to disable write-ahead logging */
  public static final boolean WAL_OFF = false;

  /** Set this to {@link #WAL_OFF} to turn off write-ahead logging (WAL) */
  public static final String WAL_PROPERTY = "hbase.mapreduce.tableoutputformat.write.wal";

  /**
   * Optional job parameter to specify a peer cluster. Used specifying remote cluster when copying
   * between hbase clusters (the source is picked up from <code>hbase-site.xml</code>).
   * @see TableMapReduceUtil#initTableReducerJob(String, Class, org.apache.hadoop.mapreduce.Job,
   *      Class, java.net.URI)
   */
  public static final String OUTPUT_CLUSTER = "hbase.mapred.outputcluster";

  /**
   * Prefix for configuration property overrides to apply in {@link #setConf(Configuration)}. For
   * keys matching this prefix, the prefix is stripped, and the value is set in the configuration
   * with the resulting key, ie. the entry "hbase.mapred.output.key1 = value1" would be set in the
   * configuration as "key1 = value1". Use this to set properties which should only be applied to
   * the {@code TableOutputFormat} configuration and not the input configuration.
   * @deprecated Since 3.0.0, will be removed in 4.0.0. You do not need to use this way for
   *             specifying configurations any more, you can specify any configuration with the
   *             connection uri's queries specified by the {@link #OUTPUT_CLUSTER} parameter.
   */
  @Deprecated
  public static final String OUTPUT_CONF_PREFIX = "hbase.mapred.output.";

  /**
   * Optional job parameter to specify a peer cluster. Used specifying remote cluster when copying
   * between hbase clusters (the source is picked up from <code>hbase-site.xml</code>).
   * @see TableMapReduceUtil#initTableReducerJob(String, Class, org.apache.hadoop.mapreduce.Job,
   *      Class, String)
   * @deprecated Since 3.0.0, will be removed in 4.0.0. Use {@link #OUTPUT_CLUSTER} to specify the
   *             peer cluster instead.
   */
  @Deprecated
  public static final String QUORUM_ADDRESS = OUTPUT_CONF_PREFIX + "quorum";

  /**
   * Optional job parameter to specify peer cluster's ZK client port.
   * @deprecated Since 3.0.0, will be removed in 4.0.0. You do not need to use this way for
   *             specifying configurations any more, you can specify any configuration with the
   *             connection uri's queries specified by the {@link #OUTPUT_CLUSTER} parameter.
   */
  @Deprecated
  public static final String QUORUM_PORT = OUTPUT_CONF_PREFIX + "quorum.port";

  /**
   * Optional specification of the rs class name of the peer cluster.
   * @deprecated Since 2.5.9, 2.6.1 and 2.7.0, will be removed in 4.0.0. Does not take effect from
   *             long ago, see HBASE-6044.
   */
  @Deprecated
  public static final String REGION_SERVER_CLASS = OUTPUT_CONF_PREFIX + "rs.class";
  /**
   * Optional specification of the rs impl name of the peer cluster
   * @deprecated Since 2.5.9, 2.6.1 and 2.7.0, will be removed in 4.0.0. Does not take effect from
   *             long ago, see HBASE-6044.
   */
  @Deprecated
  public static final String REGION_SERVER_IMPL = OUTPUT_CONF_PREFIX + "rs.impl";

  /** The configuration. */
  private Configuration conf = null;

  private static Connection createConnection(Configuration conf) throws IOException {
    String outputCluster = conf.get(OUTPUT_CLUSTER);
    if (!StringUtils.isBlank(outputCluster)) {
      URI uri;
      try {
        uri = new URI(outputCluster);
      } catch (URISyntaxException e) {
        throw new IOException(
          "malformed connection uri: " + outputCluster + ", please check config " + OUTPUT_CLUSTER,
          e);
      }
      return ConnectionFactory.createConnection(uri, conf);
    } else {
      return ConnectionFactory.createConnection(conf);
    }
  }

  /**
   * Writes the reducer output to an HBase table.
   */
  protected class TableRecordWriter extends RecordWriter<KEY, Mutation> {

    private Connection connection;
    private BufferedMutator mutator;
    boolean useWriteAheadLogging;

    public TableRecordWriter() throws IOException {
      this.connection = createConnection(conf);
      String tableName = conf.get(OUTPUT_TABLE);
      this.mutator = connection.getBufferedMutator(TableName.valueOf(tableName));
      LOG.info("Created table instance for " + tableName);
      this.useWriteAheadLogging = conf.getBoolean(WAL_PROPERTY, WAL_ON);
    }

    /**
     * Closes the writer, in this case flush table commits.
     * @param context The context.
     * @throws IOException When closing the writer fails.
     * @see RecordWriter#close(TaskAttemptContext)
     */
    @Override
    public void close(TaskAttemptContext context) throws IOException {
      try {
        if (mutator != null) {
          mutator.close();
        }
      } finally {
        if (connection != null) {
          connection.close();
        }
      }
    }

    /**
     * Writes a key/value pair into the table.
     * @param key   The key.
     * @param value The value.
     * @throws IOException When writing fails.
     * @see RecordWriter#write(Object, Object)
     */
    @Override
    public void write(KEY key, Mutation value) throws IOException {
      if (!(value instanceof Put) && !(value instanceof Delete)) {
        throw new IOException("Pass a Delete or a Put");
      }
      if (!useWriteAheadLogging) {
        value.setDurability(Durability.SKIP_WAL);
      }
      mutator.mutate(value);
    }
  }

  /**
   * Creates a new record writer. Be aware that the baseline javadoc gives the impression that there
   * is a single {@link RecordWriter} per job but in HBase, it is more natural if we give you a new
   * RecordWriter per call of this method. You must close the returned RecordWriter when done.
   * Failure to do so will drop writes.
   * @param context The current task context.
   * @return The newly created writer instance.
   * @throws IOException          When creating the writer fails.
   * @throws InterruptedException When the job is cancelled.
   */
  @Override
  public RecordWriter<KEY, Mutation> getRecordWriter(TaskAttemptContext context)
    throws IOException, InterruptedException {
    return new TableRecordWriter();
  }

  /**
   * Checks if the output table exists and is enabled.
   * @param context The current context.
   * @throws IOException          When the check fails.
   * @throws InterruptedException When the job is aborted.
   * @see OutputFormat#checkOutputSpecs(JobContext)
   */
  @Override
  public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {
    Configuration hConf = getConf();
    if (hConf == null) {
      hConf = context.getConfiguration();
    }

    try (Connection connection = createConnection(hConf); Admin admin = connection.getAdmin()) {
      TableName tableName = TableName.valueOf(hConf.get(OUTPUT_TABLE));
      if (!admin.tableExists(tableName)) {
        throw new TableNotFoundException(
          "Can't write, table does not exist:" + tableName.getNameAsString());
      }

      if (!admin.isTableEnabled(tableName)) {
        throw new TableNotEnabledException(
          "Can't write, table is not enabled: " + tableName.getNameAsString());
      }
    }
  }

  /**
   * Returns the output committer.
   * @param context The current context.
   * @return The committer.
   * @throws IOException          When creating the committer fails.
   * @throws InterruptedException When the job is aborted.
   * @see OutputFormat#getOutputCommitter(TaskAttemptContext)
   */
  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext context)
    throws IOException, InterruptedException {
    return new TableOutputCommitter();
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration otherConf) {
    String tableName = otherConf.get(OUTPUT_TABLE);
    if (tableName == null || tableName.length() <= 0) {
      throw new IllegalArgumentException("Must specify table name");
    }

    String address = otherConf.get(QUORUM_ADDRESS);
    int zkClientPort = otherConf.getInt(QUORUM_PORT, 0);

    try {
      this.conf = HBaseConfiguration.createClusterConf(otherConf, address, OUTPUT_CONF_PREFIX);
      if (zkClientPort != 0) {
        this.conf.setInt(HConstants.ZOOKEEPER_CLIENT_PORT, zkClientPort);
      }
    } catch (IOException e) {
      LOG.error(e.toString(), e);
      throw new RuntimeException(e);
    }
  }
}
