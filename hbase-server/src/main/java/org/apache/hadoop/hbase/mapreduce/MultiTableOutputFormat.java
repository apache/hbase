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
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * <p>
 * Hadoop output format that writes to one or more HBase tables. The key is
 * taken to be the table name while the output value <em>must</em> be either a
 * {@link Put} or a {@link Delete} instance. All tables must already exist, and
 * all Puts and Deletes must reference only valid column families.
 * </p>
 *
 * <p>
 * Write-ahead logging (WAL) for Puts can be disabled by setting
 * {@link #WAL_PROPERTY} to {@link #WAL_OFF}. Default value is {@link #WAL_ON}.
 * Note that disabling write-ahead logging is only appropriate for jobs where
 * loss of data due to region server failure can be tolerated (for example,
 * because it is easy to rerun a bulk import).
 * </p>
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class MultiTableOutputFormat extends OutputFormat<ImmutableBytesWritable, Mutation> {
  /** Set this to {@link #WAL_OFF} to turn off write-ahead logging (WAL) */
  public static final String WAL_PROPERTY = "hbase.mapreduce.multitableoutputformat.wal";
  /** Property value to use write-ahead logging */
  public static final boolean WAL_ON = true;
  /** Property value to disable write-ahead logging */
  public static final boolean WAL_OFF = false;
  /**
   * Record writer for outputting to multiple HTables.
   */
  protected static class MultiTableRecordWriter extends
      RecordWriter<ImmutableBytesWritable, Mutation> {
    private static final Log LOG = LogFactory.getLog(MultiTableRecordWriter.class);
    Connection connection;
    Map<ImmutableBytesWritable, BufferedMutator> mutatorMap = new HashMap<>();
    Configuration conf;
    boolean useWriteAheadLogging;

    /**
     * @param conf
     *          HBaseConfiguration to used
     * @param useWriteAheadLogging
     *          whether to use write ahead logging. This can be turned off (
     *          <tt>false</tt>) to improve performance when bulk loading data.
     */
    public MultiTableRecordWriter(Configuration conf,
        boolean useWriteAheadLogging) {
      LOG.debug("Created new MultiTableRecordReader with WAL "
          + (useWriteAheadLogging ? "on" : "off"));
      this.conf = conf;
      this.useWriteAheadLogging = useWriteAheadLogging;
    }

    /**
     * @param tableName
     *          the name of the table, as a string
     * @return the named mutator
     * @throws IOException
     *           if there is a problem opening a table
     */
    BufferedMutator getBufferedMutator(ImmutableBytesWritable tableName) throws IOException {
      if(this.connection == null){
        this.connection = ConnectionFactory.createConnection(conf);
      }
      if (!mutatorMap.containsKey(tableName)) {
        LOG.debug("Opening HTable \"" + Bytes.toString(tableName.get())+ "\" for writing");

        BufferedMutator mutator =
            connection.getBufferedMutator(TableName.valueOf(tableName.get()));
        mutatorMap.put(tableName, mutator);
      }
      return mutatorMap.get(tableName);
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException {
      for (BufferedMutator mutator : mutatorMap.values()) {
        mutator.close();
      }
      if (connection != null) {
        connection.close();
      }
    }

    /**
     * Writes an action (Put or Delete) to the specified table.
     *
     * @param tableName
     *          the table being updated.
     * @param action
     *          the update, either a put or a delete.
     * @throws IllegalArgumentException
     *          if the action is not a put or a delete.
     */
    @Override
    public void write(ImmutableBytesWritable tableName, Mutation action) throws IOException {
      BufferedMutator mutator = getBufferedMutator(tableName);
      // The actions are not immutable, so we defensively copy them
      if (action instanceof Put) {
        Put put = new Put((Put) action);
        put.setDurability(useWriteAheadLogging ? Durability.SYNC_WAL
            : Durability.SKIP_WAL);
        mutator.mutate(put);
      } else if (action instanceof Delete) {
        Delete delete = new Delete((Delete) action);
        mutator.mutate(delete);
      } else
        throw new IllegalArgumentException(
            "action must be either Delete or Put");
    }
  }

  @Override
  public void checkOutputSpecs(JobContext context) throws IOException,
      InterruptedException {
    // we can't know ahead of time if it's going to blow up when the user
    // passes a table name that doesn't exist, so nothing useful here.
  }

  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext context)
      throws IOException, InterruptedException {
    return new TableOutputCommitter();
  }

  @Override
  public RecordWriter<ImmutableBytesWritable, Mutation> getRecordWriter(TaskAttemptContext context)
      throws IOException, InterruptedException {
    Configuration conf = context.getConfiguration();
    return new MultiTableRecordWriter(HBaseConfiguration.create(conf),
        conf.getBoolean(WAL_PROPERTY, WAL_ON));
  }

}
