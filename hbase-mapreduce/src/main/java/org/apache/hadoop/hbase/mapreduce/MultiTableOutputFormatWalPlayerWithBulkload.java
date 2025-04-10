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

import static org.apache.hadoop.hbase.tool.BulkLoadHFiles.create;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.tool.BulkLoadHFiles;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 * Hadoop output format for WALPlayerWithBulkload that writes to one or more HBase tables or does
 * Bulkload operation. The key is taken to be the table name while the output value <em>must</em> be
 * either a {@link Put} or a {@link Delete} instance or a list of {@link BulkLoadHFiles}. All tables
 * must already exist, and all Puts and Deletes must reference only valid column families.
 * </p>
 * <p>
 * Write-ahead logging (WAL) for Puts can be disabled by setting {@link #WAL_PROPERTY} to
 * {@link #WAL_OFF}. Default value is {@link #WAL_ON}. Note that disabling write-ahead logging is
 * only appropriate for jobs where loss of data due to region server failure can be tolerated (for
 * example, because it is easy to rerun a bulk import).
 * </p>
 */
@InterfaceAudience.Public
public class MultiTableOutputFormatWalPlayerWithBulkload
  extends OutputFormat<ImmutableBytesWritable, MutationOrBulkLoad> {
  /** Set this to {@link #WAL_OFF} to turn off write-ahead logging (WAL) */
  public static final String WAL_PROPERTY = "hbase.mapreduce.multitableoutputformat.wal";
  /** Property value to use write-ahead logging */
  public static final boolean WAL_ON = true;
  /** Property value to disable write-ahead logging */
  public static final boolean WAL_OFF = false;

  /**
   * Record writer for outputting to multiple HTables.
   */
  protected static class MultiTableRecordWriter
    extends RecordWriter<ImmutableBytesWritable, MutationOrBulkLoad> {
    private static final Logger LOG = LoggerFactory.getLogger(MultiTableRecordWriter.class);
    Connection connection;
    Map<ImmutableBytesWritable, BufferedMutator> mutatorMap = new HashMap<>();
    Configuration conf;
    boolean useWriteAheadLogging;

    /**
     * HBaseConfiguration to used whether to use write ahead logging. This can be turned off (
     * <tt>false</tt>) to improve performance when bulk loading data.
     */
    public MultiTableRecordWriter(Configuration conf, boolean useWriteAheadLogging)
      throws IOException {
      LOG.debug(
        "Created new MultiTableRecordReader with WAL " + (useWriteAheadLogging ? "on" : "off"));
      this.conf = conf;
      this.useWriteAheadLogging = useWriteAheadLogging;
    }

    /**
     * the name of the table, as a string
     * @return the named mutator if there is a problem opening a table
     */
    BufferedMutator getBufferedMutator(ImmutableBytesWritable tableName) throws IOException {
      if (this.connection == null) {
        this.connection = ConnectionFactory.createConnection(conf);
      }
      if (!mutatorMap.containsKey(tableName)) {
        LOG.debug("Opening HTable \"" + Bytes.toString(tableName.get()) + "\" for writing");

        BufferedMutator mutator = connection.getBufferedMutator(TableName.valueOf(tableName.get()));
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
     * Writes an action (Put or Delete or Bulkload) to the specified table
     */
    @Override
    public void write(ImmutableBytesWritable tableName, MutationOrBulkLoad action)
      throws IOException {
      if (action.getMutation() != null) {
        handleMutation(tableName, action.getMutation());
        return;
      }
      handleBulkLoad(tableName, action.getBulkLoadFiles());
    }

    private void handleMutation(ImmutableBytesWritable tableName, Mutation action)
      throws IOException {
      BufferedMutator mutator = getBufferedMutator(tableName);
      // The actions are not immutable, so we defensively copy them
      if (action instanceof Put) {
        Put put = new Put((Put) action);
        put.setDurability(useWriteAheadLogging ? Durability.SYNC_WAL : Durability.SKIP_WAL);
        mutator.mutate(put);
      } else if (action instanceof Delete) {
        Delete delete = new Delete((Delete) action);
        mutator.mutate(delete);
      } else throw new IllegalArgumentException("action must be either Delete or Put");
    }

    private void handleBulkLoad(ImmutableBytesWritable tableName, List<String> bulkLoadFiles)
      throws IOException {

      TableName table = TableName.valueOf(tableName.get());
      LOG.debug("Starting bulk load for table: {}", table);

      BulkLoadHFiles bulkLoader = create(conf);
      LOG.info("Processing {} HFiles for bulk loading into table: {}", bulkLoadFiles.size(), table);

      // This map will hold the family-to-files mapping needed for the bulk load operation
      Map<byte[], List<Path>> family2Files = new HashMap<>();

      try {
        for (String file : bulkLoadFiles) {
          Path filePath = new Path(file);
          String family = filePath.getParent().getName();
          byte[] familyBytes = Bytes.toBytes(family);

          // Add the file to the list of files for the corresponding column family
          family2Files.computeIfAbsent(familyBytes, k -> new ArrayList<>()).add(filePath);
          LOG.debug("Mapped file {} to family {}", filePath, family);
        }

        LOG.debug("Executing bulk load into table: {}", table);
        bulkLoader.bulkLoad(table, family2Files);

        LOG.debug("Bulk load completed successfully for table: {}", table);
      } catch (IOException e) {
        LOG.error("Error during bulk load for table: {}. Exception: {}", table, e.getMessage(), e);
        throw new IOException("Failed to complete bulk load for table: " + table, e);
      }
    }
  }

  @Override
  public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {
    // we can't know ahead of time if it's going to blow up when the user
    // passes a table name that doesn't exist, so nothing useful here.
  }

  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext context)
    throws IOException, InterruptedException {
    return new TableOutputCommitter();
  }

  @Override
  public RecordWriter<ImmutableBytesWritable, MutationOrBulkLoad>
    getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
    Configuration conf = context.getConfiguration();
    return new MultiTableRecordWriter(HBaseConfiguration.create(conf),
      conf.getBoolean(WAL_PROPERTY, WAL_ON));
  }

}
