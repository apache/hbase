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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.mapred.InvalidJobConfException;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;

/**
 * Convert Map/Reduce output and write it to an HBase table
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class TableOutputFormat extends FileOutputFormat<ImmutableBytesWritable, Put> {

  /** JobConf parameter that specifies the output table */
  public static final String OUTPUT_TABLE = "hbase.mapred.outputtable";
  private static final Log LOG = LogFactory.getLog(TableOutputFormat.class);

  /**
   * Convert Reduce output (key, value) to (HStoreKey, KeyedDataArrayWritable)
   * and write to an HBase table.
   */
  protected static class TableRecordWriter implements RecordWriter<ImmutableBytesWritable, Put> {
    private final Connection conn;
    private final Table table;

    /**
     * Instantiate a TableRecordWriter with the HBase HClient for writing. Assumes control over the
     * lifecycle of {@code conn}.
     */
    public TableRecordWriter(Connection conn, TableName tableName) throws IOException {
      this.conn = conn;
      this.table = conn.getTable(tableName);
      ((HTable) this.table).setAutoFlush(false, true);
    }

    public void close(Reporter reporter) throws IOException {
      table.close();
      conn.close();
    }

    public void write(ImmutableBytesWritable key, Put value) throws IOException {
      table.put(new Put(value));
    }
  }

  @Override
  public RecordWriter<ImmutableBytesWritable, Put> getRecordWriter(FileSystem ignored, JobConf job,
      String name, Progressable progress) throws IOException {
    TableName tableName = TableName.valueOf(job.get(OUTPUT_TABLE));
    Connection conn = null;
    try {
      conn = ConnectionFactory.createConnection(HBaseConfiguration.create(job));
    } catch(IOException e) {
      LOG.error(e);
      throw e;
    }
    return new TableRecordWriter(conn, tableName);
  }

  @Override
  public void checkOutputSpecs(FileSystem ignored, JobConf job)
      throws FileAlreadyExistsException, InvalidJobConfException, IOException {
    String tableName = job.get(OUTPUT_TABLE);
    if(tableName == null) {
      throw new IOException("Must specify table name");
    }
  }
}
