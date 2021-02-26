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

import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.util.StringUtils;

/**
 * Convert HBase tabular data into a format that is consumable by Map/Reduce.
 */
@InterfaceAudience.Public
public class TableInputFormat extends TableInputFormatBase implements
    JobConfigurable {
  private static final Logger LOG = LoggerFactory.getLogger(TableInputFormat.class);

  /**
   * space delimited list of columns
   */
  public static final String COLUMN_LIST = "hbase.mapred.tablecolumns";

  public void configure(JobConf job) {
    try {
      initialize(job);
    } catch (Exception e) {
      LOG.error(StringUtils.stringifyException(e));
    }
  }

  @Override
  protected void initialize(JobConf job) throws IOException {
    Path[] tableNames = FileInputFormat.getInputPaths(job);
    String colArg = job.get(COLUMN_LIST);
    String[] colNames = colArg.split(" ");
    byte [][] m_cols = new byte[colNames.length][];
    for (int i = 0; i < m_cols.length; i++) {
      m_cols[i] = Bytes.toBytes(colNames[i]);
    }
    setInputColumns(m_cols);
    Connection connection = ConnectionFactory.createConnection(job);
    initializeTable(connection, TableName.valueOf(tableNames[0].getName()));
  }

  public void validateInput(JobConf job) throws IOException {
    // expecting exactly one path
    Path [] tableNames = FileInputFormat.getInputPaths(job);
    if (tableNames == null || tableNames.length > 1) {
      throw new IOException("expecting one table name");
    }

    // connected to table?
    if (getTable() == null) {
      throw new IOException("could not connect to table '" +
        tableNames[0].getName() + "'");
    }

    // expecting at least one column
    String colArg = job.get(COLUMN_LIST);
    if (colArg == null || colArg.length() == 0) {
      throw new IOException("expecting at least one column");
    }
  }
}
