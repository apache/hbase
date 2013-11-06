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
package org.apache.hadoop.hbase.mapred;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

/**
 * A Base for {@link TableInputFormat}s. Receives a {@link HTable}, a
 * byte[] of input columns and optionally a {@link Filter}.
 * Subclasses may use other TableRecordReader implementations.
 * <p>
 * An example of a subclass:
 * <pre>
 *   class ExampleTIF extends TableInputFormatBase implements JobConfigurable {
 *
 *     public void configure(JobConf job) {
 *       HTable exampleTable = new HTable(new HBaseConfiguration(job),
 *         Bytes.toBytes("exampleTable"));
 *       // mandatory
 *       setHTable(exampleTable);
 *       Text[] inputColumns = new byte [][] { Bytes.toBytes("columnA"),
 *         Bytes.toBytes("columnB") };
 *       // mandatory
 *       setInputColumns(inputColumns);
 *       RowFilterInterface exampleFilter = new RegExpRowFilter("keyPrefix.*");
 *       // optional
 *       setRowFilter(exampleFilter);
 *     }
 *
 *     public void validateInput(JobConf job) throws IOException {
 *     }
 *  }
 * </pre>
 */

@Deprecated
public abstract class TableInputFormatBase
implements InputFormat<ImmutableBytesWritable, Result> {
  final Log LOG = LogFactory.getLog(TableInputFormatBase.class);
  private byte [][] inputColumns;
  private HTable table;
  private TableRecordReader tableRecordReader;
  private Filter rowFilter;

  /** Holds the details for the internal scanner. */
  private Scan scan;

  /** The number of mappers to assign to each region. */
  private int numMappers = 1;

  /** The total number of mappers. The number of mappers per region is
   * mutually exclusive with number of mappers per job. In case both
   * are defined, mapperPerJob will take the precedence.*/
  private boolean mappersPerJob = false;

  /** Splitting algorithm to be used to split the keys */
  private String splitAlgmName; // default to Uniform

  /**
   * Builds a TableRecordReader. If no TableRecordReader was provided, uses
   * the default.
   *
   * @see org.apache.hadoop.mapred.InputFormat#getRecordReader(InputSplit,
   *      JobConf, Reporter)
   */
  public RecordReader<ImmutableBytesWritable, Result> getRecordReader(
      InputSplit split, JobConf job, Reporter reporter)
  throws IOException {
    TableSplit tSplit = (TableSplit) split;
    TableRecordReader trr = this.tableRecordReader;
    // if no table record reader was provided use default
    if (trr == null) {
      trr = new TableRecordReader(job);
    }
    trr.setStartRow(tSplit.getStartRow());
    trr.setEndRow(tSplit.getEndRow());
    trr.setHTable(this.table);
    trr.setScan(scan);
    trr.setInputColumns(this.inputColumns);
    trr.setRowFilter(this.rowFilter);
    trr.setTimeoutRetryNumber(
      job.getInt("hbase.mapred.client.timeoutretry.number", 3));
    trr.setTimeoutRetrySleepBaseMs(
      job.getInt("hbase.mapred.client.timeoutretry.sleepbasems", 5000));
    trr.init();
    return trr;
  }

  /**
   * Calculates the splits that will serve as input for the map tasks.
   * <ul>
   * Splits are created in number equal to the smallest between numSplits and
   * the number of {@link HRegion}s in the table. If the number of splits is
   * smaller than the number of {@link HRegion}s then splits are spanned across
   * multiple {@link HRegion}s and are grouped the most evenly possible. In the
   * case splits are uneven the bigger splits are placed first in the
   * {@link InputSplit} array.
   *
   * @param job the map task {@link JobConf}
   * @param numSplits a hint to calculate the number of splits (mapred.map.tasks).
   *
   * @return the input splits
   *
   * @see org.apache.hadoop.mapred.InputFormat#getSplits(org.apache.hadoop.mapred.JobConf, int)
   */
  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
    List<org.apache.hadoop.mapreduce.InputSplit> newStyleSplits =
        org.apache.hadoop.hbase.mapreduce.TableInputFormatBase.getSplitsInternal(
            table, job, scan, numMappers, mappersPerJob, splitAlgmName, null);
    int n = newStyleSplits.size();
    InputSplit[] result = new InputSplit[n];
    for (int i = 0; i < n; ++i) {
      org.apache.hadoop.hbase.mapreduce.TableSplit newTableSplit =
          (org.apache.hadoop.hbase.mapreduce.TableSplit) newStyleSplits.get(i);
      result[i] = new TableSplit(table.getTableName(),
          newTableSplit.getStartRow(),
          newTableSplit.getEndRow(),
          newTableSplit.getRegionLocation());
    }
    return result;
  }

  /**
   * @param inputColumns to be passed in {@link Result} to the map task.
   */
  protected void setInputColumns(byte [][] inputColumns) {
    this.inputColumns = inputColumns;
  }

  /**
   * Allows subclasses to get the {@link HTable}.
   */
  protected HTable getHTable() {
    return this.table;
  }

  /**
   * Allows subclasses to set the {@link HTable}.
   *
   * @param table to get the data from
   */
  protected void setHTable(HTable table) {
    this.table = table;
  }

  /**
   * Allows subclasses to set the {@link TableRecordReader}.
   *
   * @param tableRecordReader
   *                to provide other {@link TableRecordReader} implementations.
   */
  protected void setTableRecordReader(TableRecordReader tableRecordReader) {
    this.tableRecordReader = tableRecordReader;
  }

  /**
   * Allows subclasses to set the {@link Filter} to be used.
   *
   * @param rowFilter
   */
  protected void setRowFilter(Filter rowFilter) {
    this.rowFilter = rowFilter;
  }

  /**
   * Sets the scan defining the actual details like columns etc.
   *
   * @param scan  The scan to set.
   */
  public void setScan(Scan scan) {
    this.scan = scan;
  }

  /**
   * Sets the number of mappers assigned to each region.
   *
   * @param num
   * @throws IllegalArgumentException When <code>num</code> <= 0.
   */
  public void setNumMapperPerRegion(int num) throws IllegalArgumentException {
    if (num <= 0) {
      throw new IllegalArgumentException("Expecting at least 1 mapper " +
          "per region; instead got: " + num);
    }
    if (!mappersPerJob) {
      numMappers = num;
    } else {
      LOG.warn("Ignoring mappersPerRegion config value as mappersPerJob is" +
        " already set.");
    }
  }

  /**
   * Sets the number of mappers per job.
   *
   * @param num
   * @throws IllegalArgumentException When <code>num</code> <= 0.
   */
  public void setNumMappersPerJob(int num) throws IllegalArgumentException {
    if (num <= 0) {
      throw new IllegalArgumentException("Expecting at least 1 mapper " +
          "per region; instead got: " + num);
    }
    if (numMappers > 1) {
      LOG.warn("Overriding num of mappers per region.");
    }
    numMappers = num;
    mappersPerJob = true;
  }

  public void setSplitAlgorithm(String name) {
    this.splitAlgmName = name;
  }
}
