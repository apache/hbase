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

package org.apache.hadoop.hbase.regionserver;


/**
 * This interface will be implemented to allow single regions to push metrics into
 * MetricsRegionAggregateSource that will in turn push data to the Hadoop metrics system.
 */
public interface MetricsRegionSource extends Comparable<MetricsRegionSource> {

  String OPS_SAMPLE_NAME = "ops";
  String SIZE_VALUE_NAME = "size";
  String COMPACTIONS_COMPLETED_COUNT = "compactionsCompletedCount";
  String NUM_BYTES_COMPACTED_COUNT = "numBytesCompactedCount";
  String NUM_FILES_COMPACTED_COUNT = "numFilesCompactedCount";
  String COMPACTIONS_COMPLETED_DESC = "Number of compactions that have completed.";
  String  NUM_BYTES_COMPACTED_DESC =
      "Sum of filesize on all files entering a finished, successful or aborted, compaction";
  String NUM_FILES_COMPACTED_DESC =
      "Number of files that were input for finished, successful or aborted, compactions";
  String COPROCESSOR_EXECUTION_STATISTICS = "coprocessorExecutionStatistics";
  String COPROCESSOR_EXECUTION_STATISTICS_DESC = "Statistics for coprocessor execution times";
  String REPLICA_ID = "replicaid";
  String REPLICA_ID_DESC = "The replica ID of a region. 0 is primary, otherwise is secondary";

  /**
   * Close the region's metrics as this region is closing.
   */
  void close();

  /**
   * Update related counts of puts.
   */
  void updatePut();

  /**
   * Update related counts of deletes.
   */
  void updateDelete();

  /**
   * Update count and sizes of gets.
   * @param getSize size in bytes of the resulting key values for a get
   */
  void updateGetSize(long getSize);

  /**
   * Update time of gets
   * @param mills time for this get operation.
   */
  void updateGet(long mills);

  /**
   * Update the count and sizes of resultScanner.next()
   * @param scanSize Size in bytes of the resulting key values for a next()
   */
  void updateScanSize(long scanSize);

  /**
   * Update time used of resultScanner.next().
   * */
  void updateScanTime(long mills);

  /**
   * Update related counts of increments.
   */
  void updateIncrement();

  /**
   * Update related counts of appends.
   */
  void updateAppend();

  /**
   * Get the aggregate source to which this reports.
   */
  MetricsRegionAggregateSource getAggregateSource();


}
