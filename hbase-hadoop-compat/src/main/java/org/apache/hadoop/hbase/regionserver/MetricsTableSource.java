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

import java.io.Closeable;

import org.apache.yetus.audience.InterfaceAudience;

/**
 * This interface will be implemented to allow region server to push table metrics into
 * MetricsRegionAggregateSource that will in turn push data to the Hadoop metrics system.
 */
@InterfaceAudience.Private
public interface MetricsTableSource extends Comparable<MetricsTableSource>, Closeable {

  String TABLE_SIZE = "tableSize";
  String TABLE_SIZE_DESC = "Total size of the table in the region server";

  String getTableName();

  /**
   * Close the table's metrics as all the region are closing.
   */
  @Override
  void close();

  void registerMetrics();

  /**
   * Get the aggregate source to which this reports.
   */
  MetricsTableAggregateSource getAggregateSource();

  /**
   * Update the split transaction time histogram
   * @param t time it took, in milliseconds
   */
  void updateSplitTime(long t);

  /**
   * Increment number of a requested splits
   */
  void incrSplitRequest();

  /**
   * Increment number of successful splits
   */
  void incrSplitSuccess();

  /**
   * Update the flush time histogram
   * @param t time it took, in milliseconds
   */
  void updateFlushTime(long t);

  /**
   * Update the flush memstore size histogram
   * @param bytes the number of bytes in the memstore
   */
  void updateFlushMemstoreSize(long bytes);

  /**
   * Update the flush output file size histogram
   * @param bytes the number of bytes in the output file
   */
  void updateFlushOutputSize(long bytes);

  /**
   * Update the compaction time histogram, both major and minor
   * @param isMajor whether compaction is a major compaction
   * @param t time it took, in milliseconds
   */
  void updateCompactionTime(boolean isMajor, long t);

  /**
   * Update the compaction input number of files histogram
   * @param isMajor whether compaction is a major compaction
   * @param c number of files
   */
  void updateCompactionInputFileCount(boolean isMajor, long c);

  /**
   * Update the compaction total input file size histogram
   * @param isMajor whether compaction is a major compaction
   * @param bytes the number of bytes of the compaction input file
   */
  void updateCompactionInputSize(boolean isMajor, long bytes);

  /**
   * Update the compaction output number of files histogram
   * @param isMajor whether compaction is a major compaction
   * @param c number of files
   */
  void updateCompactionOutputFileCount(boolean isMajor, long c);

  /**
   * Update the compaction total output file size
   * @param isMajor whether compaction is a major compaction
   * @param bytes the number of bytes of the compaction input file
   */
  void updateCompactionOutputSize(boolean isMajor, long bytes);

}
