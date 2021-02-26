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

import java.util.Map;

import org.apache.yetus.audience.InterfaceAudience;

/**
 * Interface of class that will wrap an HRegion and export numbers so they can be
 * used in MetricsRegionSource
 */
@InterfaceAudience.Private
public interface MetricsRegionWrapper {

  /**
   * Get the name of the table the region belongs to.
   *
   * @return The string version of the table name.
   */
  String getTableName();

  /**
   * Get the name of the namespace this table is in.
   * @return String version of the namespace.  Can't be empty.
   */
  String getNamespace();

  /**
   * Get the name of the region.
   *
   * @return The encoded name of the region.
   */
  String getRegionName();

  /**
   * Get the number of stores hosted on this region server.
   */
  long getNumStores();

  /**
   * Get the number of store files hosted on this region server.
   */
  long getNumStoreFiles();

  /**
   * Get the size of the memstore on this region server.
   */
  long getMemStoreSize();

  /**
   * Get the total size of the store files this region server is serving from.
   */
  long getStoreFileSize();

  /**
   * Get the total number of read requests that have been issued against this region
   */
  long getReadRequestCount();

  /**
   * Get the total number of filtered read requests that have been issued against this region
   */
  long getFilteredReadRequestCount();

  /**
   * @return Max age of store files under this region
   */
  long getMaxStoreFileAge();

  /**
   * @return Min age of store files under this region
   */
  long getMinStoreFileAge();

  /**
   *  @return Average age of store files under this region
   */
  long getAvgStoreFileAge();

  /**
   *  @return Number of reference files under this region
   */
  long getNumReferenceFiles();

  /**
   * Get the total number of mutations that have been issued against this region.
   */
  long getWriteRequestCount();

  long getTotalRequestCount();

  long getNumFilesCompacted();

  long getNumBytesCompacted();

  long getNumCompactionsCompleted();

  /**
   *  @return Age of the last major compaction
   */
  long getLastMajorCompactionAge();

  /**
   * Returns the total number of compactions that have been reported as failed on this region.
   * Note that a given compaction can be reported as both completed and failed if an exception
   * is thrown in the processing after {@code HRegion.compact()}.
   */
  long getNumCompactionsFailed();

  /**
   * @return the total number of compactions that are currently queued(or being executed) at point in
   *  time
   */
  long getNumCompactionsQueued();

  /**
   * @return the total number of flushes currently queued(being executed) for this region at point in
   *  time
   */
  long getNumFlushesQueued();

  /**
   * @return the max number of compactions queued for this region
   * Note that this metric is updated periodically and hence might miss some data points
   */
  long getMaxCompactionQueueSize();

  /**
   * @return the max number of flushes queued for this region
   * Note that this metric is updated periodically and hence might miss some data points
   */
  long getMaxFlushQueueSize();

  int getRegionHashCode();

  /**
   * Get the replica id of this region.
   */
  int getReplicaId();

  /**
   * @return the number of references active on the store
   */
  long getStoreRefCount();

  /**
   * @return the max number of references active on any store file among
   *   all compacted store files that belong to this region
   */
  long getMaxCompactedStoreFileRefCount();

  /**
   * @return the number of row reads completely on memstore per store
   */
  Map<String, Long> getMemstoreOnlyRowReadsCount();

  /**
   * @return the number of row reads on memstore and file per store
   */
  Map<String, Long> getMixedRowReadsCount();

}
