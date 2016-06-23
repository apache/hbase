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
 * Interface of class that will wrap an HRegion and export numbers so they can be
 * used in MetricsRegionSource
 */
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
  long getMemstoreSize();

  /**
   * Get the total size of the store files this region server is serving from.
   */
  long getStoreFileSize();

  /**
   * Get the total number of read requests that have been issued against this region
   */
  long getReadRequestCount();

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

  long getNumFilesCompacted();

  long getNumBytesCompacted();

  long getNumCompactionsCompleted();

  /**
   * Returns the total number of compactions that have been reported as failed on this region.
   * Note that a given compaction can be reported as both completed and failed if an exception
   * is thrown in the processing after {@code HRegion.compact()}.
   */
  long getNumCompactionsFailed();

  int getRegionHashCode();

  /**
   * Get the replica id of this region.
   */
  int getReplicaId();
}
