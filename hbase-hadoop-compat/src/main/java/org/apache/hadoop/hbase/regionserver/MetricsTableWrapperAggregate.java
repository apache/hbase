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
 * Interface of class that will wrap a MetricsTableSource and export numbers so they can be
 * used in MetricsTableSource
 */
@InterfaceAudience.Private
public interface MetricsTableWrapperAggregate {
  public String HASH = "#";
  /**
   * Get the number of read requests that have been issued against this table
   */
  long getReadRequestCount(String table);

  /**
   * Get the number of write requests that have been issued against this table
   */
  long getFilteredReadRequestCount(String table);
  /**
   * Get the number of write requests that have been issued for this table
   */
  long getWriteRequestCount(String table);

  /**
   * Get the total number of requests that have been issued for this table
   */
  long getTotalRequestsCount(String table);

  /**
   * Get the memory store size against this table
   */
  long getMemStoreSize(String table);

  /**
   * Get the store file size against this table
   */
  long getStoreFileSize(String table);

  /**
   * Get the table region size against this table
   */
  long getTableSize(String table);


  /**
   * Get the average region size for this table
   */
  long getAvgRegionSize(String table);

  /**
   * Get the number of regions hosted on for this table
   */
  long getNumRegions(String table);

  /**
   * Get the number of stores hosted on for this table
   */
  long getNumStores(String table);

  /**
   * Get the number of store files hosted for this table
   */
  long getNumStoreFiles(String table);

  /**
   * @return Max age of store files for this table
   */
  long getMaxStoreFileAge(String table);

  /**
   * @return Min age of store files for this table
   */
  long getMinStoreFileAge(String table);

  /**
   *  @return Average age of store files for this table
   */
  long getAvgStoreFileAge(String table);

  /**
   *  @return Number of reference files for this table
   */
  long getNumReferenceFiles(String table);

  /**
   * @return number of row reads completely from memstore per store for this table
   */
  Map<String, Long> getMemstoreOnlyRowReadsCount(String table);

  /**
   * @return number of row reads from file and memstore per store for this table
   */
  Map<String, Long> getMixedRowReadsCount(String table);
}
