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

import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public interface MetricsStoreWrapper {
/**
 * @return the current store name
 */
  String getStoreName();

/**
 * @return the current region name
 */
  String getRegionName();

/**
 * @return the table name associated with the store
 */
  String getTableName();

/**
 * @return  the namespace associated with the store.
 */
  String getNamespace();

  /**
   * Get the number of store files hosted on this store.
   */
  long getNumStoreFiles();

  /**
   * Get the size of the memstore on this store.
   */
  long getMemStoreSize();

  /**
   * Get the total size of the store files this store is serving from.
   */
  long getStoreFileSize();

  /**
   * Get the total number of filtered read requests that have been issued against this store
   */
//  long getFilteredReadRequestCount();

  /**
   * @return Max age of store files under this store
   */
  long getMaxStoreFileAge();

  /**
   * @return Min age of store files under this store
   */
  long getMinStoreFileAge();

  /**
   *  @return Average age of store files under this store
   */
  long getAvgStoreFileAge();

  /**
   *  @return Number of reference files under this store
   */
  long getNumReferenceFiles();

  /**
   * @return the number of references active on the store
   */
  long getStoreRefCount();

  /**
   * Get the total number of read requests that have been issued against this store
   */
  long getReadRequestCount();

  /**
   * Get the number of read requests from the memstore 
   */
  long getMemstoreReadRequestsCount();
  
  /**
   * Get the number of read requests from the store files
   * @return
   */
  long getFileReadRequestCount();
}
