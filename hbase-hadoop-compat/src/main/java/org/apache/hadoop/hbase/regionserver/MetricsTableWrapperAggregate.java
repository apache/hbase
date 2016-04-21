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
 * Interface of class that will wrap a MetricsTableSource and export numbers so they can be
 * used in MetricsTableSource
 */
public interface MetricsTableWrapperAggregate {

  /**
   * Get the number of read requests that have been issued against this table
   */
  long getReadRequestsCount(String table);

  /**
   * Get the number of write requests that have been issued against this table
   */
  long getWriteRequestsCount(String table);

  /**
   * Get the total number of requests that have been issued against this table
   */
  long getTotalRequestsCount(String table);

  /**
   * Get the memory store size against this table
   */
  long getMemstoresSize(String table);

  /**
   * Get the store file size against this table
   */
  long getStoreFilesSize(String table);

  /**
   * Get the table region size against this table
   */
  long getTableSize(String table);
}
