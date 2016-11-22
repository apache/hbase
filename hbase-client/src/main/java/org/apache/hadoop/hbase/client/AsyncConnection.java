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
package org.apache.hadoop.hbase.client;

import java.io.Closeable;
import java.util.concurrent.ExecutorService;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;

/**
 * The asynchronous version of Connection.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public interface AsyncConnection extends Closeable {

  /**
   * Returns the {@link org.apache.hadoop.conf.Configuration} object used by this instance.
   * <p>
   * The reference returned is not a copy, so any change made to it will affect this instance.
   */
  Configuration getConfiguration();

  /**
   * Retrieve a AsyncRegionLocator implementation to inspect region information on a table. The
   * returned AsyncRegionLocator is not thread-safe, so a new instance should be created for each
   * using thread. This is a lightweight operation. Pooling or caching of the returned
   * AsyncRegionLocator is neither required nor desired.
   * @param tableName Name of the table who's region is to be examined
   * @return An AsyncRegionLocator instance
   */
  AsyncTableRegionLocator getRegionLocator(TableName tableName);

  /**
   * Retrieve an RawAsyncTable implementation for accessing a table. The returned Table is not
   * thread safe, a new instance should be created for each using thread. This is a lightweight
   * operation, pooling or caching of the returned AsyncTable is neither required nor desired.
   * <p>
   * This method no longer checks table existence. An exception will be thrown if the table does not
   * exist only when the first operation is attempted.
   * @param tableName the name of the table
   * @return an RawAsyncTable to use for interactions with this table
   */
  RawAsyncTable getRawTable(TableName tableName);

  /**
   * Retrieve an AsyncTable implementation for accessing a table. The returned Table is not thread
   * safe, a new instance should be created for each using thread. This is a lightweight operation,
   * pooling or caching of the returned AsyncTable is neither required nor desired.
   * <p>
   * This method no longer checks table existence. An exception will be thrown if the table does not
   * exist only when the first operation is attempted.
   * @param tableName the name of the table
   * @param pool the thread pool to use for executing callback
   * @return an AsyncTable to use for interactions with this table
   */
  AsyncTable getTable(TableName tableName, ExecutorService pool);
}