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

import java.util.concurrent.CompletableFuture;

import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;

/**
 * The asynchronous version of RegionLocator.
 * <p>
 * Usually the implementations will not throw any exception directly, you need to get the exception
 * from the returned {@link CompletableFuture}.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public interface AsyncTableRegionLocator {

  /**
   * Gets the fully qualified table name instance of the table whose region we want to locate.
   */
  TableName getName();

  /**
   * Finds the region on which the given row is being served. Does not reload the cache.
   * <p>
   * Returns the location of the region to which the row belongs.
   * @param row Row to find.
   */
  default CompletableFuture<HRegionLocation> getRegionLocation(byte[] row) {
    return getRegionLocation(row, false);
  }

  /**
   * Finds the region on which the given row is being served.
   * <p>
   * Returns the location of the region to which the row belongs.
   * @param row Row to find.
   * @param reload true to reload information or false to use cached information
   */
  CompletableFuture<HRegionLocation> getRegionLocation(byte[] row, boolean reload);
}
