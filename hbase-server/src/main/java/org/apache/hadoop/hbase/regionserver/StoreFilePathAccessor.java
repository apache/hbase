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

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Helper class to interact with the hbase storefile tracking data persisted as off-memory data
 * from the {@link StoreFileManager}
 *
 * There are two set of tracking storefiles, 'included' and 'excluded'.
 *
 * e.g. list of storefile paths in 'included' should be the identical copy of the in-memory
 * {@link HStoreFile}'s Path(s) and can be reused during region opens and region
 * reassignment.
 *
 * list of storefile paths in 'excluded' is used for tracking compacted storefiles
 *
 */
@InterfaceAudience.Private
public interface StoreFilePathAccessor {

  /**
   * Create the storefile tracking with the help of using the masterService
   * @param masterServices instance of HMaster
   * @throws IOException if Master is not running or connection has been lost
   */
  void initialize(final MasterServices masterServices) throws IOException;

  /**
   * GET storefile paths from the 'included' data set
   * @param tableName name of the current table in String
   * @param regionName name of the current region in String
   * @param storeName name of the column family in String, to be combined with regionName to make
   *                 the row key.
   * @return list of StoreFile paths that should be included in reads in this store,
   *         returns an empty list if the target cell is empty or doesn't exist.
   * @throws IOException if a remote or network exception occurs during Get
   */
  List<Path> getIncludedStoreFilePaths(final String tableName, final String regionName,
      final String storeName) throws IOException;

  /**
   * GET storefile paths from the 'excluded' data set
   * @param tableName name of the current table in String
   * @param regionName name of the current region in String
   * @param storeName name of the column family in String, to be combined with regionName to make
   *                 the row key.
   * @return list of StoreFile paths that should be excluded from reads in this store,
   *         returns an empty list if the target cell is empty or doesn't exist.
   * @throws IOException if a remote or network exception occurs during Get
   */
  List<Path> getExcludedStoreFilePaths(final String tableName, final String regionName,
      final String storeName) throws IOException;

  /**
   * Write storefile paths to the 'included' data set
   * @param tableName name of the current table in String
   * @param regionName name of the current region in String
   * @param storeName name of the column family in String, to be combined with regionName to make
   *                 the row key.
   * @param storeFilePaths list of StoreFile paths representing files to be included in reads
   * @throws IOException if a remote or network exception occurs during Put
   */
  void writeIncludedStoreFilePaths(final String tableName, final String regionName,
      final String storeName, final List<Path> storeFilePaths) throws IOException;

  /**
   * Write storefile paths to the 'excluded' data set
   * @param tableName name of the current table in String
   * @param regionName name of the current region in String
   * @param storeName name of the column family in String, to be combined with regionName to make
   *                 the row key.
   * @param storeFilePaths list of StoreFile paths representing files to be excluded from reads
   * @throws IOException if a remote or network exception occurs during Put
   */
  void writeExcludedStoreFilePaths(final String tableName, final String regionName,
      final String storeName, final List<Path> storeFilePaths) throws IOException;

  /**
   * Delete storefile paths for a tracking column family, normally used when a region-store is
   * completely removed due to region split or merge
   * @param tableName name of the current table in String
   * @param regionName name of the current region in String
   * @param storeName name of the column family in String, to be combined with regionName to make
   *                 the row key.
   * @throws IOException if a remote or network exception occurs during delete
   */
  void deleteStoreFilePaths(final String tableName, final String regionName, final String storeName)
      throws IOException;

}
