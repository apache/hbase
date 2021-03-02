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
import java.util.Set;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Helper class to interact with the hbase storefile tracking data persisted as off-memory data
 * from the {@link StoreFileManager}
 *
 * There is only a set of tracking storefiles, 'included'.
 *
 * e.g. list of storefile paths in 'included' should be the identical copy of the in-memory
 * {@link HStoreFile}'s Path(s) and can be reused during region opens and region reassignment.
 */
@InterfaceAudience.Private
public interface StoreFilePathAccessor {

  /**
   * Get storefile paths from the 'included' data set
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
   * Write an entity that should be persisted into the tracking data for the
   * specific column family of a given region
   *
   * it would be happened during storefile operation e.g. flush and compaction.
   *
   * @param tableName name of the current table in String
   * @param regionName name of the current region in String
   * @param storeName name of the column family in String, to be combined with regionName to make
   *                 the row key.
   * @param storeFilePathUpdate Updates to be persisted
   * @throws IOException if a remote or network exception occurs during write
   */
  void writeStoreFilePaths(final String tableName, final String regionName,
    final String storeName, final StoreFilePathUpdate storeFilePathUpdate) throws IOException;

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
