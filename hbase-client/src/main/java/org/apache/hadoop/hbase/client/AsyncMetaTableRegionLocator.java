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

import static org.apache.hadoop.hbase.util.FutureUtils.addListener;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.MetaCellComparator;
import org.apache.hadoop.hbase.TableName;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * The class for locating region for meta table.
 */
@InterfaceAudience.Private
class AsyncMetaTableRegionLocator extends AbstractAsyncTableRegionLocator {

  AsyncMetaTableRegionLocator(AsyncConnectionImpl conn, TableName tableName, int maxConcurrent) {
    // for meta region we should use MetaCellComparator to compare the row keys
    super(conn, tableName, maxConcurrent, MetaCellComparator.ROW_COMPARATOR);
  }

  @Override
  protected void locate(LocateRequest req) {
    addListener(conn.registry.locateMeta(req.row, req.locateType), (locs, error) -> {
      if (error != null) {
        onLocateComplete(req, null, error);
        return;
      }
      if (validateRegionLocations(locs, req)) {
        onLocateComplete(req, locs, null);
      }
    });
  }

  @Override
  CompletableFuture<List<HRegionLocation>>
    getAllRegionLocations(boolean excludeOfflinedSplitParents) {
    return conn.registry.getAllMetaRegionLocations(excludeOfflinedSplitParents);
  }
}
