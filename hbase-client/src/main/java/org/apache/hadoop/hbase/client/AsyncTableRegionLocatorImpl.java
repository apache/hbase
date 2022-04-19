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

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.hadoop.hbase.AsyncMetaTableAccessor;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.TableName;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * The implementation of AsyncRegionLocator.
 */
@InterfaceAudience.Private
class AsyncTableRegionLocatorImpl implements AsyncTableRegionLocator {

  private final TableName tableName;

  private final AsyncConnectionImpl conn;

  public AsyncTableRegionLocatorImpl(TableName tableName, AsyncConnectionImpl conn) {
    this.tableName = tableName;
    this.conn = conn;
  }

  @Override
  public TableName getName() {
    return tableName;
  }

  @Override
  public CompletableFuture<HRegionLocation> getRegionLocation(byte[] row, int replicaId,
      boolean reload) {
    return conn.getLocator().getRegionLocation(tableName, row, replicaId, RegionLocateType.CURRENT,
      reload, -1L);
  }

  @Override
  public CompletableFuture<List<HRegionLocation>> getAllRegionLocations() {
    if (TableName.isMetaTableName(tableName)) {
      return conn.registry.getMetaRegionLocations()
        .thenApply(locs -> Arrays.asList(locs.getRegionLocations()));
    }
    CompletableFuture<List<HRegionLocation>> future = AsyncMetaTableAccessor
            .getTableHRegionLocations(conn.getTable(TableName.META_TABLE_NAME), tableName);
    addListener(future, (locs, error) -> locs.forEach(loc -> conn.getLocator()
      .getNonMetaRegionLocator().addLocationToCache(loc)));
    return future;
  }

  @Override
  public CompletableFuture<List<HRegionLocation>> getRegionLocations(byte[] row, boolean reload) {
    return conn.getLocator()
      .getRegionLocations(tableName, row, RegionLocateType.CURRENT, reload, -1L)
      .thenApply(locs -> Arrays.asList(locs.getRegionLocations()));
  }

  @Override
  public void clearRegionLocationCache() {
    conn.getLocator().clearCache(tableName);
  }
}
