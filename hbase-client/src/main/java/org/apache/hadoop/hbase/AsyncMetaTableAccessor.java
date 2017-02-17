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
package org.apache.hadoop.hbase;

import static org.apache.hadoop.hbase.TableName.META_TABLE_NAME;

import java.io.IOException;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.RawAsyncTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.TableState;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Pair;

/**
 * The asynchronous meta table accessor. Used to read/write region and assignment information store
 * in <code>hbase:meta</code>.
 */
@InterfaceAudience.Private
public class AsyncMetaTableAccessor {

  private static final Log LOG = LogFactory.getLog(AsyncMetaTableAccessor.class);

  public static CompletableFuture<Boolean> tableExists(RawAsyncTable metaTable, TableName tableName) {
    if (tableName.equals(META_TABLE_NAME)) {
      return CompletableFuture.completedFuture(true);
    }
    return getTableState(metaTable, tableName).thenApply(Optional::isPresent);
  }

  public static CompletableFuture<Optional<TableState>> getTableState(RawAsyncTable metaTable,
      TableName tableName) {
    CompletableFuture<Optional<TableState>> future = new CompletableFuture<>();
    Get get = new Get(tableName.getName()).addColumn(getTableFamily(), getStateColumn());
    long time = EnvironmentEdgeManager.currentTime();
    try {
      get.setTimeRange(0, time);
      metaTable.get(get).whenComplete((result, error) -> {
        if (error != null) {
          future.completeExceptionally(error);
          return;
        }
        try {
          future.complete(getTableState(result));
        } catch (IOException e) {
          future.completeExceptionally(e);
        }
      });
    } catch (IOException ioe) {
      future.completeExceptionally(ioe);
    }
    return future;
  }

  public static CompletableFuture<Pair<HRegionInfo, ServerName>> getRegion(RawAsyncTable metaTable,
      byte[] regionName) {
    CompletableFuture<Pair<HRegionInfo, ServerName>> future = new CompletableFuture<>();
    byte[] row = regionName;
    HRegionInfo parsedInfo = null;
    try {
      parsedInfo = MetaTableAccessor.parseRegionInfoFromRegionName(regionName);
      row = MetaTableAccessor.getMetaKeyForRegion(parsedInfo);
    } catch (Exception parseEx) {
      // Ignore if regionName is a encoded region name.
    }

    final HRegionInfo finalHRI = parsedInfo;
    metaTable.get(new Get(row).addFamily(HConstants.CATALOG_FAMILY)).whenComplete((r, err) -> {
      if (err != null) {
        future.completeExceptionally(err);
        return;
      }
      RegionLocations locations = MetaTableAccessor.getRegionLocations(r);
      HRegionLocation hrl = locations == null ? null
          : locations.getRegionLocation(finalHRI == null ? 0 : finalHRI.getReplicaId());
      if (hrl == null) {
        future.complete(null);
      } else {
        future.complete(new Pair<>(hrl.getRegionInfo(), hrl.getServerName()));
      }
    });

    return future;
  }

  private static Optional<TableState> getTableState(Result r) throws IOException {
    Cell cell = r.getColumnLatestCell(getTableFamily(), getStateColumn());
    if (cell == null) return Optional.empty();
    try {
      return Optional.of(TableState.parseFrom(
        TableName.valueOf(r.getRow()),
        Arrays.copyOfRange(cell.getValueArray(), cell.getValueOffset(), cell.getValueOffset()
            + cell.getValueLength())));
    } catch (DeserializationException e) {
      throw new IOException("Failed to parse table state from result: " + r, e);
    }
  }

  /**
   * Returns the column family used for table columns.
   * @return HConstants.TABLE_FAMILY.
   */
  private static byte[] getTableFamily() {
    return HConstants.TABLE_FAMILY;
  }

  /**
   * Returns the column qualifier for serialized table state
   * @return HConstants.TABLE_STATE_QUALIFIER
   */
  private static byte[] getStateColumn() {
    return HConstants.TABLE_STATE_QUALIFIER;
  }
}
