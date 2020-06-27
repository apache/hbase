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

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.hadoop.hbase.CatalogFamilyFormat;
import org.apache.hadoop.hbase.ClientMetaTableAccessor;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The class for locating region for table other than meta.
 */
@InterfaceAudience.Private
class AsyncNonMetaTableRegionLocator extends AbstractAsyncTableRegionLocator {

  private static final Logger LOG = LoggerFactory.getLogger(AsyncNonMetaTableRegionLocator.class);

  private final int prefetchLimit;

  private final boolean useMetaReplicas;

  AsyncNonMetaTableRegionLocator(AsyncConnectionImpl conn, TableName tableName, int maxConcurrent,
    int prefetchLimit, boolean useMetaReplicas) {
    super(conn, tableName, maxConcurrent, Bytes.BYTES_COMPARATOR);
    this.prefetchLimit = prefetchLimit;
    this.useMetaReplicas = useMetaReplicas;
  }

  // return whether we should stop the scan
  private boolean onScanNext(TableName tableName, LocateRequest req, Result result) {
    RegionLocations locs = CatalogFamilyFormat.getRegionLocations(result);
    if (LOG.isDebugEnabled()) {
      LOG.debug("The fetched location of '{}', row='{}', locateType={} is {}", tableName,
        Bytes.toStringBinary(req.row), req.locateType, locs);
    }
    if (!validateRegionLocations(locs, req)) {
      return true;
    }
    if (locs.getDefaultRegionLocation().getRegion().isSplitParent()) {
      return false;
    }
    onLocateComplete(req, locs, null);
    return true;
  }

  @Override
  protected void locate(LocateRequest req) {
    if (LOG.isTraceEnabled()) {
      LOG.trace("Try locate '{}', row='{}', locateType={} in meta", tableName,
        Bytes.toStringBinary(req.row), req.locateType);
    }
    Scan scan =
      CatalogFamilyFormat.createRegionLocateScan(tableName, req.row, req.locateType, prefetchLimit);
    if (useMetaReplicas) {
      scan.setConsistency(Consistency.TIMELINE);
    }
    conn.getTable(TableName.META_TABLE_NAME).scan(scan, new AdvancedScanResultConsumer() {

      private boolean completeNormally = false;

      private boolean tableNotFound = true;

      @Override
      public void onError(Throwable error) {
        onLocateComplete(req, null, error);
      }

      @Override
      public void onComplete() {
        if (tableNotFound) {
          onLocateComplete(req, null, new TableNotFoundException(tableName));
        } else if (!completeNormally) {
          onLocateComplete(req, null, new IOException(
            "Unable to find region for '" + Bytes.toStringBinary(req.row) + "' in " + tableName));
        }
      }

      @Override
      public void onNext(Result[] results, ScanController controller) {
        if (results.length == 0) {
          return;
        }
        tableNotFound = false;
        int i = 0;
        for (; i < results.length; i++) {
          if (onScanNext(tableName, req, results[i])) {
            completeNormally = true;
            controller.terminate();
            i++;
            break;
          }
        }
        // Add the remaining results into cache
        if (i < results.length) {
          for (; i < results.length; i++) {
            RegionLocations locs = CatalogFamilyFormat.getRegionLocations(results[i]);
            if (locs == null) {
              continue;
            }
            HRegionLocation loc = locs.getDefaultRegionLocation();
            if (loc == null) {
              continue;
            }
            RegionInfo info = loc.getRegion();
            if (info == null || info.isOffline() || info.isSplitParent()) {
              continue;
            }
            RegionLocations addedLocs = cache.add(locs);
            synchronized (this) {
              clearCompletedRequests(addedLocs);
            }
          }
        }
      }
    });
  }

  @Override
  CompletableFuture<List<HRegionLocation>>
    getAllRegionLocations(boolean excludeOfflinedSplitParents) {
    return ClientMetaTableAccessor.getTableHRegionLocations(
      conn.getTable(TableName.META_TABLE_NAME), tableName, excludeOfflinedSplitParents);
  }
}
