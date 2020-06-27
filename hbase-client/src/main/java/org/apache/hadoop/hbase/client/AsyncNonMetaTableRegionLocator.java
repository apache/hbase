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

import static org.apache.hadoop.hbase.HConstants.DEFAULT_USE_META_REPLICAS;
import static org.apache.hadoop.hbase.HConstants.USE_META_REPLICAS;
import static org.apache.hadoop.hbase.TableName.META_TABLE_NAME;
import static org.apache.hadoop.hbase.client.RegionLocator.LOCATOR_META_REPLICAS_MODE;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.hadoop.hbase.CatalogFamilyFormat;
import org.apache.hadoop.hbase.ClientMetaTableAccessor;
import org.apache.hadoop.hbase.HConstants;
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

  // The mode tells if HedgedRead, LoadBalance mode is supported.
  // The default mode is CatalogReplicaMode.None.
  private CatalogReplicaMode metaReplicaMode;

  private CatalogReplicaLoadBalanceSelector metaReplicaSelector;

  AsyncNonMetaTableRegionLocator(AsyncConnectionImpl conn, TableName tableName, int maxConcurrent,
    int prefetchLimit) {
    super(conn, tableName, maxConcurrent, Bytes.BYTES_COMPARATOR);
    this.prefetchLimit = prefetchLimit;
    // Get the region locator's meta replica mode.
    this.metaReplicaMode = CatalogReplicaMode.fromString(conn.getConfiguration()
      .get(LOCATOR_META_REPLICAS_MODE, CatalogReplicaMode.NONE.toString()));

    switch (this.metaReplicaMode) {
      case LOAD_BALANCE:
        String replicaSelectorClass = conn.getConfiguration().
          get(RegionLocator.LOCATOR_META_REPLICAS_MODE_LOADBALANCE_SELECTOR,
          CatalogReplicaLoadBalanceSimpleSelector.class.getName());

        this.metaReplicaSelector = CatalogReplicaLoadBalanceSelectorFactory.createSelector(
          replicaSelectorClass, META_TABLE_NAME, conn, () -> {
            int numOfReplicas = CatalogReplicaLoadBalanceSelector.UNINITIALIZED_NUM_OF_REPLICAS;
            try {
              RegionLocations metaLocations = conn.getLocator()
                .getRegionLocations(TableName.META_TABLE_NAME, HConstants.EMPTY_START_ROW,
                  RegionLocateType.CURRENT, true, conn.connConf.getReadRpcTimeoutNs())
                .get();
              numOfReplicas = metaLocations.size();
            } catch (Exception e) {
              LOG.error("Failed to get table {}'s region replication, ", META_TABLE_NAME, e);
            }
            return numOfReplicas;
          });
        break;
      case NONE:
        // If user does not configure LOCATOR_META_REPLICAS_MODE, let's check the legacy config.
        boolean useMetaReplicas = conn.getConfiguration().getBoolean(USE_META_REPLICAS,
          DEFAULT_USE_META_REPLICAS);
        if (useMetaReplicas) {
          this.metaReplicaMode = CatalogReplicaMode.HEDGED_READ;
        }
        break;
      default:
        // Doing nothing
    }
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
    switch (this.metaReplicaMode) {
      case LOAD_BALANCE:
        int metaReplicaId = this.metaReplicaSelector.select(tableName, req.row, req.locateType);
        if (metaReplicaId != RegionInfo.DEFAULT_REPLICA_ID) {
          // If the selector gives a non-primary meta replica region, then go with it.
          // Otherwise, just go to primary in non-hedgedRead mode.
          scan.setConsistency(Consistency.TIMELINE);
          scan.setReplicaId(metaReplicaId);
        }
        break;
      case HEDGED_READ:
        scan.setConsistency(Consistency.TIMELINE);
        break;
      default:
        // do nothing
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
