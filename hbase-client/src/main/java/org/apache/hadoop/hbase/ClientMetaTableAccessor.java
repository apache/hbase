/*
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

import static org.apache.hadoop.hbase.client.RegionLocator.LOCATOR_META_REPLICAS_MODE;
import static org.apache.hadoop.hbase.util.FutureUtils.addListener;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import org.apache.hadoop.hbase.client.AdvancedScanResultConsumer;
import org.apache.hadoop.hbase.client.AsyncTable;
import org.apache.hadoop.hbase.client.Consistency;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Scan.ReadType;
import org.apache.hadoop.hbase.client.TableState;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The (asynchronous) meta table accessor used at client side. Used to read/write region and
 * assignment information store in <code>hbase:meta</code>.
 * @since 2.0.0
 * @see CatalogFamilyFormat
 */
@InterfaceAudience.Private
public final class ClientMetaTableAccessor {

  private static final Logger LOG = LoggerFactory.getLogger(ClientMetaTableAccessor.class);

  private ClientMetaTableAccessor() {
  }

  @InterfaceAudience.Private
  @SuppressWarnings("ImmutableEnumChecker")
  public enum QueryType {
    ALL(HConstants.TABLE_FAMILY, HConstants.CATALOG_FAMILY),
    REGION(HConstants.CATALOG_FAMILY),
    TABLE(HConstants.TABLE_FAMILY),
    REPLICATION(HConstants.REPLICATION_BARRIER_FAMILY);

    private final byte[][] families;

    QueryType(byte[]... families) {
      this.families = families;
    }

    byte[][] getFamilies() {
      return this.families;
    }
  }

  public static CompletableFuture<Boolean> tableExists(AsyncTable<?> metaTable,
    TableName tableName) {
    return getTableState(metaTable, tableName).thenApply(Optional::isPresent);
  }

  public static CompletableFuture<Optional<TableState>> getTableState(AsyncTable<?> metaTable,
    TableName tableName) {
    CompletableFuture<Optional<TableState>> future = new CompletableFuture<>();
    Get get = new Get(tableName.getName()).addColumn(HConstants.TABLE_FAMILY,
      HConstants.TABLE_STATE_QUALIFIER);
    addListener(metaTable.get(get), (result, error) -> {
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
    return future;
  }

  /** Returns the HRegionLocation from meta for the given region */
  public static CompletableFuture<Optional<HRegionLocation>>
    getRegionLocation(AsyncTable<?> metaTable, byte[] regionName) {
    CompletableFuture<Optional<HRegionLocation>> future = new CompletableFuture<>();
    try {
      RegionInfo parsedRegionInfo = CatalogFamilyFormat.parseRegionInfoFromRegionName(regionName);
      addListener(metaTable.get(new Get(CatalogFamilyFormat.getMetaKeyForRegion(parsedRegionInfo))
        .addFamily(HConstants.CATALOG_FAMILY)), (r, err) -> {
          if (err != null) {
            future.completeExceptionally(err);
            return;
          }
          future.complete(getRegionLocations(r)
            .map(locations -> locations.getRegionLocation(parsedRegionInfo.getReplicaId())));
        });
    } catch (IOException parseEx) {
      LOG.warn("Failed to parse the passed region name: " + Bytes.toStringBinary(regionName));
      future.completeExceptionally(parseEx);
    }
    return future;
  }

  /** Returns the HRegionLocation from meta for the given encoded region name */
  public static CompletableFuture<Optional<HRegionLocation>>
    getRegionLocationWithEncodedName(AsyncTable<?> metaTable, byte[] encodedRegionName) {
    CompletableFuture<Optional<HRegionLocation>> future = new CompletableFuture<>();
    addListener(
      metaTable
        .scanAll(new Scan().setReadType(ReadType.PREAD).addFamily(HConstants.CATALOG_FAMILY)),
      (results, err) -> {
        if (err != null) {
          future.completeExceptionally(err);
          return;
        }
        String encodedRegionNameStr = Bytes.toString(encodedRegionName);
        results.stream().filter(result -> !result.isEmpty())
          .filter(result -> CatalogFamilyFormat.getRegionInfo(result) != null).forEach(result -> {
            getRegionLocations(result).ifPresent(locations -> {
              for (HRegionLocation location : locations.getRegionLocations()) {
                if (
                  location != null
                    && encodedRegionNameStr.equals(location.getRegion().getEncodedName())
                ) {
                  future.complete(Optional.of(location));
                  return;
                }
              }
            });
          });
        future.complete(Optional.empty());
      });
    return future;
  }

  private static Optional<TableState> getTableState(Result r) throws IOException {
    return Optional.ofNullable(CatalogFamilyFormat.getTableState(r));
  }

  /**
   * Used to get all region locations for the specific table
   * @param metaTable scanner over meta table
   * @param tableName table we're looking for, can be null for getting all regions
   * @return the list of region locations. The return value will be wrapped by a
   *         {@link CompletableFuture}.
   */
  public static CompletableFuture<List<HRegionLocation>> getTableHRegionLocations(
    AsyncTable<AdvancedScanResultConsumer> metaTable, TableName tableName) {
    CompletableFuture<List<HRegionLocation>> future = new CompletableFuture<>();
    addListener(getTableRegionsAndLocations(metaTable, tableName, true), (locations, err) -> {
      if (err != null) {
        future.completeExceptionally(err);
      } else if (locations == null || locations.isEmpty()) {
        future.complete(Collections.emptyList());
      } else {
        List<HRegionLocation> regionLocations =
          locations.stream().map(loc -> new HRegionLocation(loc.getFirst(), loc.getSecond()))
            .collect(Collectors.toList());
        future.complete(regionLocations);
      }
    });
    return future;
  }

  /**
   * Used to get table regions' info and server.
   * @param metaTable                   scanner over meta table
   * @param tableName                   table we're looking for, can be null for getting all regions
   * @param excludeOfflinedSplitParents don't return split parents
   * @return the list of regioninfos and server. The return value will be wrapped by a
   *         {@link CompletableFuture}.
   */
  private static CompletableFuture<List<Pair<RegionInfo, ServerName>>> getTableRegionsAndLocations(
    final AsyncTable<AdvancedScanResultConsumer> metaTable, final TableName tableName,
    final boolean excludeOfflinedSplitParents) {
    CompletableFuture<List<Pair<RegionInfo, ServerName>>> future = new CompletableFuture<>();
    if (MetaTableName.getInstance().equals(tableName)) {
      future.completeExceptionally(new IOException(
        "This method can't be used to locate meta regions;" + " use MetaTableLocator instead"));
    }

    // Make a version of CollectingVisitor that collects RegionInfo and ServerAddress
    CollectRegionLocationsVisitor visitor =
      new CollectRegionLocationsVisitor(excludeOfflinedSplitParents);

    addListener(scanMeta(metaTable, tableName, QueryType.REGION, visitor), (v, error) -> {
      if (error != null) {
        future.completeExceptionally(error);
        return;
      }
      future.complete(visitor.getResults());
    });
    return future;
  }

  /**
   * Performs a scan of META table for given table.
   * @param metaTable scanner over meta table
   * @param tableName table within we scan
   * @param type      scanned part of meta
   * @param visitor   Visitor invoked against each row
   */
  private static CompletableFuture<Void> scanMeta(AsyncTable<AdvancedScanResultConsumer> metaTable,
    TableName tableName, QueryType type, final Visitor visitor) {
    return scanMeta(metaTable, getTableStartRowForMeta(tableName, type),
      getTableStopRowForMeta(tableName, type), type, Integer.MAX_VALUE, visitor);
  }

  /**
   * Performs a scan of META table for given table.
   * @param metaTable scanner over meta table
   * @param startRow  Where to start the scan
   * @param stopRow   Where to stop the scan
   * @param type      scanned part of meta
   * @param maxRows   maximum rows to return
   * @param visitor   Visitor invoked against each row
   */
  private static CompletableFuture<Void> scanMeta(AsyncTable<AdvancedScanResultConsumer> metaTable,
    byte[] startRow, byte[] stopRow, QueryType type, int maxRows, final Visitor visitor) {
    int rowUpperLimit = maxRows > 0 ? maxRows : Integer.MAX_VALUE;
    Scan scan = getMetaScan(metaTable, rowUpperLimit);
    for (byte[] family : type.getFamilies()) {
      scan.addFamily(family);
    }
    if (startRow != null) {
      scan.withStartRow(startRow);
    }
    if (stopRow != null) {
      scan.withStopRow(stopRow);
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Scanning META" + " starting at row=" + Bytes.toStringBinary(scan.getStartRow())
        + " stopping at row=" + Bytes.toStringBinary(scan.getStopRow()) + " for max="
        + rowUpperLimit + " with caching=" + scan.getCaching());
    }

    CompletableFuture<Void> future = new CompletableFuture<Void>();
    // Get the region locator's meta replica mode.
    CatalogReplicaMode metaReplicaMode = CatalogReplicaMode.fromString(metaTable.getConfiguration()
      .get(LOCATOR_META_REPLICAS_MODE, CatalogReplicaMode.NONE.toString()));

    if (metaReplicaMode == CatalogReplicaMode.LOAD_BALANCE) {
      addListener(metaTable.getDescriptor(), (desc, error) -> {
        if (error != null) {
          LOG.error("Failed to get meta table descriptor, error: ", error);
          future.completeExceptionally(error);
          return;
        }

        int numOfReplicas = desc.getRegionReplication();
        if (numOfReplicas > 1) {
          int replicaId = ThreadLocalRandom.current().nextInt(numOfReplicas);

          // When the replicaId is 0, do not set to Consistency.TIMELINE
          if (replicaId > 0) {
            scan.setReplicaId(replicaId);
            scan.setConsistency(Consistency.TIMELINE);
          }
        }
        metaTable.scan(scan, new MetaTableScanResultConsumer(rowUpperLimit, visitor, future));
      });
    } else {
      if (metaReplicaMode == CatalogReplicaMode.HEDGED_READ) {
        scan.setConsistency(Consistency.TIMELINE);
      }
      metaTable.scan(scan, new MetaTableScanResultConsumer(rowUpperLimit, visitor, future));
    }

    return future;
  }

  private static final class MetaTableScanResultConsumer implements AdvancedScanResultConsumer {

    private int currentRowCount;

    private final int rowUpperLimit;

    private final Visitor visitor;

    private final CompletableFuture<Void> future;

    MetaTableScanResultConsumer(int rowUpperLimit, Visitor visitor,
      CompletableFuture<Void> future) {
      this.rowUpperLimit = rowUpperLimit;
      this.visitor = visitor;
      this.future = future;
      this.currentRowCount = 0;
    }

    @Override
    public void onError(Throwable error) {
      future.completeExceptionally(error);
    }

    @Override
    @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "NP_NONNULL_PARAM_VIOLATION",
        justification = "https://github.com/findbugsproject/findbugs/issues/79")
    public void onComplete() {
      future.complete(null);
    }

    @Override
    public void onNext(Result[] results, ScanController controller) {
      boolean terminateScan = false;
      for (Result result : results) {
        try {
          if (!visitor.visit(result)) {
            terminateScan = true;
            break;
          }
        } catch (Exception e) {
          future.completeExceptionally(e);
          terminateScan = true;
          break;
        }
        if (++currentRowCount >= rowUpperLimit) {
          terminateScan = true;
          break;
        }
      }
      if (terminateScan) {
        controller.terminate();
      }
    }
  }

  /**
   * Implementations 'visit' a catalog table row.
   */
  public interface Visitor {
    /**
     * Visit the catalog table row.
     * @param r A row from catalog table
     * @return True if we are to proceed scanning the table, else false if we are to stop now.
     */
    boolean visit(final Result r) throws IOException;
  }

  /**
   * Implementations 'visit' a catalog table row but with close() at the end.
   */
  public interface CloseableVisitor extends Visitor, Closeable {
  }

  /**
   * A {@link Visitor} that collects content out of passed {@link Result}.
   */
  private static abstract class CollectingVisitor<T> implements Visitor {
    final List<T> results = new ArrayList<>();

    @Override
    public boolean visit(Result r) throws IOException {
      if (r != null && !r.isEmpty()) {
        add(r);
      }
      return true;
    }

    abstract void add(Result r);

    /** Returns Collected results; wait till visits complete to collect all possible results */
    List<T> getResults() {
      return this.results;
    }
  }

  static class CollectRegionLocationsVisitor
    extends CollectingVisitor<Pair<RegionInfo, ServerName>> {

    private final boolean excludeOfflinedSplitParents;

    private RegionLocations current = null;

    CollectRegionLocationsVisitor(boolean excludeOfflinedSplitParents) {
      this.excludeOfflinedSplitParents = excludeOfflinedSplitParents;
    }

    @Override
    public boolean visit(Result r) throws IOException {
      Optional<RegionLocations> currentRegionLocations = getRegionLocations(r);
      current = currentRegionLocations.orElse(null);
      if (current == null || current.getRegionLocation().getRegion() == null) {
        LOG.warn("No serialized RegionInfo in " + r);
        return true;
      }
      RegionInfo hri = current.getRegionLocation().getRegion();
      if (excludeOfflinedSplitParents && hri.isSplitParent()) {
        return true;
      }
      // Else call super and add this Result to the collection.
      return super.visit(r);
    }

    @Override
    void add(Result r) {
      if (current == null) {
        return;
      }
      for (HRegionLocation loc : current.getRegionLocations()) {
        if (loc != null) {
          this.results.add(new Pair<RegionInfo, ServerName>(loc.getRegion(), loc.getServerName()));
        }
      }
    }
  }

  /**
   * Collects all returned.
   */
  static class CollectAllVisitor extends CollectingVisitor<Result> {
    @Override
    void add(Result r) {
      this.results.add(r);
    }
  }

  private static Scan getMetaScan(AsyncTable<?> metaTable, int rowUpperLimit) {
    Scan scan = new Scan();
    int scannerCaching = metaTable.getConfiguration().getInt(HConstants.HBASE_META_SCANNER_CACHING,
      HConstants.DEFAULT_HBASE_META_SCANNER_CACHING);
    if (
      metaTable.getConfiguration().getBoolean(HConstants.USE_META_REPLICAS,
        HConstants.DEFAULT_USE_META_REPLICAS)
    ) {
      scan.setConsistency(Consistency.TIMELINE);
    }
    if (rowUpperLimit <= scannerCaching) {
      scan.setLimit(rowUpperLimit);
    }
    int rows = Math.min(rowUpperLimit, scannerCaching);
    scan.setCaching(rows);
    return scan;
  }

  /** Returns an HRegionLocationList extracted from the result. */
  private static Optional<RegionLocations> getRegionLocations(Result r) {
    return Optional.ofNullable(CatalogFamilyFormat.getRegionLocations(r));
  }

  /** Returns start row for scanning META according to query type */
  public static byte[] getTableStartRowForMeta(TableName tableName, QueryType type) {
    if (tableName == null) {
      return null;
    }
    switch (type) {
      case REGION:
      case REPLICATION: {
        byte[] startRow = new byte[tableName.getName().length + 2];
        System.arraycopy(tableName.getName(), 0, startRow, 0, tableName.getName().length);
        startRow[startRow.length - 2] = HConstants.DELIMITER;
        startRow[startRow.length - 1] = HConstants.DELIMITER;
        return startRow;
      }
      case ALL:
      case TABLE:
      default: {
        return tableName.getName();
      }
    }
  }

  /** Returns stop row for scanning META according to query type */
  public static byte[] getTableStopRowForMeta(TableName tableName, QueryType type) {
    if (tableName == null) {
      return null;
    }
    final byte[] stopRow;
    switch (type) {
      case REGION:
      case REPLICATION: {
        stopRow = new byte[tableName.getName().length + 3];
        System.arraycopy(tableName.getName(), 0, stopRow, 0, tableName.getName().length);
        stopRow[stopRow.length - 3] = ' ';
        stopRow[stopRow.length - 2] = HConstants.DELIMITER;
        stopRow[stopRow.length - 1] = HConstants.DELIMITER;
        break;
      }
      case ALL:
      case TABLE:
      default: {
        stopRow = new byte[tableName.getName().length + 1];
        System.arraycopy(tableName.getName(), 0, stopRow, 0, tableName.getName().length);
        stopRow[stopRow.length - 1] = ' ';
        break;
      }
    }
    return stopRow;
  }
}
