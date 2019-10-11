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

import static org.apache.hadoop.hbase.util.FutureUtils.addListener;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.SortedMap;
import java.util.concurrent.CompletableFuture;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.hadoop.hbase.MetaTableAccessor.CollectingVisitor;
import org.apache.hadoop.hbase.MetaTableAccessor.QueryType;
import org.apache.hadoop.hbase.MetaTableAccessor.Visitor;
import org.apache.hadoop.hbase.client.AdvancedScanResultConsumer;
import org.apache.hadoop.hbase.client.AsyncTable;
import org.apache.hadoop.hbase.client.Consistency;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Scan.ReadType;
import org.apache.hadoop.hbase.client.TableState;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The asynchronous meta table accessor. Used to read/write region and assignment information store
 * in <code>hbase:meta</code>.
 * @since 2.0.0
 */
@InterfaceAudience.Private
public class AsyncMetaTableAccessor {

  private static final Logger LOG = LoggerFactory.getLogger(AsyncMetaTableAccessor.class);


  /** The delimiter for meta columns for replicaIds &gt; 0 */
  private static final char META_REPLICA_ID_DELIMITER = '_';

  /** A regex for parsing server columns from meta. See above javadoc for meta layout */
  private static final Pattern SERVER_COLUMN_PATTERN = Pattern
      .compile("^server(_[0-9a-fA-F]{4})?$");

  public static CompletableFuture<Boolean> tableExists(AsyncTable<?> metaTable,
      TableName tableName) {
    return getTableState(metaTable, tableName).thenApply(Optional::isPresent);
  }

  public static CompletableFuture<Optional<TableState>> getTableState(AsyncTable<?> metaTable,
      TableName tableName) {
    CompletableFuture<Optional<TableState>> future = new CompletableFuture<>();
    Get get = new Get(tableName.getName()).addColumn(getTableFamily(), getStateColumn());
    long time = EnvironmentEdgeManager.currentTime();
    try {
      get.setTimeRange(0, time);
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
    } catch (IOException ioe) {
      future.completeExceptionally(ioe);
    }
    return future;
  }

  /**
   * Returns the HRegionLocation from meta for the given region
   * @param metaTable
   * @param regionName region we're looking for
   * @return HRegionLocation for the given region
   */
  public static CompletableFuture<Optional<HRegionLocation>> getRegionLocation(
      AsyncTable<?> metaTable, byte[] regionName) {
    CompletableFuture<Optional<HRegionLocation>> future = new CompletableFuture<>();
    try {
      RegionInfo parsedRegionInfo = MetaTableAccessor.parseRegionInfoFromRegionName(regionName);
      addListener(metaTable.get(new Get(MetaTableAccessor.getMetaKeyForRegion(parsedRegionInfo))
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

  /**
   * Returns the HRegionLocation from meta for the given encoded region name
   * @param metaTable
   * @param encodedRegionName region we're looking for
   * @return HRegionLocation for the given region
   */
  public static CompletableFuture<Optional<HRegionLocation>> getRegionLocationWithEncodedName(
      AsyncTable<?> metaTable, byte[] encodedRegionName) {
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
          .filter(result -> MetaTableAccessor.getRegionInfo(result) != null).forEach(result -> {
            getRegionLocations(result).ifPresent(locations -> {
              for (HRegionLocation location : locations.getRegionLocations()) {
                if (location != null &&
                  encodedRegionNameStr.equals(location.getRegion().getEncodedName())) {
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
   * Used to get all region locations for the specific table.
   * @param metaTable
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
   * @param metaTable
   * @param tableName table we're looking for, can be null for getting all regions
   * @param excludeOfflinedSplitParents don't return split parents
   * @return the list of regioninfos and server. The return value will be wrapped by a
   *         {@link CompletableFuture}.
   */
  private static CompletableFuture<List<Pair<RegionInfo, ServerName>>> getTableRegionsAndLocations(
      final AsyncTable<AdvancedScanResultConsumer> metaTable,
      final TableName tableName, final boolean excludeOfflinedSplitParents) {
    CompletableFuture<List<Pair<RegionInfo, ServerName>>> future = new CompletableFuture<>();
    if (TableName.META_TABLE_NAME.equals(tableName)) {
      future.completeExceptionally(new IOException(
          "This method can't be used to locate meta regions;" + " use MetaTableLocator instead"));
    }

    // Make a version of CollectingVisitor that collects RegionInfo and ServerAddress
    CollectingVisitor<Pair<RegionInfo, ServerName>> visitor =
      new CollectingVisitor<Pair<RegionInfo, ServerName>>() {
      private RegionLocations current = null;

      @Override
      public boolean visit(Result r) throws IOException {
        Optional<RegionLocations> currentRegionLocations = getRegionLocations(r);
        current = currentRegionLocations.orElse(null);
        if (current == null || current.getRegionLocation().getRegion() == null) {
          LOG.warn("No serialized RegionInfo in " + r);
          return true;
        }
        RegionInfo hri = current.getRegionLocation().getRegion();
        if (excludeOfflinedSplitParents && hri.isSplitParent()) return true;
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
            this.results.add(new Pair<RegionInfo, ServerName>(loc.getRegion(), loc
                .getServerName()));
          }
        }
      }
    };

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
   * @param metaTable
   * @param tableName table withing we scan
   * @param type scanned part of meta
   * @param visitor Visitor invoked against each row
   */
  private static CompletableFuture<Void> scanMeta(AsyncTable<AdvancedScanResultConsumer> metaTable,
      TableName tableName, QueryType type, final Visitor visitor) {
    return scanMeta(metaTable, getTableStartRowForMeta(tableName, type),
      getTableStopRowForMeta(tableName, type), type, Integer.MAX_VALUE, visitor);
  }

  /**
   * Performs a scan of META table for given table.
   * @param metaTable
   * @param startRow Where to start the scan
   * @param stopRow Where to stop the scan
   * @param type scanned part of meta
   * @param maxRows maximum rows to return
   * @param visitor Visitor invoked against each row
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
    metaTable.scan(scan, new MetaTableScanResultConsumer(rowUpperLimit, visitor, future));
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

  private static Scan getMetaScan(AsyncTable<?> metaTable, int rowUpperLimit) {
    Scan scan = new Scan();
    int scannerCaching = metaTable.getConfiguration().getInt(HConstants.HBASE_META_SCANNER_CACHING,
      HConstants.DEFAULT_HBASE_META_SCANNER_CACHING);
    if (metaTable.getConfiguration().getBoolean(HConstants.USE_META_REPLICAS,
      HConstants.DEFAULT_USE_META_REPLICAS)) {
      scan.setConsistency(Consistency.TIMELINE);
    }
    if (rowUpperLimit <= scannerCaching) {
      scan.setLimit(rowUpperLimit);
    }
    int rows = Math.min(rowUpperLimit, scannerCaching);
    scan.setCaching(rows);
    return scan;
  }

  /**
   * Returns an HRegionLocationList extracted from the result.
   * @return an HRegionLocationList containing all locations for the region range or null if we
   *         can't deserialize the result.
   */
  private static Optional<RegionLocations> getRegionLocations(final Result r) {
    if (r == null) return Optional.empty();
    Optional<RegionInfo> regionInfo = getHRegionInfo(r, getRegionInfoColumn());
    if (!regionInfo.isPresent()) return Optional.empty();

    List<HRegionLocation> locations = new ArrayList<HRegionLocation>(1);
    NavigableMap<byte[], NavigableMap<byte[], byte[]>> familyMap = r.getNoVersionMap();

    locations.add(getRegionLocation(r, regionInfo.get(), 0));

    NavigableMap<byte[], byte[]> infoMap = familyMap.get(getCatalogFamily());
    if (infoMap == null) return Optional.of(new RegionLocations(locations));

    // iterate until all serverName columns are seen
    int replicaId = 0;
    byte[] serverColumn = getServerColumn(replicaId);
    SortedMap<byte[], byte[]> serverMap = null;
    serverMap = infoMap.tailMap(serverColumn, false);

    if (serverMap.isEmpty()) return Optional.of(new RegionLocations(locations));

    for (Map.Entry<byte[], byte[]> entry : serverMap.entrySet()) {
      replicaId = parseReplicaIdFromServerColumn(entry.getKey());
      if (replicaId < 0) {
        break;
      }
      HRegionLocation location = getRegionLocation(r, regionInfo.get(), replicaId);
      // In case the region replica is newly created, it's location might be null. We usually do not
      // have HRL's in RegionLocations object with null ServerName. They are handled as null HRLs.
      if (location == null || location.getServerName() == null) {
        locations.add(null);
      } else {
        locations.add(location);
      }
    }

    return Optional.of(new RegionLocations(locations));
  }

  /**
   * Returns the HRegionLocation parsed from the given meta row Result
   * for the given regionInfo and replicaId. The regionInfo can be the default region info
   * for the replica.
   * @param r the meta row result
   * @param regionInfo RegionInfo for default replica
   * @param replicaId the replicaId for the HRegionLocation
   * @return HRegionLocation parsed from the given meta row Result for the given replicaId
   */
  private static HRegionLocation getRegionLocation(final Result r, final RegionInfo regionInfo,
      final int replicaId) {
    Optional<ServerName> serverName = getServerName(r, replicaId);
    long seqNum = getSeqNumDuringOpen(r, replicaId);
    RegionInfo replicaInfo = RegionReplicaUtil.getRegionInfoForReplica(regionInfo, replicaId);
    return new HRegionLocation(replicaInfo, serverName.orElse(null), seqNum);
  }

  /**
   * Returns a {@link ServerName} from catalog table {@link Result}.
   * @param r Result to pull from
   * @return A ServerName instance.
   */
  private static Optional<ServerName> getServerName(final Result r, final int replicaId) {
    byte[] serverColumn = getServerColumn(replicaId);
    Cell cell = r.getColumnLatestCell(getCatalogFamily(), serverColumn);
    if (cell == null || cell.getValueLength() == 0) return Optional.empty();
    String hostAndPort = Bytes.toString(cell.getValueArray(), cell.getValueOffset(),
      cell.getValueLength());
    byte[] startcodeColumn = getStartCodeColumn(replicaId);
    cell = r.getColumnLatestCell(getCatalogFamily(), startcodeColumn);
    if (cell == null || cell.getValueLength() == 0) return Optional.empty();
    try {
      return Optional.of(ServerName.valueOf(hostAndPort,
        Bytes.toLong(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength())));
    } catch (IllegalArgumentException e) {
      LOG.error("Ignoring invalid region for server " + hostAndPort + "; cell=" + cell, e);
      return Optional.empty();
    }
  }

  /**
   * The latest seqnum that the server writing to meta observed when opening the region.
   * E.g. the seqNum when the result of {@link #getServerName(Result, int)} was written.
   * @param r Result to pull the seqNum from
   * @return SeqNum, or HConstants.NO_SEQNUM if there's no value written.
   */
  private static long getSeqNumDuringOpen(final Result r, final int replicaId) {
    Cell cell = r.getColumnLatestCell(getCatalogFamily(), getSeqNumColumn(replicaId));
    if (cell == null || cell.getValueLength() == 0) return HConstants.NO_SEQNUM;
    return Bytes.toLong(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
  }

  /**
   * @param tableName table we're working with
   * @return start row for scanning META according to query type
   */
  private static byte[] getTableStartRowForMeta(TableName tableName, QueryType type) {
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

  /**
   * @param tableName table we're working with
   * @return stop row for scanning META according to query type
   */
  private static byte[] getTableStopRowForMeta(TableName tableName, QueryType type) {
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

  /**
   * Returns the RegionInfo object from the column {@link HConstants#CATALOG_FAMILY} and
   * <code>qualifier</code> of the catalog table result.
   * @param r a Result object from the catalog table scan
   * @param qualifier Column family qualifier
   * @return An RegionInfo instance.
   */
  private static Optional<RegionInfo> getHRegionInfo(final Result r, byte[] qualifier) {
    Cell cell = r.getColumnLatestCell(getCatalogFamily(), qualifier);
    if (cell == null) return Optional.empty();
    return Optional.ofNullable(RegionInfo.parseFromOrNull(cell.getValueArray(),
      cell.getValueOffset(), cell.getValueLength()));
  }

  /**
   * Returns the column family used for meta columns.
   * @return HConstants.CATALOG_FAMILY.
   */
  private static byte[] getCatalogFamily() {
    return HConstants.CATALOG_FAMILY;
  }

  /**
   * Returns the column family used for table columns.
   * @return HConstants.TABLE_FAMILY.
   */
  private static byte[] getTableFamily() {
    return HConstants.TABLE_FAMILY;
  }

  /**
   * Returns the column qualifier for serialized region info
   * @return HConstants.REGIONINFO_QUALIFIER
   */
  private static byte[] getRegionInfoColumn() {
    return HConstants.REGIONINFO_QUALIFIER;
  }

  /**
   * Returns the column qualifier for serialized table state
   * @return HConstants.TABLE_STATE_QUALIFIER
   */
  private static byte[] getStateColumn() {
    return HConstants.TABLE_STATE_QUALIFIER;
  }

  /**
   * Returns the column qualifier for server column for replicaId
   * @param replicaId the replicaId of the region
   * @return a byte[] for server column qualifier
   */
  private static byte[] getServerColumn(int replicaId) {
    return replicaId == 0
      ? HConstants.SERVER_QUALIFIER
      : Bytes.toBytes(HConstants.SERVER_QUALIFIER_STR + META_REPLICA_ID_DELIMITER
      + String.format(RegionInfo.REPLICA_ID_FORMAT, replicaId));
  }

  /**
   * Returns the column qualifier for server start code column for replicaId
   * @param replicaId the replicaId of the region
   * @return a byte[] for server start code column qualifier
   */
  private static byte[] getStartCodeColumn(int replicaId) {
    return replicaId == 0
      ? HConstants.STARTCODE_QUALIFIER
      : Bytes.toBytes(HConstants.STARTCODE_QUALIFIER_STR + META_REPLICA_ID_DELIMITER
      + String.format(RegionInfo.REPLICA_ID_FORMAT, replicaId));
  }

  /**
   * Returns the column qualifier for seqNum column for replicaId
   * @param replicaId the replicaId of the region
   * @return a byte[] for seqNum column qualifier
   */
  private static byte[] getSeqNumColumn(int replicaId) {
    return replicaId == 0
      ? HConstants.SEQNUM_QUALIFIER
      : Bytes.toBytes(HConstants.SEQNUM_QUALIFIER_STR + META_REPLICA_ID_DELIMITER
      + String.format(RegionInfo.REPLICA_ID_FORMAT, replicaId));
  }

  /**
   * Parses the replicaId from the server column qualifier. See top of the class javadoc
   * for the actual meta layout
   * @param serverColumn the column qualifier
   * @return an int for the replicaId
   */
  private static int parseReplicaIdFromServerColumn(byte[] serverColumn) {
    String serverStr = Bytes.toString(serverColumn);

    Matcher matcher = SERVER_COLUMN_PATTERN.matcher(serverStr);
    if (matcher.matches() && matcher.groupCount() > 0) {
      String group = matcher.group(1);
      if (group != null && group.length() > 0) {
        return Integer.parseInt(group.substring(1), 16);
      } else {
        return 0;
      }
    }
    return -1;
  }
}
