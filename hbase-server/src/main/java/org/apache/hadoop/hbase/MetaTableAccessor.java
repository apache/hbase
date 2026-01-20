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

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell.Type;
import org.apache.hadoop.hbase.ClientMetaTableAccessor.QueryType;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Consistency;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableState;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.regionserver.RSAnnotationReadingPriorityFunction;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.ExceptionUtil;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.PairOfSameType;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Read/write operations on <code>hbase:meta</code> region as well as assignment information stored
 * to <code>hbase:meta</code>.
 * <p/>
 * Some of the methods of this class take ZooKeeperWatcher as a param. The only reason for this is
 * when this class is used on client-side (e.g. HBaseAdmin), we want to use short-lived connection
 * (opened before each operation, closed right after), while when used on HM or HRS (like in
 * AssignmentManager) we want permanent connection.
 * <p/>
 * HBASE-10070 adds a replicaId to HRI, meaning more than one HRI can be defined for the same table
 * range (table, startKey, endKey). For every range, there will be at least one HRI defined which is
 * called default replica.
 * <p/>
 * <h2>Meta layout</h2> For each table there is single row named for the table with a 'table' column
 * family. The column family currently has one column in it, the 'state' column:
 *
 * <pre>
 * table:state =&gt; contains table state
 * </pre>
 *
 * For the catalog family, see the comments of {@link CatalogFamilyFormat} for more details.
 * <p/>
 * TODO: Add rep_barrier for serial replication explanation. See SerialReplicationChecker.
 * <p/>
 * The actual layout of meta should be encapsulated inside MetaTableAccessor methods, and should not
 * leak out of it (through Result objects, etc)
 * @see CatalogFamilyFormat
 * @see ClientMetaTableAccessor
 */
@InterfaceAudience.Private
public final class MetaTableAccessor {

  private static final Logger LOG = LoggerFactory.getLogger(MetaTableAccessor.class);
  private static final Logger METALOG = LoggerFactory.getLogger("org.apache.hadoop.hbase.META");

  private MetaTableAccessor() {
  }

  ////////////////////////
  // Reading operations //
  ////////////////////////

  /**
   * Performs a full scan of <code>hbase:meta</code> for regions.
   * @param connection connection we're using
   * @param visitor    Visitor invoked against each row in regions family.
   */
  public static void fullScanRegions(Connection connection,
    final ClientMetaTableAccessor.Visitor visitor) throws IOException {
    scanMeta(connection, null, null, QueryType.REGION, visitor);
  }

  /**
   * Performs a full scan of <code>hbase:meta</code> for regions.
   * @param connection connection we're using
   */
  public static List<Result> fullScanRegions(Connection connection) throws IOException {
    return fullScan(connection, QueryType.REGION);
  }

  /**
   * Performs a full scan of <code>hbase:meta</code> for tables.
   * @param connection connection we're using
   * @param visitor    Visitor invoked against each row in tables family.
   */
  public static void fullScanTables(Connection connection,
    final ClientMetaTableAccessor.Visitor visitor) throws IOException {
    scanMeta(connection, null, null, QueryType.TABLE, visitor);
  }

  /**
   * Performs a full scan of <code>hbase:meta</code>.
   * @param connection connection we're using
   * @param type       scanned part of meta
   * @return List of {@link Result}
   */
  private static List<Result> fullScan(Connection connection, QueryType type) throws IOException {
    ClientMetaTableAccessor.CollectAllVisitor v = new ClientMetaTableAccessor.CollectAllVisitor();
    scanMeta(connection, null, null, type, v);
    return v.getResults();
  }

  /**
   * Callers should call close on the returned {@link Table} instance.
   * @param connection connection we're using to access Meta
   * @return An {@link Table} for <code>hbase:meta</code>
   * @throws NullPointerException if {@code connection} is {@code null}
   */
  public static Table getMetaHTable(final Connection connection) throws IOException {
    // We used to pass whole CatalogTracker in here, now we just pass in Connection
    Objects.requireNonNull(connection, "Connection cannot be null");
    if (connection.isClosed()) {
      throw new IOException("connection is closed");
    }
    return connection.getTable(connection.getMetaTableName());
  }

  /**
   * Gets the region info and assignment for the specified region.
   * @param connection connection we're using
   * @param regionName Region to lookup.
   * @return Location and RegionInfo for <code>regionName</code>
   * @deprecated use {@link #getRegionLocation(Connection, byte[])} instead
   */
  @Deprecated
  public static Pair<RegionInfo, ServerName> getRegion(Connection connection, byte[] regionName)
    throws IOException {
    HRegionLocation location = getRegionLocation(connection, regionName);
    return location == null ? null : new Pair<>(location.getRegion(), location.getServerName());
  }

  /**
   * Returns the HRegionLocation from meta for the given region
   * @param connection connection we're using
   * @param regionName region we're looking for
   * @return HRegionLocation for the given region
   */
  public static HRegionLocation getRegionLocation(Connection connection, byte[] regionName)
    throws IOException {
    byte[] row = regionName;
    RegionInfo parsedInfo = null;
    try {
      parsedInfo = CatalogFamilyFormat.parseRegionInfoFromRegionName(regionName);
      row = CatalogFamilyFormat.getMetaKeyForRegion(parsedInfo);
    } catch (Exception parseEx) {
      // Ignore. This is used with tableName passed as regionName.
    }
    Get get = new Get(row);
    get.addFamily(HConstants.CATALOG_FAMILY);
    get.setPriority(RSAnnotationReadingPriorityFunction.INTERNAL_READ_QOS);
    Result r;
    try (Table t = getMetaHTable(connection)) {
      r = t.get(get);
    }
    RegionLocations locations = CatalogFamilyFormat.getRegionLocations(r);
    return locations == null
      ? null
      : locations.getRegionLocation(
        parsedInfo == null ? RegionInfo.DEFAULT_REPLICA_ID : parsedInfo.getReplicaId());
  }

  /**
   * Returns the HRegionLocation from meta for the given region
   * @param connection connection we're using
   * @param regionInfo region information
   * @return HRegionLocation for the given region
   */
  public static HRegionLocation getRegionLocation(Connection connection, RegionInfo regionInfo)
    throws IOException {
    return CatalogFamilyFormat.getRegionLocation(getCatalogFamilyRow(connection, regionInfo),
      regionInfo, regionInfo.getReplicaId());
  }

  /** Returns Return the {@link HConstants#CATALOG_FAMILY} row from hbase:meta. */
  public static Result getCatalogFamilyRow(Connection connection, RegionInfo ri)
    throws IOException {
    Get get = new Get(CatalogFamilyFormat.getMetaKeyForRegion(ri));
    get.addFamily(HConstants.CATALOG_FAMILY);
    get.setPriority(RSAnnotationReadingPriorityFunction.INTERNAL_READ_QOS);
    try (Table t = getMetaHTable(connection)) {
      return t.get(get);
    }
  }

  /**
   * Gets the result in hbase:meta for the specified region.
   * @param connection connection we're using
   * @param regionInfo region we're looking for
   * @return result of the specified region
   */
  public static Result getRegionResult(Connection connection, RegionInfo regionInfo)
    throws IOException {
    return getCatalogFamilyRow(connection, regionInfo);
  }

  /**
   * Scans META table for a row whose key contains the specified <B>regionEncodedName</B>, returning
   * a single related <code>Result</code> instance if any row is found, null otherwise.
   * @param connection        the connection to query META table.
   * @param regionEncodedName the region encoded name to look for at META.
   * @return <code>Result</code> instance with the row related info in META, null otherwise.
   * @throws IOException if any errors occur while querying META.
   */
  public static Result scanByRegionEncodedName(Connection connection, String regionEncodedName)
    throws IOException {
    RowFilter rowFilter =
      new RowFilter(CompareOperator.EQUAL, new SubstringComparator(regionEncodedName));
    Scan scan = getMetaScan(connection.getConfiguration(), 1);
    scan.setFilter(rowFilter);
    try (Table table = getMetaHTable(connection);
      ResultScanner resultScanner = table.getScanner(scan)) {
      return resultScanner.next();
    }
  }

  /**
   * Lists all of the regions currently in META.
   * @param connection                  to connect with
   * @param excludeOfflinedSplitParents False if we are to include offlined/splitparents regions,
   *                                    true and we'll leave out offlined regions from returned list
   * @return List of all user-space regions.
   */
  public static List<RegionInfo> getAllRegions(Connection connection,
    boolean excludeOfflinedSplitParents) throws IOException {
    List<Pair<RegionInfo, ServerName>> result;

    result = getTableRegionsAndLocations(connection, null, excludeOfflinedSplitParents);

    return getListOfRegionInfos(result);

  }

  /**
   * Gets all of the regions of the specified table. Do not use this method to get meta table
   * regions, use methods in MetaTableLocator instead.
   * @param connection connection we're using
   * @param tableName  table we're looking for
   * @return Ordered list of {@link RegionInfo}.
   */
  public static List<RegionInfo> getTableRegions(Connection connection, TableName tableName)
    throws IOException {
    return getTableRegions(connection, tableName, false);
  }

  /**
   * Gets all of the regions of the specified table. Do not use this method to get meta table
   * regions, use methods in MetaTableLocator instead.
   * @param connection                  connection we're using
   * @param tableName                   table we're looking for
   * @param excludeOfflinedSplitParents If true, do not include offlined split parents in the
   *                                    return.
   * @return Ordered list of {@link RegionInfo}.
   */
  public static List<RegionInfo> getTableRegions(Connection connection, TableName tableName,
    final boolean excludeOfflinedSplitParents) throws IOException {
    List<Pair<RegionInfo, ServerName>> result =
      getTableRegionsAndLocations(connection, tableName, excludeOfflinedSplitParents);
    return getListOfRegionInfos(result);
  }

  private static List<RegionInfo>
    getListOfRegionInfos(final List<Pair<RegionInfo, ServerName>> pairs) {
    if (pairs == null || pairs.isEmpty()) {
      return Collections.emptyList();
    }
    List<RegionInfo> result = new ArrayList<>(pairs.size());
    for (Pair<RegionInfo, ServerName> pair : pairs) {
      result.add(pair.getFirst());
    }
    return result;
  }

  /**
   * This method creates a Scan object that will only scan catalog rows that belong to the specified
   * table. It doesn't specify any columns. This is a better alternative to just using a start row
   * and scan until it hits a new table since that requires parsing the HRI to get the table name.
   * @param tableName bytes of table's name
   * @return configured Scan object
   */
  public static Scan getScanForTableName(Configuration conf, TableName tableName) {
    // Start key is just the table name with delimiters
    byte[] startKey = ClientMetaTableAccessor.getTableStartRowForMeta(tableName, QueryType.REGION);
    // Stop key appends the smallest possible char to the table name
    byte[] stopKey = ClientMetaTableAccessor.getTableStopRowForMeta(tableName, QueryType.REGION);

    Scan scan = getMetaScan(conf, -1);
    scan.withStartRow(startKey);
    scan.withStopRow(stopKey);
    return scan;
  }

  private static Scan getMetaScan(Configuration conf, int rowUpperLimit) {
    Scan scan = new Scan();
    int scannerCaching = conf.getInt(HConstants.HBASE_META_SCANNER_CACHING,
      HConstants.DEFAULT_HBASE_META_SCANNER_CACHING);
    if (conf.getBoolean(HConstants.USE_META_REPLICAS, HConstants.DEFAULT_USE_META_REPLICAS)) {
      scan.setConsistency(Consistency.TIMELINE);
    }
    if (rowUpperLimit > 0) {
      scan.setLimit(rowUpperLimit);
      scan.setReadType(Scan.ReadType.PREAD);
    }
    scan.setCaching(scannerCaching);
    scan.setPriority(RSAnnotationReadingPriorityFunction.INTERNAL_READ_QOS);
    return scan;
  }

  /**
   * Do not use this method to get meta table regions, use methods in MetaTableLocator instead.
   * @param connection connection we're using
   * @param tableName  table we're looking for
   * @return Return list of regioninfos and server.
   */
  public static List<Pair<RegionInfo, ServerName>>
    getTableRegionsAndLocations(Connection connection, TableName tableName) throws IOException {
    return getTableRegionsAndLocations(connection, tableName, true);
  }

  /**
   * Do not use this method to get meta table regions, use methods in MetaTableLocator instead.
   * @param connection                  connection we're using
   * @param tableName                   table to work with, can be null for getting all regions
   * @param excludeOfflinedSplitParents don't return split parents
   * @return Return list of regioninfos and server addresses.
   */
  // What happens here when 1M regions in hbase:meta? This won't scale?
  public static List<Pair<RegionInfo, ServerName>> getTableRegionsAndLocations(
    Connection connection, @Nullable final TableName tableName,
    final boolean excludeOfflinedSplitParents) throws IOException {
    if (tableName != null && tableName.equals(connection.getMetaTableName())) {
      throw new IOException(
        "This method can't be used to locate meta regions; use MetaTableLocator instead");
    }
    // Make a version of CollectingVisitor that collects RegionInfo and ServerAddress
    ClientMetaTableAccessor.CollectRegionLocationsVisitor visitor =
      new ClientMetaTableAccessor.CollectRegionLocationsVisitor(excludeOfflinedSplitParents);
    scanMeta(connection,
      ClientMetaTableAccessor.getTableStartRowForMeta(tableName, QueryType.REGION),
      ClientMetaTableAccessor.getTableStopRowForMeta(tableName, QueryType.REGION), QueryType.REGION,
      visitor);
    return visitor.getResults();
  }

  public static void fullScanMetaAndPrint(Connection connection) throws IOException {
    ClientMetaTableAccessor.Visitor v = r -> {
      if (r == null || r.isEmpty()) {
        return true;
      }
      LOG.info("fullScanMetaAndPrint.Current Meta Row: {}", r);
      TableState state = CatalogFamilyFormat.getTableState(r);
      if (state != null) {
        LOG.info("fullScanMetaAndPrint.Table State={}", state);
      } else {
        RegionLocations locations = CatalogFamilyFormat.getRegionLocations(r);
        if (locations == null) {
          return true;
        }
        for (HRegionLocation loc : locations.getRegionLocations()) {
          if (loc != null) {
            LOG.info("fullScanMetaAndPrint.HRI Print={}", loc.getRegion());
          }
        }
      }
      return true;
    };
    scanMeta(connection, null, null, QueryType.ALL, v);
  }

  public static void scanMetaForTableRegions(Connection connection,
    ClientMetaTableAccessor.Visitor visitor, TableName tableName) throws IOException {
    scanMeta(connection, tableName, QueryType.REGION, Integer.MAX_VALUE, visitor);
  }

  private static void scanMeta(Connection connection, TableName table, QueryType type, int maxRows,
    final ClientMetaTableAccessor.Visitor visitor) throws IOException {
    scanMeta(connection, ClientMetaTableAccessor.getTableStartRowForMeta(table, type),
      ClientMetaTableAccessor.getTableStopRowForMeta(table, type), type, maxRows, visitor);
  }

  public static void scanMeta(Connection connection, @Nullable final byte[] startRow,
    @Nullable final byte[] stopRow, QueryType type, final ClientMetaTableAccessor.Visitor visitor)
    throws IOException {
    scanMeta(connection, startRow, stopRow, type, Integer.MAX_VALUE, visitor);
  }

  /**
   * Performs a scan of META table for given table starting from given row.
   * @param connection connection we're using
   * @param visitor    visitor to call
   * @param tableName  table withing we scan
   * @param row        start scan from this row
   * @param rowLimit   max number of rows to return
   */
  public static void scanMeta(Connection connection, final ClientMetaTableAccessor.Visitor visitor,
    final TableName tableName, final byte[] row, final int rowLimit) throws IOException {
    byte[] startRow = null;
    byte[] stopRow = null;
    if (tableName != null) {
      startRow = ClientMetaTableAccessor.getTableStartRowForMeta(tableName, QueryType.REGION);
      if (row != null) {
        RegionInfo closestRi = getClosestRegionInfo(connection, tableName, row);
        startRow =
          RegionInfo.createRegionName(tableName, closestRi.getStartKey(), HConstants.ZEROES, false);
      }
      stopRow = ClientMetaTableAccessor.getTableStopRowForMeta(tableName, QueryType.REGION);
    }
    scanMeta(connection, startRow, stopRow, QueryType.REGION, rowLimit, visitor);
  }

  /**
   * Performs a scan of META table.
   * @param connection connection we're using
   * @param startRow   Where to start the scan. Pass null if want to begin scan at first row.
   * @param stopRow    Where to stop the scan. Pass null if want to scan all rows from the start one
   * @param type       scanned part of meta
   * @param maxRows    maximum rows to return
   * @param visitor    Visitor invoked against each row.
   */
  public static void scanMeta(Connection connection, @Nullable final byte[] startRow,
    @Nullable final byte[] stopRow, QueryType type, int maxRows,
    final ClientMetaTableAccessor.Visitor visitor) throws IOException {
    scanMeta(connection, startRow, stopRow, type, null, maxRows, visitor);
  }

  /**
   * Performs a scan of META table.
   * @param connection connection we're using
   * @param startRow   Where to start the scan. Pass null if want to begin scan at first row.
   * @param stopRow    Where to stop the scan. Pass null if want to scan all rows from the start one
   * @param type       scanned part of meta
   * @param maxRows    maximum rows to return
   * @param visitor    Visitor invoked against each row.
   */
  public static void scanMeta(Connection connection, @Nullable final byte[] startRow,
    @Nullable final byte[] stopRow, QueryType type, @Nullable Filter filter, int maxRows,
    final ClientMetaTableAccessor.Visitor visitor) throws IOException {
    int rowUpperLimit = maxRows > 0 ? maxRows : Integer.MAX_VALUE;
    Scan scan = getMetaScan(connection.getConfiguration(), rowUpperLimit);

    for (byte[] family : type.getFamilies()) {
      scan.addFamily(family);
    }
    if (startRow != null) {
      scan.withStartRow(startRow);
    }
    if (stopRow != null) {
      scan.withStopRow(stopRow);
    }
    if (filter != null) {
      scan.setFilter(filter);
    }

    if (LOG.isTraceEnabled()) {
      LOG.trace(
        "Scanning META starting at row={} stopping at row={} for max={} with caching={} "
          + "priority={}",
        Bytes.toStringBinary(startRow), Bytes.toStringBinary(stopRow), rowUpperLimit,
        scan.getCaching(), scan.getPriority());
    }

    int currentRow = 0;
    try (Table metaTable = getMetaHTable(connection)) {
      try (ResultScanner scanner = metaTable.getScanner(scan)) {
        Result data;
        while ((data = scanner.next()) != null) {
          if (data.isEmpty()) {
            continue;
          }
          // Break if visit returns false.
          if (!visitor.visit(data)) {
            break;
          }
          if (++currentRow >= rowUpperLimit) {
            break;
          }
        }
      }
    }
    if (visitor instanceof Closeable) {
      try {
        ((Closeable) visitor).close();
      } catch (Throwable t) {
        ExceptionUtil.rethrowIfInterrupt(t);
        LOG.debug("Got exception in closing the meta scanner visitor", t);
      }
    }
  }

  /** Returns Get closest metatable region row to passed <code>row</code> */
  @NonNull
  private static RegionInfo getClosestRegionInfo(Connection connection,
    @NonNull final TableName tableName, @NonNull final byte[] row) throws IOException {
    byte[] searchRow = RegionInfo.createRegionName(tableName, row, HConstants.NINES, false);
    Scan scan = getMetaScan(connection.getConfiguration(), 1);
    scan.setReversed(true);
    scan.withStartRow(searchRow);
    try (ResultScanner resultScanner = getMetaHTable(connection).getScanner(scan)) {
      Result result = resultScanner.next();
      if (result == null) {
        throw new TableNotFoundException("Cannot find row in META " + " for table: " + tableName
          + ", row=" + Bytes.toStringBinary(row));
      }
      RegionInfo regionInfo = CatalogFamilyFormat.getRegionInfo(result);
      if (regionInfo == null) {
        throw new IOException("RegionInfo was null or empty in Meta for " + tableName + ", row="
          + Bytes.toStringBinary(row));
      }
      return regionInfo;
    }
  }

  /**
   * Returns the {@link ServerName} from catalog table {@link Result} where the region is
   * transitioning on. It should be the same as
   * {@link CatalogFamilyFormat#getServerName(Result,int)} if the server is at OPEN state.
   * @param r Result to pull the transitioning server name from
   * @return A ServerName instance or {@link CatalogFamilyFormat#getServerName(Result,int)} if
   *         necessary fields not found or empty.
   */
  @Nullable
  public static ServerName getTargetServerName(final Result r, final int replicaId) {
    final Cell cell = r.getColumnLatestCell(HConstants.CATALOG_FAMILY,
      CatalogFamilyFormat.getServerNameColumn(replicaId));
    if (cell == null || cell.getValueLength() == 0) {
      RegionLocations locations = CatalogFamilyFormat.getRegionLocations(r);
      if (locations != null) {
        HRegionLocation location = locations.getRegionLocation(replicaId);
        if (location != null) {
          return location.getServerName();
        }
      }
      return null;
    }
    return ServerName.parseServerName(
      Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
  }

  /**
   * Returns the daughter regions by reading the corresponding columns of the catalog table Result.
   * @param data a Result object from the catalog table scan
   * @return pair of RegionInfo or PairOfSameType(null, null) if region is not a split parent
   */
  public static PairOfSameType<RegionInfo> getDaughterRegions(Result data) {
    RegionInfo splitA = CatalogFamilyFormat.getRegionInfo(data, HConstants.SPLITA_QUALIFIER);
    RegionInfo splitB = CatalogFamilyFormat.getRegionInfo(data, HConstants.SPLITB_QUALIFIER);
    return new PairOfSameType<>(splitA, splitB);
  }

  /**
   * Fetch table state for given table from META table
   * @param conn      connection to use
   * @param tableName table to fetch state for
   */
  @Nullable
  public static TableState getTableState(Connection conn, TableName tableName) throws IOException {
    if (TableName.isMetaTableName(tableName)) {
      return new TableState(tableName, TableState.State.ENABLED);
    }
    Table metaHTable = getMetaHTable(conn);
    Get get = new Get(tableName.getName()).addColumn(HConstants.TABLE_FAMILY,
      HConstants.TABLE_STATE_QUALIFIER);
    Result result = metaHTable.get(get);
    return CatalogFamilyFormat.getTableState(result);
  }

  /**
   * Fetch table states from META table
   * @param conn connection to use
   * @return map {tableName -&gt; state}
   */
  public static Map<TableName, TableState> getTableStates(Connection conn) throws IOException {
    final Map<TableName, TableState> states = new LinkedHashMap<>();
    ClientMetaTableAccessor.Visitor collector = r -> {
      TableState state = CatalogFamilyFormat.getTableState(r);
      if (state != null) {
        states.put(state.getTableName(), state);
      }
      return true;
    };
    fullScanTables(conn, collector);
    return states;
  }

  /**
   * Updates state in META Do not use. For internal use only.
   * @param conn      connection to use
   * @param tableName table to look for
   */
  public static void updateTableState(Connection conn, TableName tableName, TableState.State actual)
    throws IOException {
    updateTableState(conn, new TableState(tableName, actual));
  }

  ////////////////////////
  // Editing operations //
  ////////////////////////

  /**
   * Generates and returns a {@link Put} containing the {@link RegionInfo} for the catalog table.
   * @throws IllegalArgumentException when the provided RegionInfo is not the default replica.
   */
  public static Put makePutFromRegionInfo(RegionInfo regionInfo) throws IOException {
    return makePutFromRegionInfo(regionInfo, EnvironmentEdgeManager.currentTime());
  }

  /**
   * Generates and returns a {@link Put} containing the {@link RegionInfo} for the catalog table.
   * @throws IllegalArgumentException when the provided RegionInfo is not the default replica.
   */
  public static Put makePutFromRegionInfo(RegionInfo regionInfo, long ts) throws IOException {
    byte[] metaKeyForRegion = CatalogFamilyFormat.getMetaKeyForRegion(regionInfo);
    try {
      Put put = new Put(metaKeyForRegion, ts);
      return addRegionInfo(put, regionInfo);
    } catch (IllegalArgumentException ex) {
      LOG.error(
        "Got exception while creating put for regioninfo {}." + "meta key for regioninfo is {}",
        regionInfo.getRegionNameAsString(), metaKeyForRegion);
      throw ex;
    }
  }

  /**
   * Generates and returns a Delete containing the region info for the catalog table
   */
  public static Delete makeDeleteFromRegionInfo(RegionInfo regionInfo, long ts) {
    if (regionInfo == null) {
      throw new IllegalArgumentException("Can't make a delete for null region");
    }
    if (regionInfo.getReplicaId() != RegionInfo.DEFAULT_REPLICA_ID) {
      throw new IllegalArgumentException(
        "Can't make delete for a replica region. Operate on the primary");
    }
    Delete delete = new Delete(CatalogFamilyFormat.getMetaKeyForRegion(regionInfo));
    delete.addFamily(HConstants.CATALOG_FAMILY, ts);
    return delete;
  }

  /**
   * Adds split daughters to the Put
   */
  public static Put addDaughtersToPut(Put put, RegionInfo splitA, RegionInfo splitB)
    throws IOException {
    if (splitA != null) {
      put.add(CellBuilderFactory.create(CellBuilderType.SHALLOW_COPY).setRow(put.getRow())
        .setFamily(HConstants.CATALOG_FAMILY).setQualifier(HConstants.SPLITA_QUALIFIER)
        .setTimestamp(put.getTimestamp()).setType(Type.Put).setValue(RegionInfo.toByteArray(splitA))
        .build());
    }
    if (splitB != null) {
      put.add(CellBuilderFactory.create(CellBuilderType.SHALLOW_COPY).setRow(put.getRow())
        .setFamily(HConstants.CATALOG_FAMILY).setQualifier(HConstants.SPLITB_QUALIFIER)
        .setTimestamp(put.getTimestamp()).setType(Type.Put).setValue(RegionInfo.toByteArray(splitB))
        .build());
    }
    return put;
  }

  /**
   * Put the passed <code>p</code> to the <code>hbase:meta</code> table.
   * @param connection connection we're using
   * @param p          Put to add to hbase:meta
   */
  private static void putToMetaTable(Connection connection, Put p) throws IOException {
    try (Table table = getMetaHTable(connection)) {
      put(table, p);
    }
  }

  /**
   * @param t Table to use
   * @param p put to make
   */
  private static void put(Table t, Put p) throws IOException {
    debugLogMutation(p);
    t.put(p);
  }

  /**
   * Put the passed <code>ps</code> to the <code>hbase:meta</code> table.
   * @param connection connection we're using
   * @param ps         Put to add to hbase:meta
   */
  public static void putsToMetaTable(final Connection connection, final List<Put> ps)
    throws IOException {
    if (ps.isEmpty()) {
      return;
    }
    try (Table t = getMetaHTable(connection)) {
      debugLogMutations(ps);
      // the implementation for putting a single Put is much simpler so here we do a check first.
      if (ps.size() == 1) {
        t.put(ps.get(0));
      } else {
        t.put(ps);
      }
    }
  }

  /**
   * Delete the passed <code>d</code> from the <code>hbase:meta</code> table.
   * @param connection connection we're using
   * @param d          Delete to add to hbase:meta
   */
  private static void deleteFromMetaTable(final Connection connection, final Delete d)
    throws IOException {
    List<Delete> dels = new ArrayList<>(1);
    dels.add(d);
    deleteFromMetaTable(connection, dels);
  }

  /**
   * Delete the passed <code>deletes</code> from the <code>hbase:meta</code> table.
   * @param connection connection we're using
   * @param deletes    Deletes to add to hbase:meta This list should support #remove.
   */
  private static void deleteFromMetaTable(final Connection connection, final List<Delete> deletes)
    throws IOException {
    try (Table t = getMetaHTable(connection)) {
      debugLogMutations(deletes);
      t.delete(deletes);
    }
  }

  /**
   * Set the column value corresponding to this {@code replicaId}'s {@link RegionState} to the
   * provided {@code state}. Mutates the provided {@link Put}.
   */
  public static Put addRegionStateToPut(Put put, int replicaId, RegionState.State state)
    throws IOException {
    put.add(CellBuilderFactory.create(CellBuilderType.SHALLOW_COPY).setRow(put.getRow())
      .setFamily(HConstants.CATALOG_FAMILY)
      .setQualifier(CatalogFamilyFormat.getRegionStateColumn(replicaId))
      .setTimestamp(put.getTimestamp()).setType(Cell.Type.Put).setValue(Bytes.toBytes(state.name()))
      .build());
    return put;
  }

  /**
   * Update state column in hbase:meta.
   */
  public static void updateRegionState(Connection connection, RegionInfo ri,
    RegionState.State state) throws IOException {
    final Put put = makePutFromRegionInfo(ri);
    addRegionStateToPut(put, ri.getReplicaId(), state);
    putsToMetaTable(connection, Collections.singletonList(put));
  }

  /**
   * Adds daughter region infos to hbase:meta row for the specified region.
   * <p/>
   * Note that this does not add its daughter's as different rows, but adds information about the
   * daughters in the same row as the parent. Now only used in snapshot. Use
   * {@link org.apache.hadoop.hbase.master.assignment.RegionStateStore} if you want to split a
   * region.
   * @param connection connection we're using
   * @param regionInfo RegionInfo of parent region
   * @param splitA     first split daughter of the parent regionInfo
   * @param splitB     second split daughter of the parent regionInfo
   * @throws IOException if problem connecting or updating meta
   */
  public static void addSplitsToParent(Connection connection, RegionInfo regionInfo,
    RegionInfo splitA, RegionInfo splitB) throws IOException {
    try (Table meta = getMetaHTable(connection)) {
      Put put = makePutFromRegionInfo(regionInfo);
      addDaughtersToPut(put, splitA, splitB);
      meta.put(put);
      debugLogMutation(put);
      LOG.debug("Added region {}", regionInfo.getRegionNameAsString());
    }
  }

  /**
   * Adds a hbase:meta row for each of the specified new regions. Initial state for new regions is
   * CLOSED.
   * @param connection  connection we're using
   * @param regionInfos region information list
   * @throws IOException if problem connecting or updating meta
   */
  public static void addRegionsToMeta(Connection connection, List<RegionInfo> regionInfos,
    int regionReplication) throws IOException {
    addRegionsToMeta(connection, regionInfos, regionReplication,
      EnvironmentEdgeManager.currentTime());
  }

  /**
   * Adds a hbase:meta row for each of the specified new regions. Initial state for new regions is
   * CLOSED.
   * @param connection  connection we're using
   * @param regionInfos region information list
   * @param ts          desired timestamp
   * @throws IOException if problem connecting or updating meta
   */
  public static void addRegionsToMeta(Connection connection, List<RegionInfo> regionInfos,
    int regionReplication, long ts) throws IOException {
    List<Put> puts = new ArrayList<>();
    for (RegionInfo regionInfo : regionInfos) {
      if (!RegionReplicaUtil.isDefaultReplica(regionInfo)) {
        continue;
      }
      Put put = makePutFromRegionInfo(regionInfo, ts);
      // New regions are added with initial state of CLOSED.
      addRegionStateToPut(put, regionInfo.getReplicaId(), RegionState.State.CLOSED);
      // Add empty locations for region replicas so that number of replicas can be cached
      // whenever the primary region is looked up from meta
      for (int i = 1; i < regionReplication; i++) {
        addEmptyLocation(put, i);
      }
      puts.add(put);
    }
    putsToMetaTable(connection, puts);
    LOG.info("Added {} regions to meta.", puts.size());
  }

  /**
   * Update state of the table in meta.
   * @param connection what we use for update
   * @param state      new state
   */
  private static void updateTableState(Connection connection, TableState state) throws IOException {
    Put put = makePutFromTableState(state, EnvironmentEdgeManager.currentTime());
    putToMetaTable(connection, put);
    LOG.info("Updated {} in {}", state, connection.getMetaTableName());
  }

  /**
   * Construct PUT for given state
   * @param state new state
   */
  public static Put makePutFromTableState(TableState state, long ts) {
    Put put = new Put(state.getTableName().getName(), ts);
    put.addColumn(HConstants.TABLE_FAMILY, HConstants.TABLE_STATE_QUALIFIER,
      state.convert().toByteArray());
    return put;
  }

  /**
   * Remove state for table from meta
   * @param connection to use for deletion
   * @param table      to delete state for
   */
  public static void deleteTableState(Connection connection, TableName table) throws IOException {
    long time = EnvironmentEdgeManager.currentTime();
    Delete delete = new Delete(table.getName());
    delete.addColumns(HConstants.TABLE_FAMILY, HConstants.TABLE_STATE_QUALIFIER, time);
    deleteFromMetaTable(connection, delete);
    LOG.info("Deleted table " + table + " state from META");
  }

  /**
   * Updates the location of the specified region in hbase:meta to be the specified server hostname
   * and startcode.
   * <p>
   * Uses passed catalog tracker to get a connection to the server hosting hbase:meta and makes
   * edits to that region.
   * @param connection       connection we're using
   * @param regionInfo       region to update location of
   * @param openSeqNum       the latest sequence number obtained when the region was open
   * @param sn               Server name
   * @param masterSystemTime wall clock time from master if passed in the open region RPC
   */
  public static void updateRegionLocation(Connection connection, RegionInfo regionInfo,
    ServerName sn, long openSeqNum, long masterSystemTime) throws IOException {
    updateLocation(connection, regionInfo, sn, openSeqNum, masterSystemTime);
  }

  /**
   * Updates the location of the specified region to be the specified server.
   * <p>
   * Connects to the specified server which should be hosting the specified catalog region name to
   * perform the edit.
   * @param connection       connection we're using
   * @param regionInfo       region to update location of
   * @param sn               Server name
   * @param openSeqNum       the latest sequence number obtained when the region was open
   * @param masterSystemTime wall clock time from master if passed in the open region RPC
   * @throws IOException In particular could throw {@link java.net.ConnectException} if the server
   *                     is down on other end.
   */
  private static void updateLocation(Connection connection, RegionInfo regionInfo, ServerName sn,
    long openSeqNum, long masterSystemTime) throws IOException {
    // region replicas are kept in the primary region's row
    Put put = new Put(CatalogFamilyFormat.getMetaKeyForRegion(regionInfo), masterSystemTime);
    addRegionInfo(put, regionInfo);
    addLocation(put, sn, openSeqNum, regionInfo.getReplicaId());
    putToMetaTable(connection, put);
    LOG.info("Updated row {} with server = {}", regionInfo.getRegionNameAsString(), sn);
  }

  public static Put addRegionInfo(final Put p, final RegionInfo hri) throws IOException {
    p.add(CellBuilderFactory.create(CellBuilderType.SHALLOW_COPY).setRow(p.getRow())
      .setFamily(HConstants.CATALOG_FAMILY).setQualifier(HConstants.REGIONINFO_QUALIFIER)
      .setTimestamp(p.getTimestamp()).setType(Type.Put)
      // Serialize the Default Replica HRI otherwise scan of hbase:meta
      // shows an info:regioninfo value with encoded name and region
      // name that differs from that of the hbase;meta row.
      .setValue(RegionInfo.toByteArray(RegionReplicaUtil.getRegionInfoForDefaultReplica(hri)))
      .build());
    return p;
  }

  public static Put addLocation(Put p, ServerName sn, long openSeqNum, int replicaId)
    throws IOException {
    CellBuilder builder = CellBuilderFactory.create(CellBuilderType.SHALLOW_COPY);
    return p
      .add(builder.clear().setRow(p.getRow()).setFamily(HConstants.CATALOG_FAMILY)
        .setQualifier(CatalogFamilyFormat.getServerColumn(replicaId)).setTimestamp(p.getTimestamp())
        .setType(Cell.Type.Put).setValue(Bytes.toBytes(sn.getAddress().toString())).build())
      .add(builder.clear().setRow(p.getRow()).setFamily(HConstants.CATALOG_FAMILY)
        .setQualifier(CatalogFamilyFormat.getStartCodeColumn(replicaId))
        .setTimestamp(p.getTimestamp()).setType(Cell.Type.Put)
        .setValue(Bytes.toBytes(sn.getStartCode())).build())
      .add(builder.clear().setRow(p.getRow()).setFamily(HConstants.CATALOG_FAMILY)
        .setQualifier(CatalogFamilyFormat.getSeqNumColumn(replicaId)).setTimestamp(p.getTimestamp())
        .setType(Type.Put).setValue(Bytes.toBytes(openSeqNum)).build());
  }

  public static Put addEmptyLocation(Put p, int replicaId) throws IOException {
    CellBuilder builder = CellBuilderFactory.create(CellBuilderType.SHALLOW_COPY);
    return p
      .add(builder.clear().setRow(p.getRow()).setFamily(HConstants.CATALOG_FAMILY)
        .setQualifier(CatalogFamilyFormat.getServerColumn(replicaId)).setTimestamp(p.getTimestamp())
        .setType(Type.Put).build())
      .add(builder.clear().setRow(p.getRow()).setFamily(HConstants.CATALOG_FAMILY)
        .setQualifier(CatalogFamilyFormat.getStartCodeColumn(replicaId))
        .setTimestamp(p.getTimestamp()).setType(Cell.Type.Put).build())
      .add(builder.clear().setRow(p.getRow()).setFamily(HConstants.CATALOG_FAMILY)
        .setQualifier(CatalogFamilyFormat.getSeqNumColumn(replicaId)).setTimestamp(p.getTimestamp())
        .setType(Cell.Type.Put).build());
  }

  private static void debugLogMutations(List<? extends Mutation> mutations) throws IOException {
    if (!METALOG.isDebugEnabled()) {
      return;
    }
    // Logging each mutation in separate line makes it easier to see diff between them visually
    // because of common starting indentation.
    for (Mutation mutation : mutations) {
      debugLogMutation(mutation);
    }
  }

  private static void debugLogMutation(Mutation p) throws IOException {
    METALOG.debug("{} {}", p.getClass().getSimpleName(), p.toJSON());
  }
}
