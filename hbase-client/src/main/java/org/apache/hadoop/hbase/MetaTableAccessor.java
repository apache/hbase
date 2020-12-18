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
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell.Type;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Consistency;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableState;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcUtils;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.master.RegionState.State;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.protobuf.generated.MultiRowMutationProtos.MultiRowMutationService;
import org.apache.hadoop.hbase.protobuf.generated.MultiRowMutationProtos.MutateRowsRequest;
import org.apache.hadoop.hbase.protobuf.generated.MultiRowMutationProtos.MutateRowsResponse;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.ExceptionUtil;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.PairOfSameType;
import org.apache.hbase.thirdparty.com.google.common.base.Throwables;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 * Read/write operations on <code>hbase:meta</code> region as well as assignment information stored
 * to <code>hbase:meta</code>.
 * </p>
 * <p>
 * Some of the methods of this class take ZooKeeperWatcher as a param. The only reason for this is
 * when this class is used on client-side (e.g. HBaseAdmin), we want to use short-lived connection
 * (opened before each operation, closed right after), while when used on HM or HRS (like in
 * AssignmentManager) we want permanent connection.
 * </p>
 * <p>
 * HBASE-10070 adds a replicaId to HRI, meaning more than one HRI can be defined for the same table
 * range (table, startKey, endKey). For every range, there will be at least one HRI defined which is
 * called default replica.
 * </p>
 * <p>
 * <h2>Meta layout</h2>
 *
 * <pre>
 * For each table there is single row named for the table with a 'table' column family.
 * The column family currently has one column in it, the 'state' column:
 *
 * table:state             => contains table state
 *
 * Then for each table range ('Region'), there is a single row, formatted as:
 * &lt;tableName&gt;,&lt;startKey&gt;,&lt;regionId&gt;,&lt;encodedRegionName&gt;.
 * This row is the serialized regionName of the default region replica.
 * Columns are:
 * info:regioninfo         => contains serialized HRI for the default region replica
 * info:server             => contains hostname:port (in string form) for the server hosting
 *                            the default regionInfo replica
 * info:server_&lt;replicaId&gt => contains hostname:port (in string form) for the server hosting
 *                                 the regionInfo replica with replicaId
 * info:serverstartcode    => contains server start code (in binary long form) for the server
 *                            hosting the default regionInfo replica
 * info:serverstartcode_&lt;replicaId&gt => contains server start code (in binary long form) for
 *                                          the server hosting the regionInfo replica with
 *                                          replicaId
 * info:seqnumDuringOpen   => contains seqNum (in binary long form) for the region at the time
 *                             the server opened the region with default replicaId
 * info:seqnumDuringOpen_&lt;replicaId&gt => contains seqNum (in binary long form) for the region
 *                                           at the time the server opened the region with
 *                                           replicaId
 * info:splitA             => contains a serialized HRI for the first daughter region if the
 *                             region is split
 * info:splitB             => contains a serialized HRI for the second daughter region if the
 *                             region is split
 * info:merge*             => contains a serialized HRI for a merge parent region. There will be two
 *                             or more of these columns in a row. A row that has these columns is
 *                             undergoing a merge and is the result of the merge. Columns listed
 *                             in marge* columns are the parents of this merged region. Example
 *                             columns: info:merge0001, info:merge0002. You make also see 'mergeA',
 *                             and 'mergeB'. This is old form replaced by the new format that allows
 *                             for more than two parents to be merged at a time.
 * TODO: Add rep_barrier for serial replication explaination. See SerialReplicationChecker.
 * </pre>
 * </p>
 * <p>
 * The actual layout of meta should be encapsulated inside MetaTableAccessor methods, and should not
 * leak out of it (through Result objects, etc)
 * </p>
 */
@InterfaceAudience.Private
public class MetaTableAccessor {

  private static final Logger LOG = LoggerFactory.getLogger(MetaTableAccessor.class);
  private static final Logger METALOG = LoggerFactory.getLogger("org.apache.hadoop.hbase.META");

  public static final byte[] REPLICATION_PARENT_QUALIFIER = Bytes.toBytes("parent");

  private static final byte ESCAPE_BYTE = (byte) 0xFF;

  private static final byte SEPARATED_BYTE = 0x00;

  @InterfaceAudience.Private
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

  /** The delimiter for meta columns for replicaIds &gt; 0 */
  static final char META_REPLICA_ID_DELIMITER = '_';

  /** A regex for parsing server columns from meta. See above javadoc for meta layout */
  private static final Pattern SERVER_COLUMN_PATTERN
    = Pattern.compile("^server(_[0-9a-fA-F]{4})?$");

  ////////////////////////
  // Reading operations //
  ////////////////////////

  /**
   * Performs a full scan of <code>hbase:meta</code> for regions.
   * @param connection connection we're using
   * @param visitor Visitor invoked against each row in regions family.
   */
  public static void fullScanRegions(Connection connection, final Visitor visitor)
      throws IOException {
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
   * @param visitor Visitor invoked against each row in tables family.
   */
  public static void fullScanTables(Connection connection, final Visitor visitor)
      throws IOException {
    scanMeta(connection, null, null, QueryType.TABLE, visitor);
  }

  /**
   * Performs a full scan of <code>hbase:meta</code>.
   * @param connection connection we're using
   * @param type scanned part of meta
   * @return List of {@link Result}
   */
  private static List<Result> fullScan(Connection connection, QueryType type) throws IOException {
    CollectAllVisitor v = new CollectAllVisitor();
    scanMeta(connection, null, null, type, v);
    return v.getResults();
  }

  /**
   * Callers should call close on the returned {@link Table} instance.
   * @param connection connection we're using to access Meta
   * @return An {@link Table} for <code>hbase:meta</code>
   */
  public static Table getMetaHTable(final Connection connection) throws IOException {
    // We used to pass whole CatalogTracker in here, now we just pass in Connection
    if (connection == null) {
      throw new NullPointerException("No connection");
    } else if (connection.isClosed()) {
      throw new IOException("connection is closed");
    }
    return connection.getTable(TableName.META_TABLE_NAME);
  }

  /**
   * @param t Table to use (will be closed when done).
   * @param g Get to run
   */
  private static Result get(final Table t, final Get g) throws IOException {
    if (t == null) return null;
    try {
      return t.get(g);
    } finally {
      t.close();
    }
  }

  /**
   * Gets the region info and assignment for the specified region.
   * @param connection connection we're using
   * @param regionName Region to lookup.
   * @return Location and RegionInfo for <code>regionName</code>
   * @deprecated use {@link #getRegionLocation(Connection, byte[])} instead
   */
  @Deprecated
  public static Pair<RegionInfo, ServerName> getRegion(Connection connection, byte [] regionName)
    throws IOException {
    HRegionLocation location = getRegionLocation(connection, regionName);
    return location == null
      ? null
      : new Pair<>(location.getRegionInfo(), location.getServerName());
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
      parsedInfo = parseRegionInfoFromRegionName(regionName);
      row = getMetaKeyForRegion(parsedInfo);
    } catch (Exception parseEx) {
      // If it is not a valid regionName(i.e, tableName), it needs to return null here
      // as querying meta table wont help.
      return null;
    }
    Get get = new Get(row);
    get.addFamily(HConstants.CATALOG_FAMILY);
    Result r = get(getMetaHTable(connection), get);
    RegionLocations locations = getRegionLocations(r);
    return locations == null ? null
      : locations.getRegionLocation(parsedInfo == null ? 0 : parsedInfo.getReplicaId());
  }

  /**
   * Returns the HRegionLocation from meta for the given region
   * @param connection connection we're using
   * @param regionInfo region information
   * @return HRegionLocation for the given region
   */
  public static HRegionLocation getRegionLocation(Connection connection, RegionInfo regionInfo)
      throws IOException {
    return getRegionLocation(getCatalogFamilyRow(connection, regionInfo),
        regionInfo, regionInfo.getReplicaId());
  }

  /**
   * @return Return the {@link HConstants#CATALOG_FAMILY} row from hbase:meta.
   */
  public static Result getCatalogFamilyRow(Connection connection, RegionInfo ri)
      throws IOException {
    Get get = new Get(getMetaKeyForRegion(ri));
    get.addFamily(HConstants.CATALOG_FAMILY);
    return get(getMetaHTable(connection), get);
  }

  /** Returns the row key to use for this regionInfo */
  public static byte[] getMetaKeyForRegion(RegionInfo regionInfo) {
    return RegionReplicaUtil.getRegionInfoForDefaultReplica(regionInfo).getRegionName();
  }

  /** Returns an HRI parsed from this regionName. Not all the fields of the HRI
   * is stored in the name, so the returned object should only be used for the fields
   * in the regionName.
   */
  // This should be moved to RegionInfo? TODO.
  public static RegionInfo parseRegionInfoFromRegionName(byte[] regionName) throws IOException {
    byte[][] fields = RegionInfo.parseRegionName(regionName);
    long regionId = Long.parseLong(Bytes.toString(fields[2]));
    int replicaId = fields.length > 3 ? Integer.parseInt(Bytes.toString(fields[3]), 16) : 0;
    return RegionInfoBuilder.newBuilder(TableName.valueOf(fields[0]))
      .setStartKey(fields[1]).setRegionId(regionId).setReplicaId(replicaId).build();
  }

  /**
   * Gets the result in hbase:meta for the specified region.
   * @param connection connection we're using
   * @param regionName region we're looking for
   * @return result of the specified region
   */
  public static Result getRegionResult(Connection connection,
      byte[] regionName) throws IOException {
    Get get = new Get(regionName);
    get.addFamily(HConstants.CATALOG_FAMILY);
    return get(getMetaHTable(connection), get);
  }

  /**
   * Scans META table for a row whose key contains the specified <B>regionEncodedName</B>,
   * returning a single related <code>Result</code> instance if any row is found, null otherwise.
   *
   * @param connection the connection to query META table.
   * @param regionEncodedName the region encoded name to look for at META.
   * @return <code>Result</code> instance with the row related info in META, null otherwise.
   * @throws IOException if any errors occur while querying META.
   */
  public static Result scanByRegionEncodedName(Connection connection,
      String regionEncodedName) throws IOException {
    RowFilter rowFilter = new RowFilter(CompareOperator.EQUAL,
      new SubstringComparator(regionEncodedName));
    Scan scan = getMetaScan(connection.getConfiguration(), 1);
    scan.setFilter(rowFilter);
    try (Table table = getMetaHTable(connection);
        ResultScanner resultScanner = table.getScanner(scan)) {
      return resultScanner.next();
    }
  }

  /**
   * @return Return all regioninfos listed in the 'info:merge*' columns of
   *   the <code>regionName</code> row.
   */
  @Nullable
  public static List<RegionInfo> getMergeRegions(Connection connection, byte[] regionName)
      throws IOException {
    return getMergeRegions(getRegionResult(connection, regionName).rawCells());
  }

  /**
   * Check whether the given {@code regionName} has any 'info:merge*' columns.
   */
  public static boolean hasMergeRegions(Connection conn, byte[] regionName) throws IOException {
    return hasMergeRegions(getRegionResult(conn, regionName).rawCells());
  }

  /**
   * @return Deserialized values of &lt;qualifier,regioninfo&gt; pairs taken from column values that
   *         match the regex 'info:merge.*' in array of <code>cells</code>.
   */
  @Nullable
  public static Map<String, RegionInfo> getMergeRegionsWithName(Cell [] cells) {
    if (cells == null) {
      return null;
    }
    Map<String, RegionInfo> regionsToMerge = null;
    for (Cell cell: cells) {
      if (!isMergeQualifierPrefix(cell)) {
        continue;
      }
      // Ok. This cell is that of a info:merge* column.
      RegionInfo ri = RegionInfo.parseFromOrNull(cell.getValueArray(), cell.getValueOffset(),
        cell.getValueLength());
      if (ri != null) {
        if (regionsToMerge == null) {
          regionsToMerge = new LinkedHashMap<>();
        }
        regionsToMerge.put(Bytes.toString(CellUtil.cloneQualifier(cell)), ri);
      }
    }
    return regionsToMerge;
  }

  /**
   * @return Deserialized regioninfo values taken from column values that match
   *   the regex 'info:merge.*' in array of <code>cells</code>.
   */
  @Nullable
  public static List<RegionInfo> getMergeRegions(Cell [] cells) {
    Map<String, RegionInfo> mergeRegionsWithName = getMergeRegionsWithName(cells);
    return (mergeRegionsWithName == null) ? null : new ArrayList<>(mergeRegionsWithName.values());
  }

  /**
   * @return True if any merge regions present in <code>cells</code>; i.e.
   *   the column in <code>cell</code> matches the regex 'info:merge.*'.
   */
  public static boolean hasMergeRegions(Cell [] cells) {
    for (Cell cell: cells) {
      if (!isMergeQualifierPrefix(cell)) {
        continue;
      }
      return true;
    }
    return false;
  }

  /**
   * @return True if the column in <code>cell</code> matches the regex 'info:merge.*'.
   */
  private static boolean isMergeQualifierPrefix(Cell cell) {
    // Check to see if has family and that qualifier starts with the merge qualifier 'merge'
    return CellUtil.matchingFamily(cell, HConstants.CATALOG_FAMILY) &&
      PrivateCellUtil.qualifierStartsWith(cell, HConstants.MERGE_QUALIFIER_PREFIX);
  }

  /**
   * Lists all of the regions currently in META.
   *
   * @param connection to connect with
   * @param excludeOfflinedSplitParents False if we are to include offlined/splitparents regions,
   *                                    true and we'll leave out offlined regions from returned list
   * @return List of all user-space regions.
   */
  public static List<RegionInfo> getAllRegions(Connection connection,
      boolean excludeOfflinedSplitParents)
      throws IOException {
    List<Pair<RegionInfo, ServerName>> result;

    result = getTableRegionsAndLocations(connection, null,
        excludeOfflinedSplitParents);

    return getListOfRegionInfos(result);

  }

  /**
   * Gets all of the regions of the specified table. Do not use this method
   * to get meta table regions, use methods in MetaTableLocator instead.
   * @param connection connection we're using
   * @param tableName table we're looking for
   * @return Ordered list of {@link RegionInfo}.
   */
  public static List<RegionInfo> getTableRegions(Connection connection, TableName tableName)
  throws IOException {
    return getTableRegions(connection, tableName, false);
  }

  /**
   * Gets all of the regions of the specified table. Do not use this method
   * to get meta table regions, use methods in MetaTableLocator instead.
   * @param connection connection we're using
   * @param tableName table we're looking for
   * @param excludeOfflinedSplitParents If true, do not include offlined split
   * parents in the return.
   * @return Ordered list of {@link RegionInfo}.
   */
  public static List<RegionInfo> getTableRegions(Connection connection, TableName tableName,
      final boolean excludeOfflinedSplitParents) throws IOException {
    List<Pair<RegionInfo, ServerName>> result =
      getTableRegionsAndLocations(connection, tableName, excludeOfflinedSplitParents);
    return getListOfRegionInfos(result);
  }

  private static List<RegionInfo> getListOfRegionInfos(
      final List<Pair<RegionInfo, ServerName>> pairs) {
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
   * @param tableName table we're working with
   * @return start row for scanning META according to query type
   */
  public static byte[] getTableStartRowForMeta(TableName tableName, QueryType type) {
    if (tableName == null) {
      return null;
    }
    switch (type) {
    case REGION:
      byte[] startRow = new byte[tableName.getName().length + 2];
      System.arraycopy(tableName.getName(), 0, startRow, 0, tableName.getName().length);
      startRow[startRow.length - 2] = HConstants.DELIMITER;
      startRow[startRow.length - 1] = HConstants.DELIMITER;
      return startRow;
    case ALL:
    case TABLE:
    default:
      return tableName.getName();
    }
  }

  /**
   * @param tableName table we're working with
   * @return stop row for scanning META according to query type
   */
  public static byte[] getTableStopRowForMeta(TableName tableName, QueryType type) {
    if (tableName == null) {
      return null;
    }
    final byte[] stopRow;
    switch (type) {
    case REGION:
      stopRow = new byte[tableName.getName().length + 3];
      System.arraycopy(tableName.getName(), 0, stopRow, 0, tableName.getName().length);
      stopRow[stopRow.length - 3] = ' ';
      stopRow[stopRow.length - 2] = HConstants.DELIMITER;
      stopRow[stopRow.length - 1] = HConstants.DELIMITER;
      break;
    case ALL:
    case TABLE:
    default:
      stopRow = new byte[tableName.getName().length + 1];
      System.arraycopy(tableName.getName(), 0, stopRow, 0, tableName.getName().length);
      stopRow[stopRow.length - 1] = ' ';
      break;
    }
    return stopRow;
  }

  /**
   * This method creates a Scan object that will only scan catalog rows that
   * belong to the specified table. It doesn't specify any columns.
   * This is a better alternative to just using a start row and scan until
   * it hits a new table since that requires parsing the HRI to get the table
   * name.
   * @param tableName bytes of table's name
   * @return configured Scan object
   */
  public static Scan getScanForTableName(Configuration conf, TableName tableName) {
    // Start key is just the table name with delimiters
    byte[] startKey = getTableStartRowForMeta(tableName, QueryType.REGION);
    // Stop key appends the smallest possible char to the table name
    byte[] stopKey = getTableStopRowForMeta(tableName, QueryType.REGION);

    Scan scan = getMetaScan(conf, -1);
    scan.setStartRow(startKey);
    scan.setStopRow(stopKey);
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
    return scan;
  }

  /**
   * Do not use this method to get meta table regions, use methods in MetaTableLocator instead.
   * @param connection connection we're using
   * @param tableName table we're looking for
   * @return Return list of regioninfos and server.
   */
  public static List<Pair<RegionInfo, ServerName>>
    getTableRegionsAndLocations(Connection connection, TableName tableName)
      throws IOException {
    return getTableRegionsAndLocations(connection, tableName, true);
  }

  /**
   * Do not use this method to get meta table regions, use methods in MetaTableLocator instead.
   * @param connection connection we're using
   * @param tableName table to work with, can be null for getting all regions
   * @param excludeOfflinedSplitParents don't return split parents
   * @return Return list of regioninfos and server addresses.
   */
  // What happens here when 1M regions in hbase:meta? This won't scale?
  public static List<Pair<RegionInfo, ServerName>> getTableRegionsAndLocations(
      Connection connection, @Nullable final TableName tableName,
      final boolean excludeOfflinedSplitParents) throws IOException {
    if (tableName != null && tableName.equals(TableName.META_TABLE_NAME)) {
      throw new IOException("This method can't be used to locate meta regions;"
        + " use MetaTableLocator instead");
    }
    // Make a version of CollectingVisitor that collects RegionInfo and ServerAddress
    CollectingVisitor<Pair<RegionInfo, ServerName>> visitor =
      new CollectingVisitor<Pair<RegionInfo, ServerName>>() {
        private RegionLocations current = null;

        @Override
        public boolean visit(Result r) throws IOException {
          current = getRegionLocations(r);
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
              this.results.add(new Pair<>(loc.getRegion(), loc.getServerName()));
            }
          }
        }
      };
    scanMeta(connection,
        getTableStartRowForMeta(tableName, QueryType.REGION),
        getTableStopRowForMeta(tableName, QueryType.REGION),
        QueryType.REGION, visitor);
    return visitor.getResults();
  }

  /**
   * @param connection connection we're using
   * @param serverName server whose regions we're interested in
   * @return List of user regions installed on this server (does not include
   * catalog regions).
   * @throws IOException
   */
  public static NavigableMap<RegionInfo, Result>
  getServerUserRegions(Connection connection, final ServerName serverName)
    throws IOException {
    final NavigableMap<RegionInfo, Result> hris = new TreeMap<>();
    // Fill the above hris map with entries from hbase:meta that have the passed
    // servername.
    CollectingVisitor<Result> v = new CollectingVisitor<Result>() {
      @Override
      void add(Result r) {
        if (r == null || r.isEmpty()) return;
        RegionLocations locations = getRegionLocations(r);
        if (locations == null) return;
        for (HRegionLocation loc : locations.getRegionLocations()) {
          if (loc != null) {
            if (loc.getServerName() != null && loc.getServerName().equals(serverName)) {
              hris.put(loc.getRegion(), r);
            }
          }
        }
      }
    };
    scanMeta(connection, null, null, QueryType.REGION, v);
    return hris;
  }

  public static void fullScanMetaAndPrint(Connection connection)
    throws IOException {
    Visitor v = r -> {
      if (r ==  null || r.isEmpty()) {
        return true;
      }
      LOG.info("fullScanMetaAndPrint.Current Meta Row: " + r);
      TableState state = getTableState(r);
      if (state != null) {
        LOG.info("fullScanMetaAndPrint.Table State={}" + state);
      } else {
        RegionLocations locations = getRegionLocations(r);
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

  public static void scanMetaForTableRegions(Connection connection, Visitor visitor,
      TableName tableName) throws IOException {
    scanMeta(connection, tableName, QueryType.REGION, Integer.MAX_VALUE, visitor);
  }

  private static void scanMeta(Connection connection, TableName table, QueryType type, int maxRows,
      final Visitor visitor) throws IOException {
    scanMeta(connection, getTableStartRowForMeta(table, type), getTableStopRowForMeta(table, type),
      type, maxRows, visitor);
  }

  private static void scanMeta(Connection connection, @Nullable final byte[] startRow,
      @Nullable final byte[] stopRow, QueryType type, final Visitor visitor) throws IOException {
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
  public static void scanMeta(Connection connection, final Visitor visitor,
      final TableName tableName, final byte[] row, final int rowLimit) throws IOException {
    byte[] startRow = null;
    byte[] stopRow = null;
    if (tableName != null) {
      startRow = getTableStartRowForMeta(tableName, QueryType.REGION);
      if (row != null) {
        RegionInfo closestRi = getClosestRegionInfo(connection, tableName, row);
        startRow =
          RegionInfo.createRegionName(tableName, closestRi.getStartKey(), HConstants.ZEROES, false);
      }
      stopRow = getTableStopRowForMeta(tableName, QueryType.REGION);
    }
    scanMeta(connection, startRow, stopRow, QueryType.REGION, rowLimit, visitor);
  }

  /**
   * Performs a scan of META table.
   * @param connection connection we're using
   * @param startRow Where to start the scan. Pass null if want to begin scan
   *                 at first row.
   * @param stopRow Where to stop the scan. Pass null if want to scan all rows
   *                from the start one
   * @param type scanned part of meta
   * @param maxRows maximum rows to return
   * @param visitor Visitor invoked against each row.
   */
  static void scanMeta(Connection connection, @Nullable final byte[] startRow,
        @Nullable final byte[] stopRow, QueryType type, int maxRows, final Visitor visitor)
      throws IOException {
    scanMeta(connection, startRow, stopRow, type, null, maxRows, visitor);
  }

  private static void scanMeta(Connection connection, @Nullable final byte[] startRow,
      @Nullable final byte[] stopRow, QueryType type, @Nullable Filter filter, int maxRows,
      final Visitor visitor) throws IOException {
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
      LOG.trace("Scanning META" + " starting at row=" + Bytes.toStringBinary(startRow) +
        " stopping at row=" + Bytes.toStringBinary(stopRow) + " for max=" + rowUpperLimit +
        " with caching=" + scan.getCaching());
    }

    int currentRow = 0;
    try (Table metaTable = getMetaHTable(connection)) {
      try (ResultScanner scanner = metaTable.getScanner(scan)) {
        Result data;
        while ((data = scanner.next()) != null) {
          if (data.isEmpty()) continue;
          // Break if visit returns false.
          if (!visitor.visit(data)) break;
          if (++currentRow >= rowUpperLimit) break;
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

  /**
   * @return Get closest metatable region row to passed <code>row</code>
   */
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
        throw new TableNotFoundException("Cannot find row in META " +
            " for table: " + tableName + ", row=" + Bytes.toStringBinary(row));
      }
      RegionInfo regionInfo = getRegionInfo(result);
      if (regionInfo == null) {
        throw new IOException("RegionInfo was null or empty in Meta for " +
            tableName + ", row=" + Bytes.toStringBinary(row));
      }
      return regionInfo;
    }
  }

  /**
   * Returns the column family used for meta columns.
   * @return HConstants.CATALOG_FAMILY.
   */
  public static byte[] getCatalogFamily() {
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
  public static byte[] getRegionInfoColumn() {
    return HConstants.REGIONINFO_QUALIFIER;
  }

  /**
   * Returns the column qualifier for serialized table state
   * @return HConstants.TABLE_STATE_QUALIFIER
   */
  private static byte[] getTableStateColumn() {
    return HConstants.TABLE_STATE_QUALIFIER;
  }

  /**
   * Returns the column qualifier for serialized region state
   * @return HConstants.STATE_QUALIFIER
   */
  private static byte[] getRegionStateColumn() {
    return HConstants.STATE_QUALIFIER;
  }

  /**
   * Returns the column qualifier for serialized region state
   * @param replicaId the replicaId of the region
   * @return a byte[] for state qualifier
   */
  public static byte[] getRegionStateColumn(int replicaId) {
    return replicaId == 0 ? HConstants.STATE_QUALIFIER
        : Bytes.toBytes(HConstants.STATE_QUALIFIER_STR + META_REPLICA_ID_DELIMITER
            + String.format(RegionInfo.REPLICA_ID_FORMAT, replicaId));
  }

  /**
   * Returns the column qualifier for serialized region state
   * @param replicaId the replicaId of the region
   * @return a byte[] for sn column qualifier
   */
  public static byte[] getServerNameColumn(int replicaId) {
    return replicaId == 0 ? HConstants.SERVERNAME_QUALIFIER
        : Bytes.toBytes(HConstants.SERVERNAME_QUALIFIER_STR + META_REPLICA_ID_DELIMITER
            + String.format(RegionInfo.REPLICA_ID_FORMAT, replicaId));
  }

  /**
   * Returns the column qualifier for server column for replicaId
   * @param replicaId the replicaId of the region
   * @return a byte[] for server column qualifier
   */
  public static byte[] getServerColumn(int replicaId) {
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
  public static byte[] getStartCodeColumn(int replicaId) {
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
  public static byte[] getSeqNumColumn(int replicaId) {
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
  static int parseReplicaIdFromServerColumn(byte[] serverColumn) {
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

  /**
   * Returns a {@link ServerName} from catalog table {@link Result}.
   * @param r Result to pull from
   * @return A ServerName instance or null if necessary fields not found or empty.
   */
  @Nullable
  @InterfaceAudience.Private // for use by HMaster#getTableRegionRow which is used for testing only
  public static ServerName getServerName(final Result r, final int replicaId) {
    byte[] serverColumn = getServerColumn(replicaId);
    Cell cell = r.getColumnLatestCell(getCatalogFamily(), serverColumn);
    if (cell == null || cell.getValueLength() == 0) return null;
    String hostAndPort = Bytes.toString(
      cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
    byte[] startcodeColumn = getStartCodeColumn(replicaId);
    cell = r.getColumnLatestCell(getCatalogFamily(), startcodeColumn);
    if (cell == null || cell.getValueLength() == 0) return null;
    try {
      return ServerName.valueOf(hostAndPort,
          Bytes.toLong(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
    } catch (IllegalArgumentException e) {
      LOG.error("Ignoring invalid region for server " + hostAndPort + "; cell=" + cell, e);
      return null;
    }
  }

  /**
   * Returns the {@link ServerName} from catalog table {@link Result} where the region is
   * transitioning on. It should be the same as {@link MetaTableAccessor#getServerName(Result,int)}
   * if the server is at OPEN state.
   *
   * @param r Result to pull the transitioning server name from
   * @return A ServerName instance or {@link MetaTableAccessor#getServerName(Result,int)}
   * if necessary fields not found or empty.
   */
  @Nullable
  public static ServerName getTargetServerName(final Result r, final int replicaId) {
    final Cell cell = r.getColumnLatestCell(HConstants.CATALOG_FAMILY,
      getServerNameColumn(replicaId));
    if (cell == null || cell.getValueLength() == 0) {
      RegionLocations locations = MetaTableAccessor.getRegionLocations(r);
      if (locations != null) {
        HRegionLocation location = locations.getRegionLocation(replicaId);
        if (location != null) {
          return location.getServerName();
        }
      }
      return null;
    }
    return ServerName.parseServerName(Bytes.toString(cell.getValueArray(), cell.getValueOffset(),
      cell.getValueLength()));
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
   * Returns the daughter regions by reading the corresponding columns of the catalog table
   * Result.
   * @param data a Result object from the catalog table scan
   * @return pair of RegionInfo or PairOfSameType(null, null) if region is not a split parent
   */
  public static PairOfSameType<RegionInfo> getDaughterRegions(Result data) {
    RegionInfo splitA = getRegionInfo(data, HConstants.SPLITA_QUALIFIER);
    RegionInfo splitB = getRegionInfo(data, HConstants.SPLITB_QUALIFIER);
    return new PairOfSameType<>(splitA, splitB);
  }

  /**
   * Returns an HRegionLocationList extracted from the result.
   * @return an HRegionLocationList containing all locations for the region range or null if
   *  we can't deserialize the result.
   */
  @Nullable
  public static RegionLocations getRegionLocations(final Result r) {
    if (r == null) return null;
    RegionInfo regionInfo = getRegionInfo(r, getRegionInfoColumn());
    if (regionInfo == null) return null;

    List<HRegionLocation> locations = new ArrayList<>(1);
    NavigableMap<byte[],NavigableMap<byte[],byte[]>> familyMap = r.getNoVersionMap();

    locations.add(getRegionLocation(r, regionInfo, 0));

    NavigableMap<byte[], byte[]> infoMap = familyMap.get(getCatalogFamily());
    if (infoMap == null) return new RegionLocations(locations);

    // iterate until all serverName columns are seen
    int replicaId = 0;
    byte[] serverColumn = getServerColumn(replicaId);
    SortedMap<byte[], byte[]> serverMap;
    serverMap = infoMap.tailMap(serverColumn, false);

    if (serverMap.isEmpty()) return new RegionLocations(locations);

    for (Map.Entry<byte[], byte[]> entry : serverMap.entrySet()) {
      replicaId = parseReplicaIdFromServerColumn(entry.getKey());
      if (replicaId < 0) {
        break;
      }
      HRegionLocation location = getRegionLocation(r, regionInfo, replicaId);
      // In case the region replica is newly created, it's location might be null. We usually do not
      // have HRL's in RegionLocations object with null ServerName. They are handled as null HRLs.
      if (location.getServerName() == null) {
        locations.add(null);
      } else {
        locations.add(location);
      }
    }

    return new RegionLocations(locations);
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
    ServerName serverName = getServerName(r, replicaId);
    long seqNum = getSeqNumDuringOpen(r, replicaId);
    RegionInfo replicaInfo = RegionReplicaUtil.getRegionInfoForReplica(regionInfo, replicaId);
    return new HRegionLocation(replicaInfo, serverName, seqNum);
  }

  /**
   * Returns RegionInfo object from the column
   * HConstants.CATALOG_FAMILY:HConstants.REGIONINFO_QUALIFIER of the catalog
   * table Result.
   * @param data a Result object from the catalog table scan
   * @return RegionInfo or null
   */
  public static RegionInfo getRegionInfo(Result data) {
    return getRegionInfo(data, HConstants.REGIONINFO_QUALIFIER);
  }

  /**
   * Returns the RegionInfo object from the column {@link HConstants#CATALOG_FAMILY} and
   * <code>qualifier</code> of the catalog table result.
   * @param r a Result object from the catalog table scan
   * @param qualifier Column family qualifier
   * @return An RegionInfo instance or null.
   */
  @Nullable
  public static RegionInfo getRegionInfo(final Result r, byte [] qualifier) {
    Cell cell = r.getColumnLatestCell(getCatalogFamily(), qualifier);
    if (cell == null) return null;
    return RegionInfo.parseFromOrNull(cell.getValueArray(),
      cell.getValueOffset(), cell.getValueLength());
  }

  /**
   * Fetch table state for given table from META table
   * @param conn connection to use
   * @param tableName table to fetch state for
   */
  @Nullable
  public static TableState getTableState(Connection conn, TableName tableName)
      throws IOException {
    if (tableName.equals(TableName.META_TABLE_NAME)) {
      return new TableState(tableName, TableState.State.ENABLED);
    }
    Table metaHTable = getMetaHTable(conn);
    Get get = new Get(tableName.getName()).addColumn(getTableFamily(), getTableStateColumn());
    Result result = metaHTable.get(get);
    return getTableState(result);
  }

  /**
   * Fetch table states from META table
   * @param conn connection to use
   * @return map {tableName -&gt; state}
   */
  public static Map<TableName, TableState> getTableStates(Connection conn)
      throws IOException {
    final Map<TableName, TableState> states = new LinkedHashMap<>();
    Visitor collector = r -> {
      TableState state = getTableState(r);
      if (state != null) {
        states.put(state.getTableName(), state);
      }
      return true;
    };
    fullScanTables(conn, collector);
    return states;
  }

  /**
   * Updates state in META
   * Do not use. For internal use only.
   * @param conn connection to use
   * @param tableName table to look for
   */
  public static void updateTableState(Connection conn, TableName tableName,
      TableState.State actual) throws IOException {
    updateTableState(conn, new TableState(tableName, actual));
  }

  /**
   * Decode table state from META Result.
   * Should contain cell from HConstants.TABLE_FAMILY
   * @return null if not found
   */
  @Nullable
  public static TableState getTableState(Result r) throws IOException {
    Cell cell = r.getColumnLatestCell(getTableFamily(), getTableStateColumn());
    if (cell == null) {
      return null;
    }
    try {
      return TableState.parseFrom(TableName.valueOf(r.getRow()),
        Arrays.copyOfRange(cell.getValueArray(), cell.getValueOffset(),
          cell.getValueOffset() + cell.getValueLength()));
    } catch (DeserializationException e) {
      throw new IOException(e);
    }
  }

  /**
   * Implementations 'visit' a catalog table row.
   */
  public interface Visitor {
    /**
     * Visit the catalog table row.
     * @param r A row from catalog table
     * @return True if we are to proceed scanning the table, else false if
     * we are to stop now.
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
  static abstract class CollectingVisitor<T> implements Visitor {
    final List<T> results = new ArrayList<>();
    @Override
    public boolean visit(Result r) throws IOException {
      if (r != null && !r.isEmpty()) {
        add(r);
      }
      return true;
    }

    abstract void add(Result r);

    /**
     * @return Collected results; wait till visits complete to collect all
     * possible results
     */
    List<T> getResults() {
      return this.results;
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

  /**
   * A Visitor that skips offline regions and split parents
   */
  public static abstract class DefaultVisitorBase implements Visitor {

    DefaultVisitorBase() {
      super();
    }

    public abstract boolean visitInternal(Result rowResult) throws IOException;

    @Override
    public boolean visit(Result rowResult) throws IOException {
      RegionInfo info = getRegionInfo(rowResult);
      if (info == null) {
        return true;
      }

      //skip over offline and split regions
      if (!(info.isOffline() || info.isSplit())) {
        return visitInternal(rowResult);
      }
      return true;
    }
  }

  /**
   * A Visitor for a table. Provides a consistent view of the table's
   * hbase:meta entries during concurrent splits (see HBASE-5986 for details). This class
   * does not guarantee ordered traversal of meta entries, and can block until the
   * hbase:meta entries for daughters are available during splits.
   */
  public static abstract class TableVisitorBase extends DefaultVisitorBase {
    private TableName tableName;

    public TableVisitorBase(TableName tableName) {
      super();
      this.tableName = tableName;
    }

    @Override
    public final boolean visit(Result rowResult) throws IOException {
      RegionInfo info = getRegionInfo(rowResult);
      if (info == null) {
        return true;
      }
      if (!(info.getTable().equals(tableName))) {
        return false;
      }
      return super.visit(rowResult);
    }
  }

  ////////////////////////
  // Editing operations //
  ////////////////////////
  /**
   * Generates and returns a Put containing the region into for the catalog table
   */
  public static Put makePutFromRegionInfo(RegionInfo regionInfo, long ts) throws IOException {
    return addRegionInfo(new Put(regionInfo.getRegionName(), ts), regionInfo);
  }

  /**
   * Generates and returns a Delete containing the region info for the catalog table
   */
  public static Delete makeDeleteFromRegionInfo(RegionInfo regionInfo, long ts) {
    if (regionInfo == null) {
      throw new IllegalArgumentException("Can't make a delete for null region");
    }
    Delete delete = new Delete(regionInfo.getRegionName());
    delete.addFamily(getCatalogFamily(), ts);
    return delete;
  }

  /**
   * Adds split daughters to the Put
   */
  private static Put addDaughtersToPut(Put put, RegionInfo splitA, RegionInfo splitB)
      throws IOException {
    if (splitA != null) {
      put.add(CellBuilderFactory.create(CellBuilderType.SHALLOW_COPY)
                .setRow(put.getRow())
                .setFamily(HConstants.CATALOG_FAMILY)
                .setQualifier(HConstants.SPLITA_QUALIFIER)
                .setTimestamp(put.getTimestamp())
                .setType(Type.Put)
                .setValue(RegionInfo.toByteArray(splitA))
                .build());
    }
    if (splitB != null) {
      put.add(CellBuilderFactory.create(CellBuilderType.SHALLOW_COPY)
                .setRow(put.getRow())
                .setFamily(HConstants.CATALOG_FAMILY)
                .setQualifier(HConstants.SPLITB_QUALIFIER)
                .setTimestamp(put.getTimestamp())
                .setType(Type.Put)
                .setValue(RegionInfo.toByteArray(splitB))
                .build());
    }
    return put;
  }

  /**
   * Put the passed <code>p</code> to the <code>hbase:meta</code> table.
   * @param connection connection we're using
   * @param p Put to add to hbase:meta
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
   * @param ps Put to add to hbase:meta
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
   * @param d Delete to add to hbase:meta
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
   * @param deletes Deletes to add to hbase:meta  This list should support #remove.
   */
  private static void deleteFromMetaTable(final Connection connection, final List<Delete> deletes)
      throws IOException {
    try (Table t = getMetaHTable(connection)) {
      debugLogMutations(deletes);
      t.delete(deletes);
    }
  }

  private static Put addRegionStateToPut(Put put, RegionState.State state) throws IOException {
    put.add(CellBuilderFactory.create(CellBuilderType.SHALLOW_COPY)
        .setRow(put.getRow())
        .setFamily(HConstants.CATALOG_FAMILY)
        .setQualifier(getRegionStateColumn())
        .setTimestamp(put.getTimestamp())
        .setType(Cell.Type.Put)
        .setValue(Bytes.toBytes(state.name()))
        .build());
    return put;
  }

  /**
   * Update state column in hbase:meta.
   */
  public static void updateRegionState(Connection connection, RegionInfo ri,
      RegionState.State state) throws IOException {
    Put put = new Put(RegionReplicaUtil.getRegionInfoForDefaultReplica(ri).getRegionName());
    MetaTableAccessor.putsToMetaTable(connection,
        Collections.singletonList(addRegionStateToPut(put, state)));
  }

  /**
   * Adds daughter region infos to hbase:meta row for the specified region. Note that this does not
   * add its daughter's as different rows, but adds information about the daughters in the same row
   * as the parent. Use
   * {@link #splitRegion(Connection, RegionInfo, long, RegionInfo, RegionInfo, ServerName, int)}
   * if you want to do that.
   * @param connection connection we're using
   * @param regionInfo RegionInfo of parent region
   * @param splitA first split daughter of the parent regionInfo
   * @param splitB second split daughter of the parent regionInfo
   * @throws IOException if problem connecting or updating meta
   */
  public static void addSplitsToParent(Connection connection, RegionInfo regionInfo,
      RegionInfo splitA, RegionInfo splitB) throws IOException {
    try (Table meta = getMetaHTable(connection)) {
      Put put = makePutFromRegionInfo(regionInfo, EnvironmentEdgeManager.currentTime());
      addDaughtersToPut(put, splitA, splitB);
      meta.put(put);
      debugLogMutation(put);
      LOG.debug("Added region {}", regionInfo.getRegionNameAsString());
    }
  }

  /**
   * Adds a (single) hbase:meta row for the specified new region and its daughters. Note that this
   * does not add its daughter's as different rows, but adds information about the daughters
   * in the same row as the parent. Use
   * {@link #splitRegion(Connection, RegionInfo, long, RegionInfo, RegionInfo, ServerName, int)}
   * if you want to do that.
   * @param connection connection we're using
   * @param regionInfo region information
   * @throws IOException if problem connecting or updating meta
   */
  public static void addRegionToMeta(Connection connection, RegionInfo regionInfo)
      throws IOException {
    addRegionsToMeta(connection, Collections.singletonList(regionInfo), 1);
  }

  /**
   * Adds a hbase:meta row for each of the specified new regions. Initial state for new regions
   * is CLOSED.
   * @param connection connection we're using
   * @param regionInfos region information list
   * @throws IOException if problem connecting or updating meta
   */
  public static void addRegionsToMeta(Connection connection, List<RegionInfo> regionInfos,
      int regionReplication) throws IOException {
    addRegionsToMeta(connection, regionInfos, regionReplication,
      EnvironmentEdgeManager.currentTime());
  }

  /**
   * Adds a hbase:meta row for each of the specified new regions. Initial state for new regions
   * is CLOSED.
   * @param connection connection we're using
   * @param regionInfos region information list
   * @param ts desired timestamp
   * @throws IOException if problem connecting or updating meta
   */
  private static void addRegionsToMeta(Connection connection, List<RegionInfo> regionInfos,
      int regionReplication, long ts) throws IOException {
    List<Put> puts = new ArrayList<>();
    for (RegionInfo regionInfo : regionInfos) {
      if (RegionReplicaUtil.isDefaultReplica(regionInfo)) {
        Put put = makePutFromRegionInfo(regionInfo, ts);
        // New regions are added with initial state of CLOSED.
        addRegionStateToPut(put, RegionState.State.CLOSED);
        // Add empty locations for region replicas so that number of replicas can be cached
        // whenever the primary region is looked up from meta
        for (int i = 1; i < regionReplication; i++) {
          addEmptyLocation(put, i);
        }
        puts.add(put);
      }
    }
    putsToMetaTable(connection, puts);
    LOG.info("Added {} regions to meta.", puts.size());
  }

  static Put addMergeRegions(Put put, Collection<RegionInfo> mergeRegions) throws IOException {
    int limit = 10000; // Arbitrary limit. No room in our formatted 'task0000' below for more.
    int max = mergeRegions.size();
    if (max > limit) {
      // Should never happen!!!!! But just in case.
      throw new RuntimeException("Can't merge " + max + " regions in one go; " + limit +
          " is upper-limit.");
    }
    int counter = 0;
    for (RegionInfo ri: mergeRegions) {
      String qualifier = String.format(HConstants.MERGE_QUALIFIER_PREFIX_STR + "%04d", counter++);
      put.add(CellBuilderFactory.create(CellBuilderType.SHALLOW_COPY).
          setRow(put.getRow()).
          setFamily(HConstants.CATALOG_FAMILY).
          setQualifier(Bytes.toBytes(qualifier)).
          setTimestamp(put.getTimestamp()).
          setType(Type.Put).
          setValue(RegionInfo.toByteArray(ri)).
          build());
    }
    return put;
  }

  /**
   * Merge regions into one in an atomic operation. Deletes the merging regions in
   * hbase:meta and adds the merged region.
   * @param connection connection we're using
   * @param mergedRegion the merged region
   * @param parentSeqNum Parent regions to merge and their next open sequence id used
   *   by serial replication. Set to -1 if not needed by this table.
   * @param sn the location of the region
   */
  public static void mergeRegions(Connection connection, RegionInfo mergedRegion,
        Map<RegionInfo, Long> parentSeqNum, ServerName sn, int regionReplication)
      throws IOException {
    try (Table meta = getMetaHTable(connection)) {
      long time = HConstants.LATEST_TIMESTAMP;
      List<Mutation> mutations = new ArrayList<>();
      List<RegionInfo> replicationParents = new ArrayList<>();
      for (Map.Entry<RegionInfo, Long> e: parentSeqNum.entrySet()) {
        RegionInfo ri = e.getKey();
        long seqNum = e.getValue();
        // Deletes for merging regions
        mutations.add(makeDeleteFromRegionInfo(ri, time));
        if (seqNum > 0) {
          mutations.add(makePutForReplicationBarrier(ri, seqNum, time));
          replicationParents.add(ri);
        }
      }
      // Put for parent
      Put putOfMerged = makePutFromRegionInfo(mergedRegion, time);
      putOfMerged = addMergeRegions(putOfMerged, parentSeqNum.keySet());
      // Set initial state to CLOSED.
      // NOTE: If initial state is not set to CLOSED then merged region gets added with the
      // default OFFLINE state. If Master gets restarted after this step, start up sequence of
      // master tries to assign this offline region. This is followed by re-assignments of the
      // merged region from resumed {@link MergeTableRegionsProcedure}
      addRegionStateToPut(putOfMerged, RegionState.State.CLOSED);
      mutations.add(putOfMerged);
      // The merged is a new region, openSeqNum = 1 is fine. ServerName may be null
      // if crash after merge happened but before we got to here.. means in-memory
      // locations of offlined merged, now-closed, regions is lost. Should be ok. We
      // assign the merged region later.
      if (sn != null) {
        addLocation(putOfMerged, sn, 1, mergedRegion.getReplicaId());
      }

      // Add empty locations for region replicas of the merged region so that number of replicas
      // can be cached whenever the primary region is looked up from meta
      for (int i = 1; i < regionReplication; i++) {
        addEmptyLocation(putOfMerged, i);
      }
      // add parent reference for serial replication
      if (!replicationParents.isEmpty()) {
        addReplicationParent(putOfMerged, replicationParents);
      }
      byte[] tableRow = Bytes.toBytes(mergedRegion.getRegionNameAsString() + HConstants.DELIMITER);
      multiMutate(meta, tableRow, mutations);
    }
  }

  /**
   * Splits the region into two in an atomic operation. Offlines the parent region with the
   * information that it is split into two, and also adds the daughter regions. Does not add the
   * location information to the daughter regions since they are not open yet.
   * @param connection connection we're using
   * @param parent the parent region which is split
   * @param parentOpenSeqNum the next open sequence id for parent region, used by serial
   *          replication. -1 if not necessary.
   * @param splitA Split daughter region A
   * @param splitB Split daughter region B
   * @param sn the location of the region
   */
  public static void splitRegion(Connection connection, RegionInfo parent, long parentOpenSeqNum,
      RegionInfo splitA, RegionInfo splitB, ServerName sn, int regionReplication)
      throws IOException {
    try (Table meta = getMetaHTable(connection)) {
      long time = EnvironmentEdgeManager.currentTime();
      // Put for parent
      Put putParent = makePutFromRegionInfo(RegionInfoBuilder.newBuilder(parent)
                        .setOffline(true)
                        .setSplit(true).build(), time);
      addDaughtersToPut(putParent, splitA, splitB);

      // Puts for daughters
      Put putA = makePutFromRegionInfo(splitA, time);
      Put putB = makePutFromRegionInfo(splitB, time);
      if (parentOpenSeqNum > 0) {
        addReplicationBarrier(putParent, parentOpenSeqNum);
        addReplicationParent(putA, Collections.singletonList(parent));
        addReplicationParent(putB, Collections.singletonList(parent));
      }
      // Set initial state to CLOSED
      // NOTE: If initial state is not set to CLOSED then daughter regions get added with the
      // default OFFLINE state. If Master gets restarted after this step, start up sequence of
      // master tries to assign these offline regions. This is followed by re-assignments of the
      // daughter regions from resumed {@link SplitTableRegionProcedure}
      addRegionStateToPut(putA, RegionState.State.CLOSED);
      addRegionStateToPut(putB, RegionState.State.CLOSED);

      addSequenceNum(putA, 1, splitA.getReplicaId()); // new regions, openSeqNum = 1 is fine.
      addSequenceNum(putB, 1, splitB.getReplicaId());

      // Add empty locations for region replicas of daughters so that number of replicas can be
      // cached whenever the primary region is looked up from meta
      for (int i = 1; i < regionReplication; i++) {
        addEmptyLocation(putA, i);
        addEmptyLocation(putB, i);
      }

      byte[] tableRow = Bytes.toBytes(parent.getRegionNameAsString() + HConstants.DELIMITER);
      multiMutate(meta, tableRow, putParent, putA, putB);
    }
  }

  /**
   * Update state of the table in meta.
   * @param connection what we use for update
   * @param state new state
   */
  private static void updateTableState(Connection connection, TableState state) throws IOException {
    Put put = makePutFromTableState(state, EnvironmentEdgeManager.currentTime());
    putToMetaTable(connection, put);
    LOG.info("Updated {} in hbase:meta", state);
  }

  /**
   * Construct PUT for given state
   * @param state new state
   */
  public static Put makePutFromTableState(TableState state, long ts) {
    Put put = new Put(state.getTableName().getName(), ts);
    put.addColumn(getTableFamily(), getTableStateColumn(), state.convert().toByteArray());
    return put;
  }

  /**
   * Remove state for table from meta
   * @param connection to use for deletion
   * @param table to delete state for
   */
  public static void deleteTableState(Connection connection, TableName table)
      throws IOException {
    long time = EnvironmentEdgeManager.currentTime();
    Delete delete = new Delete(table.getName());
    delete.addColumns(getTableFamily(), getTableStateColumn(), time);
    deleteFromMetaTable(connection, delete);
    LOG.info("Deleted table " + table + " state from META");
  }

  private static void multiMutate(Table table, byte[] row,
      Mutation... mutations) throws IOException {
    multiMutate(table, row, Arrays.asList(mutations));
  }

  /**
   * Performs an atomic multi-mutate operation against the given table. Used by the likes of
   * merge and split as these want to make atomic mutations across multiple rows.
   * @throws IOException even if we encounter a RuntimeException, we'll still wrap it in an IOE.
   */
  static void multiMutate(final Table table, byte[] row, final List<Mutation> mutations)
      throws IOException {
    debugLogMutations(mutations);
    Batch.Call<MultiRowMutationService, MutateRowsResponse> callable = instance -> {
      MutateRowsRequest.Builder builder = MutateRowsRequest.newBuilder();
      for (Mutation mutation : mutations) {
        if (mutation instanceof Put) {
          builder.addMutationRequest(
            ProtobufUtil.toMutation(ClientProtos.MutationProto.MutationType.PUT, mutation));
        } else if (mutation instanceof Delete) {
          builder.addMutationRequest(
            ProtobufUtil.toMutation(ClientProtos.MutationProto.MutationType.DELETE, mutation));
        } else {
          throw new DoNotRetryIOException(
            "multi in MetaEditor doesn't support " + mutation.getClass().getName());
        }
      }
      ServerRpcController controller = new ServerRpcController();
      CoprocessorRpcUtils.BlockingRpcCallback<MutateRowsResponse> rpcCallback =
        new CoprocessorRpcUtils.BlockingRpcCallback<>();
      instance.mutateRows(controller, builder.build(), rpcCallback);
      MutateRowsResponse resp = rpcCallback.get();
      if (controller.failedOnException()) {
        throw controller.getFailedOn();
      }
      return resp;
    };
    try {
      table.coprocessorService(MultiRowMutationService.class, row, row, callable);
    } catch (Throwable e) {
      // Throw if an IOE else wrap in an IOE EVEN IF IT IS a RuntimeException (e.g.
      // a RejectedExecutionException because the hosting exception is shutting down.
      // This is old behavior worth reexamining. Procedures doing merge or split
      // currently don't handle RuntimeExceptions coming up out of meta table edits.
      // Would have to work on this at least. See HBASE-23904.
      Throwables.throwIfInstanceOf(e, IOException.class);
      throw new IOException(e);
    }
  }

  /**
   * Updates the location of the specified region in hbase:meta to be the specified server hostname
   * and startcode.
   * <p>
   * Uses passed catalog tracker to get a connection to the server hosting hbase:meta and makes
   * edits to that region.
   * @param connection connection we're using
   * @param regionInfo region to update location of
   * @param openSeqNum the latest sequence number obtained when the region was open
   * @param sn Server name
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
   * @param connection connection we're using
   * @param regionInfo region to update location of
   * @param sn Server name
   * @param openSeqNum the latest sequence number obtained when the region was open
   * @param masterSystemTime wall clock time from master if passed in the open region RPC
   * @throws IOException In particular could throw {@link java.net.ConnectException} if the server
   *           is down on other end.
   */
  private static void updateLocation(Connection connection, RegionInfo regionInfo, ServerName sn,
      long openSeqNum, long masterSystemTime) throws IOException {
    // region replicas are kept in the primary region's row
    Put put = new Put(getMetaKeyForRegion(regionInfo), masterSystemTime);
    addRegionInfo(put, regionInfo);
    addLocation(put, sn, openSeqNum, regionInfo.getReplicaId());
    putToMetaTable(connection, put);
    LOG.info("Updated row {} with server=", regionInfo.getRegionNameAsString(), sn);
  }

  /**
   * Deletes the specified region from META.
   * @param connection connection we're using
   * @param regionInfo region to be deleted from META
   */
  public static void deleteRegionInfo(Connection connection, RegionInfo regionInfo)
      throws IOException {
    Delete delete = new Delete(regionInfo.getRegionName());
    delete.addFamily(getCatalogFamily(), HConstants.LATEST_TIMESTAMP);
    deleteFromMetaTable(connection, delete);
    LOG.info("Deleted " + regionInfo.getRegionNameAsString());
  }

  /**
   * Deletes the specified regions from META.
   * @param connection connection we're using
   * @param regionsInfo list of regions to be deleted from META
   */
  public static void deleteRegionInfos(Connection connection, List<RegionInfo> regionsInfo)
      throws IOException {
    deleteRegionInfos(connection, regionsInfo, EnvironmentEdgeManager.currentTime());
  }

  /**
   * Deletes the specified regions from META.
   * @param connection connection we're using
   * @param regionsInfo list of regions to be deleted from META
   */
  private static void deleteRegionInfos(Connection connection, List<RegionInfo> regionsInfo,
        long ts)
      throws IOException {
    List<Delete> deletes = new ArrayList<>(regionsInfo.size());
    for (RegionInfo hri : regionsInfo) {
      Delete e = new Delete(hri.getRegionName());
      e.addFamily(getCatalogFamily(), ts);
      deletes.add(e);
    }
    deleteFromMetaTable(connection, deletes);
    LOG.info("Deleted {} regions from META", regionsInfo.size());
    LOG.debug("Deleted regions: {}", regionsInfo);
  }

  /**
   * Overwrites the specified regions from hbase:meta. Deletes old rows for the given regions and
   * adds new ones. Regions added back have state CLOSED.
   * @param connection connection we're using
   * @param regionInfos list of regions to be added to META
   */
  public static void overwriteRegions(Connection connection, List<RegionInfo> regionInfos,
      int regionReplication) throws IOException {
    // use master time for delete marker and the Put
    long now = EnvironmentEdgeManager.currentTime();
    deleteRegionInfos(connection, regionInfos, now);
    // Why sleep? This is the easiest way to ensure that the previous deletes does not
    // eclipse the following puts, that might happen in the same ts from the server.
    // See HBASE-9906, and HBASE-9879. Once either HBASE-9879, HBASE-8770 is fixed,
    // or HBASE-9905 is fixed and meta uses seqIds, we do not need the sleep.
    //
    // HBASE-13875 uses master timestamp for the mutations. The 20ms sleep is not needed
    addRegionsToMeta(connection, regionInfos, regionReplication, now + 1);
    LOG.info("Overwritten " + regionInfos.size() + " regions to Meta");
    LOG.debug("Overwritten regions: {} ", regionInfos);
  }

  /**
   * Deletes merge qualifiers for the specified merge region.
   * @param connection connection we're using
   * @param mergeRegion the merged region
   */
  public static void deleteMergeQualifiers(Connection connection, final RegionInfo mergeRegion)
      throws IOException {
    Delete delete = new Delete(mergeRegion.getRegionName());
    // NOTE: We are doing a new hbase:meta read here.
    Cell[] cells = getRegionResult(connection, mergeRegion.getRegionName()).rawCells();
    if (cells == null || cells.length == 0) {
      return;
    }
    List<byte[]> qualifiers = new ArrayList<>();
    for (Cell cell : cells) {
      if (!isMergeQualifierPrefix(cell)) {
        continue;
      }
      byte[] qualifier = CellUtil.cloneQualifier(cell);
      qualifiers.add(qualifier);
      delete.addColumns(getCatalogFamily(), qualifier, HConstants.LATEST_TIMESTAMP);
    }

    // There will be race condition that a GCMultipleMergedRegionsProcedure is scheduled while
    // the previous GCMultipleMergedRegionsProcedure is still going on, in this case, the second
    // GCMultipleMergedRegionsProcedure could delete the merged region by accident!
    if (qualifiers.isEmpty()) {
      LOG.info("No merged qualifiers for region " + mergeRegion.getRegionNameAsString() +
        " in meta table, they are cleaned up already, Skip.");
      return;
    }

    deleteFromMetaTable(connection, delete);
    LOG.info("Deleted merge references in " + mergeRegion.getRegionNameAsString() +
        ", deleted qualifiers " + qualifiers.stream().map(Bytes::toStringBinary).
        collect(Collectors.joining(", ")));
  }

  public static Put addRegionInfo(final Put p, final RegionInfo hri)
    throws IOException {
    p.add(CellBuilderFactory.create(CellBuilderType.SHALLOW_COPY)
        .setRow(p.getRow())
        .setFamily(getCatalogFamily())
        .setQualifier(HConstants.REGIONINFO_QUALIFIER)
        .setTimestamp(p.getTimestamp())
        .setType(Type.Put)
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
    return p.add(builder.clear()
              .setRow(p.getRow())
              .setFamily(getCatalogFamily())
              .setQualifier(getServerColumn(replicaId))
              .setTimestamp(p.getTimestamp())
              .setType(Cell.Type.Put)
              .setValue(Bytes.toBytes(sn.getAddress().toString()))
              .build())
            .add(builder.clear()
              .setRow(p.getRow())
              .setFamily(getCatalogFamily())
              .setQualifier(getStartCodeColumn(replicaId))
              .setTimestamp(p.getTimestamp())
              .setType(Cell.Type.Put)
              .setValue(Bytes.toBytes(sn.getStartcode()))
              .build())
            .add(builder.clear()
              .setRow(p.getRow())
              .setFamily(getCatalogFamily())
              .setQualifier(getSeqNumColumn(replicaId))
              .setTimestamp(p.getTimestamp())
              .setType(Type.Put)
              .setValue(Bytes.toBytes(openSeqNum))
              .build());
  }

  private static void writeRegionName(ByteArrayOutputStream out, byte[] regionName) {
    for (byte b : regionName) {
      if (b == ESCAPE_BYTE) {
        out.write(ESCAPE_BYTE);
      }
      out.write(b);
    }
  }

  public static byte[] getParentsBytes(List<RegionInfo> parents) {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    Iterator<RegionInfo> iter = parents.iterator();
    writeRegionName(bos, iter.next().getRegionName());
    while (iter.hasNext()) {
      bos.write(ESCAPE_BYTE);
      bos.write(SEPARATED_BYTE);
      writeRegionName(bos, iter.next().getRegionName());
    }
    return bos.toByteArray();
  }

  private static List<byte[]> parseParentsBytes(byte[] bytes) {
    List<byte[]> parents = new ArrayList<>();
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    for (int i = 0; i < bytes.length; i++) {
      if (bytes[i] == ESCAPE_BYTE) {
        i++;
        if (bytes[i] == SEPARATED_BYTE) {
          parents.add(bos.toByteArray());
          bos.reset();
          continue;
        }
        // fall through to append the byte
      }
      bos.write(bytes[i]);
    }
    if (bos.size() > 0) {
      parents.add(bos.toByteArray());
    }
    return parents;
  }

  private static void addReplicationParent(Put put, List<RegionInfo> parents) throws IOException {
    byte[] value = getParentsBytes(parents);
    put.add(CellBuilderFactory.create(CellBuilderType.SHALLOW_COPY).setRow(put.getRow())
      .setFamily(HConstants.REPLICATION_BARRIER_FAMILY).setQualifier(REPLICATION_PARENT_QUALIFIER)
      .setTimestamp(put.getTimestamp()).setType(Type.Put).setValue(value).build());
  }

  public static Put makePutForReplicationBarrier(RegionInfo regionInfo, long openSeqNum, long ts)
      throws IOException {
    Put put = new Put(regionInfo.getRegionName(), ts);
    addReplicationBarrier(put, openSeqNum);
    return put;
  }

  /**
   * See class comment on SerialReplicationChecker
   */
  public static void addReplicationBarrier(Put put, long openSeqNum) throws IOException {
    put.add(CellBuilderFactory.create(CellBuilderType.SHALLOW_COPY)
      .setRow(put.getRow())
      .setFamily(HConstants.REPLICATION_BARRIER_FAMILY)
      .setQualifier(HConstants.SEQNUM_QUALIFIER)
      .setTimestamp(put.getTimestamp())
      .setType(Type.Put)
      .setValue(Bytes.toBytes(openSeqNum))
      .build());
  }

  public static Put addEmptyLocation(Put p, int replicaId) throws IOException {
    CellBuilder builder = CellBuilderFactory.create(CellBuilderType.SHALLOW_COPY);
    return p.add(builder.clear()
                .setRow(p.getRow())
                .setFamily(getCatalogFamily())
                .setQualifier(getServerColumn(replicaId))
                .setTimestamp(p.getTimestamp())
                .setType(Type.Put)
                .build())
            .add(builder.clear()
                .setRow(p.getRow())
                .setFamily(getCatalogFamily())
                .setQualifier(getStartCodeColumn(replicaId))
                .setTimestamp(p.getTimestamp())
                .setType(Cell.Type.Put)
                .build())
            .add(builder.clear()
                .setRow(p.getRow())
                .setFamily(getCatalogFamily())
                .setQualifier(getSeqNumColumn(replicaId))
                .setTimestamp(p.getTimestamp())
                .setType(Cell.Type.Put)
                .build());
  }

  public static final class ReplicationBarrierResult {
    private final long[] barriers;
    private final RegionState.State state;
    private final List<byte[]> parentRegionNames;

    ReplicationBarrierResult(long[] barriers, State state, List<byte[]> parentRegionNames) {
      this.barriers = barriers;
      this.state = state;
      this.parentRegionNames = parentRegionNames;
    }

    public long[] getBarriers() {
      return barriers;
    }

    public RegionState.State getState() {
      return state;
    }

    public List<byte[]> getParentRegionNames() {
      return parentRegionNames;
    }

    @Override
    public String toString() {
      return "ReplicationBarrierResult [barriers=" + Arrays.toString(barriers) + ", state=" +
        state + ", parentRegionNames=" +
        parentRegionNames.stream().map(Bytes::toStringBinary).collect(Collectors.joining(", ")) +
        "]";
    }
  }

  private static long getReplicationBarrier(Cell c) {
    return Bytes.toLong(c.getValueArray(), c.getValueOffset(), c.getValueLength());
  }

  public static long[] getReplicationBarriers(Result result) {
    return result.getColumnCells(HConstants.REPLICATION_BARRIER_FAMILY, HConstants.SEQNUM_QUALIFIER)
      .stream().mapToLong(MetaTableAccessor::getReplicationBarrier).sorted().distinct().toArray();
  }

  private static ReplicationBarrierResult getReplicationBarrierResult(Result result) {
    long[] barriers = getReplicationBarriers(result);
    byte[] stateBytes = result.getValue(getCatalogFamily(), getRegionStateColumn());
    RegionState.State state =
      stateBytes != null ? RegionState.State.valueOf(Bytes.toString(stateBytes)) : null;
    byte[] parentRegionsBytes =
      result.getValue(HConstants.REPLICATION_BARRIER_FAMILY, REPLICATION_PARENT_QUALIFIER);
    List<byte[]> parentRegionNames =
      parentRegionsBytes != null ? parseParentsBytes(parentRegionsBytes) : Collections.emptyList();
    return new ReplicationBarrierResult(barriers, state, parentRegionNames);
  }

  public static ReplicationBarrierResult getReplicationBarrierResult(Connection conn,
      TableName tableName, byte[] row, byte[] encodedRegionName) throws IOException {
    byte[] metaStartKey = RegionInfo.createRegionName(tableName, row, HConstants.NINES, false);
    byte[] metaStopKey =
      RegionInfo.createRegionName(tableName, HConstants.EMPTY_START_ROW, "", false);
    Scan scan = new Scan().withStartRow(metaStartKey).withStopRow(metaStopKey)
      .addColumn(getCatalogFamily(), getRegionStateColumn())
      .addFamily(HConstants.REPLICATION_BARRIER_FAMILY).readAllVersions().setReversed(true)
      .setCaching(10);
    try (Table table = getMetaHTable(conn); ResultScanner scanner = table.getScanner(scan)) {
      for (Result result;;) {
        result = scanner.next();
        if (result == null) {
          return new ReplicationBarrierResult(new long[0], null, Collections.emptyList());
        }
        byte[] regionName = result.getRow();
        // TODO: we may look up a region which has already been split or merged so we need to check
        // whether the encoded name matches. Need to find a way to quit earlier when there is no
        // record for the given region, for now it will scan to the end of the table.
        if (!Bytes.equals(encodedRegionName,
          Bytes.toBytes(RegionInfo.encodeRegionName(regionName)))) {
          continue;
        }
        return getReplicationBarrierResult(result);
      }
    }
  }

  public static long[] getReplicationBarrier(Connection conn, byte[] regionName)
      throws IOException {
    try (Table table = getMetaHTable(conn)) {
      Result result = table.get(new Get(regionName)
        .addColumn(HConstants.REPLICATION_BARRIER_FAMILY, HConstants.SEQNUM_QUALIFIER)
        .readAllVersions());
      return getReplicationBarriers(result);
    }
  }

  public static List<Pair<String, Long>> getTableEncodedRegionNameAndLastBarrier(Connection conn,
      TableName tableName) throws IOException {
    List<Pair<String, Long>> list = new ArrayList<>();
    scanMeta(conn, getTableStartRowForMeta(tableName, QueryType.REPLICATION),
      getTableStopRowForMeta(tableName, QueryType.REPLICATION), QueryType.REPLICATION, r -> {
        byte[] value =
          r.getValue(HConstants.REPLICATION_BARRIER_FAMILY, HConstants.SEQNUM_QUALIFIER);
        if (value == null) {
          return true;
        }
        long lastBarrier = Bytes.toLong(value);
        String encodedRegionName = RegionInfo.encodeRegionName(r.getRow());
        list.add(Pair.newPair(encodedRegionName, lastBarrier));
        return true;
      });
    return list;
  }

  public static List<String> getTableEncodedRegionNamesForSerialReplication(Connection conn,
      TableName tableName) throws IOException {
    List<String> list = new ArrayList<>();
    scanMeta(conn, getTableStartRowForMeta(tableName, QueryType.REPLICATION),
      getTableStopRowForMeta(tableName, QueryType.REPLICATION), QueryType.REPLICATION,
      new FirstKeyOnlyFilter(), Integer.MAX_VALUE, r -> {
        list.add(RegionInfo.encodeRegionName(r.getRow()));
        return true;
      });
    return list;
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

  private static Put addSequenceNum(Put p, long openSeqNum, int replicaId) throws IOException {
    return p.add(CellBuilderFactory.create(CellBuilderType.SHALLOW_COPY)
              .setRow(p.getRow())
              .setFamily(HConstants.CATALOG_FAMILY)
              .setQualifier(getSeqNumColumn(replicaId))
              .setTimestamp(p.getTimestamp())
              .setType(Type.Put)
              .setValue(Bytes.toBytes(openSeqNum))
              .build());
  }
}
