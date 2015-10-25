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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ServiceException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Consistency;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableState;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.protobuf.generated.MultiRowMutationProtos;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.ExceptionUtil;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.PairOfSameType;

/**
 * Read/write operations on region and assignment information store in
 * <code>hbase:meta</code>.
 *
 * Some of the methods of this class take ZooKeeperWatcher as a param. The only reason
 * for this is because when used on client-side (like from HBaseAdmin), we want to use
 * short-living connection (opened before each operation, closed right after), while
 * when used on HM or HRS (like in AssignmentManager) we want permanent connection.
 */
@InterfaceAudience.Private
public class MetaTableAccessor {

  /*
   * HBASE-10070 adds a replicaId to HRI, meaning more than one HRI can be defined for the
   * same table range (table, startKey, endKey). For every range, there will be at least one
   * HRI defined which is called default replica.
   *
   * Meta layout (as of 0.98 + HBASE-10070) is like:
   *
   * For each table there is single row in column family 'table' formatted:
   * <tableName> including namespace and columns are:
   * table: state             => contains table state
   *
   * For each table range, there is a single row, formatted like:
   * <tableName>,<startKey>,<regionId>,<encodedRegionName>. This row corresponds to the regionName
   * of the default region replica.
   * Columns are:
   * info:regioninfo         => contains serialized HRI for the default region replica
   * info:server             => contains hostname:port (in string form) for the server hosting
   *                            the default regionInfo replica
   * info:server_<replicaId> => contains hostname:port (in string form) for the server hosting the
   *                            regionInfo replica with replicaId
   * info:serverstartcode    => contains server start code (in binary long form) for the server
   *                            hosting the default regionInfo replica
   * info:serverstartcode_<replicaId> => contains server start code (in binary long form) for the
   *                                     server hosting the regionInfo replica with replicaId
   * info:seqnumDuringOpen    => contains seqNum (in binary long form) for the region at the time
   *                             the server opened the region with default replicaId
   * info:seqnumDuringOpen_<replicaId> => contains seqNum (in binary long form) for the region at
   *                             the time the server opened the region with replicaId
   * info:splitA              => contains a serialized HRI for the first daughter region if the
   *                             region is split
   * info:splitB              => contains a serialized HRI for the second daughter region if the
   *                             region is split
   * info:mergeA              => contains a serialized HRI for the first parent region if the
   *                             region is the result of a merge
   * info:mergeB              => contains a serialized HRI for the second parent region if the
   *                             region is the result of a merge
   *
   * The actual layout of meta should be encapsulated inside MetaTableAccessor methods,
   * and should not leak out of it (through Result objects, etc)
   */

  private static final Log LOG = LogFactory.getLog(MetaTableAccessor.class);
  private static final Log METALOG = LogFactory.getLog("org.apache.hadoop.hbase.META");

  static final byte [] META_REGION_PREFIX;
  static {
    // Copy the prefix from FIRST_META_REGIONINFO into META_REGION_PREFIX.
    // FIRST_META_REGIONINFO == 'hbase:meta,,1'.  META_REGION_PREFIX == 'hbase:meta,'
    int len = HRegionInfo.FIRST_META_REGIONINFO.getRegionName().length - 2;
    META_REGION_PREFIX = new byte [len];
    System.arraycopy(HRegionInfo.FIRST_META_REGIONINFO.getRegionName(), 0,
      META_REGION_PREFIX, 0, len);
  }

  /**
   * Lists all of the table regions currently in META.
   * Deprecated, keep there until some test use this.
   * @param connection what we will use
   * @param tableName table to list
   * @return Map of all user-space regions to servers
   * @throws java.io.IOException
   * @deprecated use {@link #getTableRegionsAndLocations}, region can have multiple locations
   */
  @Deprecated
  public static NavigableMap<HRegionInfo, ServerName> allTableRegions(
      Connection connection, final TableName tableName) throws IOException {
    final NavigableMap<HRegionInfo, ServerName> regions =
      new TreeMap<HRegionInfo, ServerName>();
    Visitor visitor = new TableVisitorBase(tableName) {
      @Override
      public boolean visitInternal(Result result) throws IOException {
        RegionLocations locations = getRegionLocations(result);
        if (locations == null) return true;
        for (HRegionLocation loc : locations.getRegionLocations()) {
          if (loc != null) {
            HRegionInfo regionInfo = loc.getRegionInfo();
            regions.put(regionInfo, loc.getServerName());
          }
        }
        return true;
      }
    };
    scanMetaForTableRegions(connection, visitor, tableName);
    return regions;
  }

  @InterfaceAudience.Private
  public enum QueryType {
    ALL(HConstants.TABLE_FAMILY, HConstants.CATALOG_FAMILY),
    REGION(HConstants.CATALOG_FAMILY),
    TABLE(HConstants.TABLE_FAMILY);

    private final byte[][] families;

    QueryType(byte[]... families) {
      this.families = families;
    }

    byte[][] getFamilies() {
      return this.families;
    }
  }

  /** The delimiter for meta columns for replicaIds &gt; 0 */
  protected static final char META_REPLICA_ID_DELIMITER = '_';

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
   * @throws IOException
   */
  public static void fullScanRegions(Connection connection,
      final Visitor visitor)
      throws IOException {
    scanMeta(connection, null, null, QueryType.REGION, visitor);
  }

  /**
   * Performs a full scan of <code>hbase:meta</code> for regions.
   * @param connection connection we're using
   * @throws IOException
   */
  public static List<Result> fullScanRegions(Connection connection)
      throws IOException {
    return fullScan(connection, QueryType.REGION);
  }

  /**
   * Performs a full scan of <code>hbase:meta</code> for tables.
   * @param connection connection we're using
   * @param visitor Visitor invoked against each row in tables family.
   * @throws IOException
   */
  public static void fullScanTables(Connection connection,
      final Visitor visitor)
      throws IOException {
    scanMeta(connection, null, null, QueryType.TABLE, visitor);
  }

  /**
   * Performs a full scan of <code>hbase:meta</code>.
   * @param connection connection we're using
   * @param type scanned part of meta
   * @return List of {@link Result}
   * @throws IOException
   */
  public static List<Result> fullScan(Connection connection, QueryType type)
    throws IOException {
    CollectAllVisitor v = new CollectAllVisitor();
    scanMeta(connection, null, null, type, v);
    return v.getResults();
  }

  /**
   * Callers should call close on the returned {@link Table} instance.
   * @param connection connection we're using to access Meta
   * @return An {@link Table} for <code>hbase:meta</code>
   * @throws IOException
   */
  static Table getMetaHTable(final Connection connection)
  throws IOException {
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
   * @throws IOException
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
   * @return Location and HRegionInfo for <code>regionName</code>
   * @throws IOException
   * @deprecated use {@link #getRegionLocation(Connection, byte[])} instead
   */
  @Deprecated
  public static Pair<HRegionInfo, ServerName> getRegion(Connection connection, byte [] regionName)
    throws IOException {
    HRegionLocation location = getRegionLocation(connection, regionName);
    return location == null
      ? null
      : new Pair<HRegionInfo, ServerName>(location.getRegionInfo(), location.getServerName());
  }

  /**
   * Returns the HRegionLocation from meta for the given region
   * @param connection connection we're using
   * @param regionName region we're looking for
   * @return HRegionLocation for the given region
   * @throws IOException
   */
  public static HRegionLocation getRegionLocation(Connection connection,
                                                  byte[] regionName) throws IOException {
    byte[] row = regionName;
    HRegionInfo parsedInfo = null;
    try {
      parsedInfo = parseRegionInfoFromRegionName(regionName);
      row = getMetaKeyForRegion(parsedInfo);
    } catch (Exception parseEx) {
      // Ignore. This is used with tableName passed as regionName.
    }
    Get get = new Get(row);
    get.addFamily(HConstants.CATALOG_FAMILY);
    Result r = get(getMetaHTable(connection), get);
    RegionLocations locations = getRegionLocations(r);
    return locations == null
      ? null
      : locations.getRegionLocation(parsedInfo == null ? 0 : parsedInfo.getReplicaId());
  }

  /**
   * Returns the HRegionLocation from meta for the given region
   * @param connection connection we're using
   * @param regionInfo region information
   * @return HRegionLocation for the given region
   * @throws IOException
   */
  public static HRegionLocation getRegionLocation(Connection connection,
                                                  HRegionInfo regionInfo) throws IOException {
    byte[] row = getMetaKeyForRegion(regionInfo);
    Get get = new Get(row);
    get.addFamily(HConstants.CATALOG_FAMILY);
    Result r = get(getMetaHTable(connection), get);
    return getRegionLocation(r, regionInfo, regionInfo.getReplicaId());
  }

  /** Returns the row key to use for this regionInfo */
  public static byte[] getMetaKeyForRegion(HRegionInfo regionInfo) {
    return RegionReplicaUtil.getRegionInfoForDefaultReplica(regionInfo).getRegionName();
  }

  /** Returns an HRI parsed from this regionName. Not all the fields of the HRI
   * is stored in the name, so the returned object should only be used for the fields
   * in the regionName.
   */
  protected static HRegionInfo parseRegionInfoFromRegionName(byte[] regionName)
    throws IOException {
    byte[][] fields = HRegionInfo.parseRegionName(regionName);
    long regionId =  Long.parseLong(Bytes.toString(fields[2]));
    int replicaId = fields.length > 3 ? Integer.parseInt(Bytes.toString(fields[3]), 16) : 0;
    return new HRegionInfo(
      TableName.valueOf(fields[0]), fields[1], fields[1], false, regionId, replicaId);
  }

  /**
   * Gets the result in hbase:meta for the specified region.
   * @param connection connection we're using
   * @param regionName region we're looking for
   * @return result of the specified region
   * @throws IOException
   */
  public static Result getRegionResult(Connection connection,
      byte[] regionName) throws IOException {
    Get get = new Get(regionName);
    get.addFamily(HConstants.CATALOG_FAMILY);
    return get(getMetaHTable(connection), get);
  }

  /**
   * Get regions from the merge qualifier of the specified merged region
   * @return null if it doesn't contain merge qualifier, else two merge regions
   * @throws IOException
   */
  @Nullable
  public static Pair<HRegionInfo, HRegionInfo> getRegionsFromMergeQualifier(
      Connection connection, byte[] regionName) throws IOException {
    Result result = getRegionResult(connection, regionName);
    HRegionInfo mergeA = getHRegionInfo(result, HConstants.MERGEA_QUALIFIER);
    HRegionInfo mergeB = getHRegionInfo(result, HConstants.MERGEB_QUALIFIER);
    if (mergeA == null && mergeB == null) {
      return null;
    }
    return new Pair<HRegionInfo, HRegionInfo>(mergeA, mergeB);
 }

  /**
   * Checks if the specified table exists.  Looks at the hbase:meta table hosted on
   * the specified server.
   * @param connection connection we're using
   * @param tableName table to check
   * @return true if the table exists in meta, false if not
   * @throws IOException
   */
  public static boolean tableExists(Connection connection,
      final TableName tableName)
  throws IOException {
    // Catalog tables always exist.
    return tableName.equals(TableName.META_TABLE_NAME)
        || getTableState(connection, tableName) != null;
  }

  /**
   * Lists all of the regions currently in META.
   *
   * @param connection to connect with
   * @param excludeOfflinedSplitParents False if we are to include offlined/splitparents regions,
   *                                    true and we'll leave out offlined regions from returned list
   * @return List of all user-space regions.
   * @throws IOException
   */
  @VisibleForTesting
  public static List<HRegionInfo> getAllRegions(Connection connection,
      boolean excludeOfflinedSplitParents)
      throws IOException {
    List<Pair<HRegionInfo, ServerName>> result;

    result = getTableRegionsAndLocations(connection, null,
        excludeOfflinedSplitParents);

    return getListOfHRegionInfos(result);

  }

  /**
   * Gets all of the regions of the specified table. Do not use this method
   * to get meta table regions, use methods in MetaTableLocator instead.
   * @param connection connection we're using
   * @param tableName table we're looking for
   * @return Ordered list of {@link HRegionInfo}.
   * @throws IOException
   */
  public static List<HRegionInfo> getTableRegions(Connection connection, TableName tableName)
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
   * @return Ordered list of {@link HRegionInfo}.
   * @throws IOException
   */
  public static List<HRegionInfo> getTableRegions(Connection connection,
      TableName tableName, final boolean excludeOfflinedSplitParents)
      throws IOException {
    List<Pair<HRegionInfo, ServerName>> result;

    result = getTableRegionsAndLocations(connection, tableName,
      excludeOfflinedSplitParents);

    return getListOfHRegionInfos(result);
  }

  @Nullable
  static List<HRegionInfo> getListOfHRegionInfos(final List<Pair<HRegionInfo, ServerName>> pairs) {
    if (pairs == null || pairs.isEmpty()) return null;
    List<HRegionInfo> result = new ArrayList<HRegionInfo>(pairs.size());
    for (Pair<HRegionInfo, ServerName> pair: pairs) {
      result.add(pair.getFirst());
    }
    return result;
  }

  /**
   * @param current region of current table we're working with
   * @param tableName table we're checking against
   * @return True if <code>current</code> tablename is equal to
   * <code>tableName</code>
   */
  static boolean isInsideTable(final HRegionInfo current, final TableName tableName) {
    return tableName.equals(current.getTable());
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
  @Deprecated
  public static Scan getScanForTableName(Connection connection, TableName tableName) {
    // Start key is just the table name with delimiters
    byte[] startKey = getTableStartRowForMeta(tableName, QueryType.REGION);
    // Stop key appends the smallest possible char to the table name
    byte[] stopKey = getTableStopRowForMeta(tableName, QueryType.REGION);

    Scan scan = getMetaScan(connection);
    scan.setStartRow(startKey);
    scan.setStopRow(stopKey);
    return scan;
  }

  private static Scan getMetaScan(Connection connection) {
    return getMetaScan(connection, Integer.MAX_VALUE);
  }

  private static Scan getMetaScan(Connection connection, int rowUpperLimit) {
    Scan scan = new Scan();
    int scannerCaching = connection.getConfiguration()
        .getInt(HConstants.HBASE_META_SCANNER_CACHING,
            HConstants.DEFAULT_HBASE_META_SCANNER_CACHING);
    if (connection.getConfiguration().getBoolean(HConstants.USE_META_REPLICAS,
        HConstants.DEFAULT_USE_META_REPLICAS)) {
      scan.setConsistency(Consistency.TIMELINE);
    }
    if (rowUpperLimit <= scannerCaching) {
      scan.setSmall(true);
    }
    int rows = Math.min(rowUpperLimit, scannerCaching);
    scan.setCaching(rows);
    return scan;
  }
  /**
   * Do not use this method to get meta table regions, use methods in MetaTableLocator instead.
   * @param connection connection we're using
   * @param tableName table we're looking for
   * @return Return list of regioninfos and server.
   * @throws IOException
   */
  public static List<Pair<HRegionInfo, ServerName>>
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
   * @throws IOException
   */
  public static List<Pair<HRegionInfo, ServerName>> getTableRegionsAndLocations(
      Connection connection, @Nullable final TableName tableName,
      final boolean excludeOfflinedSplitParents) throws IOException {
    if (tableName != null && tableName.equals(TableName.META_TABLE_NAME)) {
      throw new IOException("This method can't be used to locate meta regions;"
        + " use MetaTableLocator instead");
    }
    // Make a version of CollectingVisitor that collects HRegionInfo and ServerAddress
    CollectingVisitor<Pair<HRegionInfo, ServerName>> visitor =
      new CollectingVisitor<Pair<HRegionInfo, ServerName>>() {
        private RegionLocations current = null;

        @Override
        public boolean visit(Result r) throws IOException {
          current = getRegionLocations(r);
          if (current == null || current.getRegionLocation().getRegionInfo() == null) {
            LOG.warn("No serialized HRegionInfo in " + r);
            return true;
          }
          HRegionInfo hri = current.getRegionLocation().getRegionInfo();
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
              this.results.add(new Pair<HRegionInfo, ServerName>(
                loc.getRegionInfo(), loc.getServerName()));
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
  public static NavigableMap<HRegionInfo, Result>
  getServerUserRegions(Connection connection, final ServerName serverName)
    throws IOException {
    final NavigableMap<HRegionInfo, Result> hris = new TreeMap<HRegionInfo, Result>();
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
              hris.put(loc.getRegionInfo(), r);
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
    Visitor v = new Visitor() {
      @Override
      public boolean visit(Result r) throws IOException {
        if (r ==  null || r.isEmpty()) return true;
        LOG.info("fullScanMetaAndPrint.Current Meta Row: " + r);
        TableState state = getTableState(r);
        if (state != null) {
          LOG.info("Table State: " + state);
        } else {
          RegionLocations locations = getRegionLocations(r);
          if (locations == null) return true;
          for (HRegionLocation loc : locations.getRegionLocations()) {
            if (loc != null) {
              LOG.info("fullScanMetaAndPrint.HRI Print= " + loc.getRegionInfo());
            }
          }
        }
        return true;
      }
    };
    scanMeta(connection, null, null, QueryType.ALL, v);
  }

  public static void scanMetaForTableRegions(Connection connection,
      Visitor visitor, TableName tableName) throws IOException {
    scanMeta(connection, tableName, QueryType.REGION, Integer.MAX_VALUE, visitor);
  }

  public static void scanMeta(Connection connection, TableName table,
      QueryType type, int maxRows, final Visitor visitor) throws IOException {
    scanMeta(connection, getTableStartRowForMeta(table, type), getTableStopRowForMeta(table, type),
        type, maxRows, visitor);
  }

  public static void scanMeta(Connection connection,
      @Nullable final byte[] startRow, @Nullable final byte[] stopRow,
      QueryType type, final Visitor visitor) throws IOException {
    scanMeta(connection, startRow, stopRow, type, Integer.MAX_VALUE, visitor);
  }

  /**
   * Performs a scan of META table for given table starting from
   * given row.
   *
   * @param connection connection we're using
   * @param visitor    visitor to call
   * @param tableName  table withing we scan
   * @param row        start scan from this row
   * @param rowLimit   max number of rows to return
   * @throws IOException
   */
  public static void scanMeta(Connection connection,
      final Visitor visitor, final TableName tableName,
      final byte[] row, final int rowLimit)
      throws IOException {

    byte[] startRow = null;
    byte[] stopRow = null;
    if (tableName != null) {
      startRow =
          getTableStartRowForMeta(tableName, QueryType.REGION);
      if (row != null) {
        HRegionInfo closestRi =
            getClosestRegionInfo(connection, tableName, row);
        startRow = HRegionInfo
            .createRegionName(tableName, closestRi.getStartKey(), HConstants.ZEROES, false);
      }
      stopRow =
          getTableStopRowForMeta(tableName, QueryType.REGION);
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
   * @throws IOException
   */
  public static void scanMeta(Connection connection,
      @Nullable final byte[] startRow, @Nullable final byte[] stopRow,
      QueryType type, int maxRows, final Visitor visitor)
  throws IOException {
    int rowUpperLimit = maxRows > 0 ? maxRows : Integer.MAX_VALUE;
    Scan scan = getMetaScan(connection, rowUpperLimit);

    for (byte[] family : type.getFamilies()) {
      scan.addFamily(family);
    }
    if (startRow != null) scan.setStartRow(startRow);
    if (stopRow != null) scan.setStopRow(stopRow);

    if (LOG.isTraceEnabled()) {
      LOG.trace("Scanning META"
          + " starting at row=" + Bytes.toStringBinary(startRow)
          + " stopping at row=" + Bytes.toStringBinary(stopRow)
          + " for max=" + rowUpperLimit
          + " with caching=" + scan.getCaching());
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
    if (visitor != null && visitor instanceof Closeable) {
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
   * @throws java.io.IOException
   */
  @Nonnull
  public static HRegionInfo getClosestRegionInfo(Connection connection,
      @Nonnull final TableName tableName,
      @Nonnull final byte[] row)
      throws IOException {
    byte[] searchRow = HRegionInfo.createRegionName(tableName, row, HConstants.NINES, false);
    Scan scan = getMetaScan(connection, 1);
    scan.setReversed(true);
    scan.setStartRow(searchRow);
    try (ResultScanner resultScanner = getMetaHTable(connection).getScanner(scan)) {
      Result result = resultScanner.next();
      if (result == null) {
        throw new TableNotFoundException("Cannot find row in META " +
            " for table: " + tableName + ", row=" + Bytes.toStringBinary(row));
      }
      HRegionInfo regionInfo = getHRegionInfo(result);
      if (regionInfo == null) {
        throw new IOException("HRegionInfo was null or empty in Meta for " +
            tableName + ", row=" + Bytes.toStringBinary(row));
      }
      return regionInfo;
    }
  }

  /**
   * Returns the column family used for meta columns.
   * @return HConstants.CATALOG_FAMILY.
   */
  protected static byte[] getCatalogFamily() {
    return HConstants.CATALOG_FAMILY;
  }

  /**
   * Returns the column family used for table columns.
   * @return HConstants.TABLE_FAMILY.
   */
  protected static byte[] getTableFamily() {
    return HConstants.TABLE_FAMILY;
  }

  /**
   * Returns the column qualifier for serialized region info
   * @return HConstants.REGIONINFO_QUALIFIER
   */
  protected static byte[] getRegionInfoColumn() {
    return HConstants.REGIONINFO_QUALIFIER;
  }

  /**
   * Returns the column qualifier for serialized table state
   *
   * @return HConstants.TABLE_STATE_QUALIFIER
   */
  protected static byte[] getStateColumn() {
    return HConstants.TABLE_STATE_QUALIFIER;
  }

  /**
   * Returns the column qualifier for server column for replicaId
   * @param replicaId the replicaId of the region
   * @return a byte[] for server column qualifier
   */
  @VisibleForTesting
  public static byte[] getServerColumn(int replicaId) {
    return replicaId == 0
      ? HConstants.SERVER_QUALIFIER
      : Bytes.toBytes(HConstants.SERVER_QUALIFIER_STR + META_REPLICA_ID_DELIMITER
      + String.format(HRegionInfo.REPLICA_ID_FORMAT, replicaId));
  }

  /**
   * Returns the column qualifier for server start code column for replicaId
   * @param replicaId the replicaId of the region
   * @return a byte[] for server start code column qualifier
   */
  @VisibleForTesting
  public static byte[] getStartCodeColumn(int replicaId) {
    return replicaId == 0
      ? HConstants.STARTCODE_QUALIFIER
      : Bytes.toBytes(HConstants.STARTCODE_QUALIFIER_STR + META_REPLICA_ID_DELIMITER
      + String.format(HRegionInfo.REPLICA_ID_FORMAT, replicaId));
  }

  /**
   * Returns the column qualifier for seqNum column for replicaId
   * @param replicaId the replicaId of the region
   * @return a byte[] for seqNum column qualifier
   */
  @VisibleForTesting
  public static byte[] getSeqNumColumn(int replicaId) {
    return replicaId == 0
      ? HConstants.SEQNUM_QUALIFIER
      : Bytes.toBytes(HConstants.SEQNUM_QUALIFIER_STR + META_REPLICA_ID_DELIMITER
      + String.format(HRegionInfo.REPLICA_ID_FORMAT, replicaId));
  }

  /**
   * Parses the replicaId from the server column qualifier. See top of the class javadoc
   * for the actual meta layout
   * @param serverColumn the column qualifier
   * @return an int for the replicaId
   */
  @VisibleForTesting
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
  private static ServerName getServerName(final Result r, final int replicaId) {
    byte[] serverColumn = getServerColumn(replicaId);
    Cell cell = r.getColumnLatestCell(getCatalogFamily(), serverColumn);
    if (cell == null || cell.getValueLength() == 0) return null;
    String hostAndPort = Bytes.toString(
      cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
    byte[] startcodeColumn = getStartCodeColumn(replicaId);
    cell = r.getColumnLatestCell(getCatalogFamily(), startcodeColumn);
    if (cell == null || cell.getValueLength() == 0) return null;
    return ServerName.valueOf(hostAndPort,
      Bytes.toLong(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
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
   * Returns an HRegionLocationList extracted from the result.
   * @return an HRegionLocationList containing all locations for the region range or null if
   *  we can't deserialize the result.
   */
  @Nullable
  public static RegionLocations getRegionLocations(final Result r) {
    if (r == null) return null;
    HRegionInfo regionInfo = getHRegionInfo(r, getRegionInfoColumn());
    if (regionInfo == null) return null;

    List<HRegionLocation> locations = new ArrayList<HRegionLocation>(1);
    NavigableMap<byte[],NavigableMap<byte[],byte[]>> familyMap = r.getNoVersionMap();

    locations.add(getRegionLocation(r, regionInfo, 0));

    NavigableMap<byte[], byte[]> infoMap = familyMap.get(getCatalogFamily());
    if (infoMap == null) return new RegionLocations(locations);

    // iterate until all serverName columns are seen
    int replicaId = 0;
    byte[] serverColumn = getServerColumn(replicaId);
    SortedMap<byte[], byte[]> serverMap = infoMap.tailMap(serverColumn, false);
    if (serverMap.isEmpty()) return new RegionLocations(locations);

    for (Map.Entry<byte[], byte[]> entry : serverMap.entrySet()) {
      replicaId = parseReplicaIdFromServerColumn(entry.getKey());
      if (replicaId < 0) {
        break;
      }
      HRegionLocation location = getRegionLocation(r, regionInfo, replicaId);
      // In case the region replica is newly created, it's location might be null. We usually do not
      // have HRL's in RegionLocations object with null ServerName. They are handled as null HRLs.
      if (location == null || location.getServerName() == null) {
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
  private static HRegionLocation getRegionLocation(final Result r, final HRegionInfo regionInfo,
                                                   final int replicaId) {
    ServerName serverName = getServerName(r, replicaId);
    long seqNum = getSeqNumDuringOpen(r, replicaId);
    HRegionInfo replicaInfo = RegionReplicaUtil.getRegionInfoForReplica(regionInfo, replicaId);
    return new HRegionLocation(replicaInfo, serverName, seqNum);
  }

  /**
   * Returns HRegionInfo object from the column
   * HConstants.CATALOG_FAMILY:HConstants.REGIONINFO_QUALIFIER of the catalog
   * table Result.
   * @param data a Result object from the catalog table scan
   * @return HRegionInfo or null
   */
  public static HRegionInfo getHRegionInfo(Result data) {
    return getHRegionInfo(data, HConstants.REGIONINFO_QUALIFIER);
  }

  /**
   * Returns the HRegionInfo object from the column {@link HConstants#CATALOG_FAMILY} and
   * <code>qualifier</code> of the catalog table result.
   * @param r a Result object from the catalog table scan
   * @param qualifier Column family qualifier
   * @return An HRegionInfo instance or null.
   */
  @Nullable
  private static HRegionInfo getHRegionInfo(final Result r, byte [] qualifier) {
    Cell cell = r.getColumnLatestCell(getCatalogFamily(), qualifier);
    if (cell == null) return null;
    return HRegionInfo.parseFromOrNull(cell.getValueArray(),
      cell.getValueOffset(), cell.getValueLength());
  }

  /**
   * Returns the daughter regions by reading the corresponding columns of the catalog table
   * Result.
   * @param data a Result object from the catalog table scan
   * @return a pair of HRegionInfo or PairOfSameType(null, null) if the region is not a split
   * parent
   */
  public static PairOfSameType<HRegionInfo> getDaughterRegions(Result data) {
    HRegionInfo splitA = getHRegionInfo(data, HConstants.SPLITA_QUALIFIER);
    HRegionInfo splitB = getHRegionInfo(data, HConstants.SPLITB_QUALIFIER);

    return new PairOfSameType<HRegionInfo>(splitA, splitB);
  }

  /**
   * Returns the merge regions by reading the corresponding columns of the catalog table
   * Result.
   * @param data a Result object from the catalog table scan
   * @return a pair of HRegionInfo or PairOfSameType(null, null) if the region is not a split
   * parent
   */
  public static PairOfSameType<HRegionInfo> getMergeRegions(Result data) {
    HRegionInfo mergeA = getHRegionInfo(data, HConstants.MERGEA_QUALIFIER);
    HRegionInfo mergeB = getHRegionInfo(data, HConstants.MERGEB_QUALIFIER);

    return new PairOfSameType<HRegionInfo>(mergeA, mergeB);
  }

  /**
   * Fetch table state for given table from META table
   * @param conn connection to use
   * @param tableName table to fetch state for
   * @return state
   * @throws IOException
   */
  @Nullable
  public static TableState getTableState(Connection conn, TableName tableName)
      throws IOException {
    Table metaHTable = getMetaHTable(conn);
    Get get = new Get(tableName.getName()).addColumn(getTableFamily(), getStateColumn());
    long time = EnvironmentEdgeManager.currentTime();
    get.setTimeRange(0, time);
    Result result =
        metaHTable.get(get);
    return getTableState(result);
  }

  /**
   * Fetch table states from META table
   * @param conn connection to use
   * @return map {tableName -&gt; state}
   * @throws IOException
   */
  public static Map<TableName, TableState> getTableStates(Connection conn)
      throws IOException {
    final Map<TableName, TableState> states = new LinkedHashMap<>();
    Visitor collector = new Visitor() {
      @Override
      public boolean visit(Result r) throws IOException {
        TableState state = getTableState(r);
        if (state != null)
          states.put(state.getTableName(), state);
        return true;
      }
    };
    fullScanTables(conn, collector);
    return states;
  }

  /**
   * Updates state in META
   * @param conn connection to use
   * @param tableName table to look for
   * @throws IOException
   */
  public static void updateTableState(Connection conn, TableName tableName,
      TableState.State actual) throws IOException {
    updateTableState(conn, new TableState(tableName, actual));
  }

  /**
   * Decode table state from META Result.
   * Should contain cell from HConstants.TABLE_FAMILY
   * @param r result
   * @return null if not found
   * @throws IOException
   */
  @Nullable
  public static TableState getTableState(Result r)
      throws IOException {
    Cell cell = r.getColumnLatestCell(getTableFamily(), getStateColumn());
    if (cell == null) return null;
    try {
      return TableState.parseFrom(TableName.valueOf(r.getRow()),
          Arrays.copyOfRange(cell.getValueArray(),
          cell.getValueOffset(), cell.getValueOffset() + cell.getValueLength()));
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
    final List<T> results = new ArrayList<T>();
    @Override
    public boolean visit(Result r) throws IOException {
      if (r ==  null || r.isEmpty()) return true;
      add(r);
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

    public DefaultVisitorBase() {
      super();
    }

    public abstract boolean visitInternal(Result rowResult) throws IOException;

    @Override
    public boolean visit(Result rowResult) throws IOException {
      HRegionInfo info = getHRegionInfo(rowResult);
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
      HRegionInfo info = getHRegionInfo(rowResult);
      if (info == null) {
        return true;
      }
      if (!(info.getTable().equals(tableName))) {
        return false;
      }
      return super.visit(rowResult);
    }
  }

  /**
   * Count regions in <code>hbase:meta</code> for passed table.
   * @param c Configuration object
   * @param tableName table name to count regions for
   * @return Count or regions in table <code>tableName</code>
   * @throws IOException
   */
  @Deprecated
  public static int getRegionCount(final Configuration c, final String tableName)
      throws IOException {
    return getRegionCount(c, TableName.valueOf(tableName));
  }

  /**
   * Count regions in <code>hbase:meta</code> for passed table.
   * @param c Configuration object
   * @param tableName table name to count regions for
   * @return Count or regions in table <code>tableName</code>
   * @throws IOException
   */
  public static int getRegionCount(final Configuration c, final TableName tableName)
  throws IOException {
    try (Connection connection = ConnectionFactory.createConnection(c)) {
      return getRegionCount(connection, tableName);
    }
  }

  /**
   * Count regions in <code>hbase:meta</code> for passed table.
   * @param connection Connection object
   * @param tableName table name to count regions for
   * @return Count or regions in table <code>tableName</code>
   * @throws IOException
   */
  public static int getRegionCount(final Connection connection, final TableName tableName)
  throws IOException {
    try (RegionLocator locator = connection.getRegionLocator(tableName)) {
      List<HRegionLocation> locations = locator.getAllRegionLocations();
      return locations == null? 0: locations.size();
    }
  }

  ////////////////////////
  // Editing operations //
  ////////////////////////

  /**
   * Generates and returns a Put containing the region into for the catalog table
   */
  public static Put makePutFromRegionInfo(HRegionInfo regionInfo)
    throws IOException {
    return makePutFromRegionInfo(regionInfo, EnvironmentEdgeManager.currentTime());
  }

  /**
   * Generates and returns a Put containing the region into for the catalog table
   */
  public static Put makePutFromRegionInfo(HRegionInfo regionInfo, long ts)
    throws IOException {
    Put put = new Put(regionInfo.getRegionName(), ts);
    addRegionInfo(put, regionInfo);
    return put;
  }

  /**
   * Generates and returns a Delete containing the region info for the catalog
   * table
   */
  public static Delete makeDeleteFromRegionInfo(HRegionInfo regionInfo) {
    long now = EnvironmentEdgeManager.currentTime();
    return makeDeleteFromRegionInfo(regionInfo, now);
  }

  /**
   * Generates and returns a Delete containing the region info for the catalog
   * table
   */
  public static Delete makeDeleteFromRegionInfo(HRegionInfo regionInfo, long ts) {
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
  public static Put addDaughtersToPut(Put put, HRegionInfo splitA, HRegionInfo splitB) {
    if (splitA != null) {
      put.addImmutable(
        HConstants.CATALOG_FAMILY, HConstants.SPLITA_QUALIFIER, splitA.toByteArray());
    }
    if (splitB != null) {
      put.addImmutable(
        HConstants.CATALOG_FAMILY, HConstants.SPLITB_QUALIFIER, splitB.toByteArray());
    }
    return put;
  }

  /**
   * Put the passed <code>p</code> to the <code>hbase:meta</code> table.
   * @param connection connection we're using
   * @param p Put to add to hbase:meta
   * @throws IOException
   */
  static void putToMetaTable(final Connection connection, final Put p)
    throws IOException {
    put(getMetaHTable(connection), p);
  }

  /**
   * @param t Table to use (will be closed when done).
   * @param p put to make
   * @throws IOException
   */
  private static void put(final Table t, final Put p) throws IOException {
    try {
      if (METALOG.isDebugEnabled()) {
        METALOG.debug(mutationToString(p));
      }
      t.put(p);
    } finally {
      t.close();
    }
  }

  /**
   * Put the passed <code>ps</code> to the <code>hbase:meta</code> table.
   * @param connection connection we're using
   * @param ps Put to add to hbase:meta
   * @throws IOException
   */
  public static void putsToMetaTable(final Connection connection, final List<Put> ps)
    throws IOException {
    Table t = getMetaHTable(connection);
    try {
      if (METALOG.isDebugEnabled()) {
        METALOG.debug(mutationsToString(ps));
      }
      t.put(ps);
    } finally {
      t.close();
    }
  }

  /**
   * Delete the passed <code>d</code> from the <code>hbase:meta</code> table.
   * @param connection connection we're using
   * @param d Delete to add to hbase:meta
   * @throws IOException
   */
  static void deleteFromMetaTable(final Connection connection, final Delete d)
    throws IOException {
    List<Delete> dels = new ArrayList<Delete>(1);
    dels.add(d);
    deleteFromMetaTable(connection, dels);
  }

  /**
   * Delete the passed <code>deletes</code> from the <code>hbase:meta</code> table.
   * @param connection connection we're using
   * @param deletes Deletes to add to hbase:meta  This list should support #remove.
   * @throws IOException
   */
  public static void deleteFromMetaTable(final Connection connection, final List<Delete> deletes)
    throws IOException {
    Table t = getMetaHTable(connection);
    try {
      if (METALOG.isDebugEnabled()) {
        METALOG.debug(mutationsToString(deletes));
      }
      t.delete(deletes);
    } finally {
      t.close();
    }
  }

  /**
   * Deletes some replica columns corresponding to replicas for the passed rows
   * @param metaRows rows in hbase:meta
   * @param replicaIndexToDeleteFrom the replica ID we would start deleting from
   * @param numReplicasToRemove how many replicas to remove
   * @param connection connection we're using to access meta table
   * @throws IOException
   */
  public static void removeRegionReplicasFromMeta(Set<byte[]> metaRows,
    int replicaIndexToDeleteFrom, int numReplicasToRemove, Connection connection)
      throws IOException {
    int absoluteIndex = replicaIndexToDeleteFrom + numReplicasToRemove;
    for (byte[] row : metaRows) {
      long now = EnvironmentEdgeManager.currentTime();
      Delete deleteReplicaLocations = new Delete(row);
      for (int i = replicaIndexToDeleteFrom; i < absoluteIndex; i++) {
        deleteReplicaLocations.addColumns(getCatalogFamily(),
          getServerColumn(i), now);
        deleteReplicaLocations.addColumns(getCatalogFamily(),
          getSeqNumColumn(i), now);
        deleteReplicaLocations.addColumns(getCatalogFamily(),
          getStartCodeColumn(i), now);
      }
      deleteFromMetaTable(connection, deleteReplicaLocations);
    }
  }

  /**
   * Execute the passed <code>mutations</code> against <code>hbase:meta</code> table.
   * @param connection connection we're using
   * @param mutations Puts and Deletes to execute on hbase:meta
   * @throws IOException
   */
  public static void mutateMetaTable(final Connection connection,
                                     final List<Mutation> mutations)
    throws IOException {
    Table t = getMetaHTable(connection);
    try {
      if (METALOG.isDebugEnabled()) {
        METALOG.debug(mutationsToString(mutations));
      }
      t.batch(mutations, null);
    } catch (InterruptedException e) {
      InterruptedIOException ie = new InterruptedIOException(e.getMessage());
      ie.initCause(e);
      throw ie;
    } finally {
      t.close();
    }
  }

  /**
   * Adds a hbase:meta row for the specified new region.
   * @param connection connection we're using
   * @param regionInfo region information
   * @throws IOException if problem connecting or updating meta
   */
  public static void addRegionToMeta(Connection connection,
                                     HRegionInfo regionInfo)
    throws IOException {
    putToMetaTable(connection, makePutFromRegionInfo(regionInfo));
    LOG.info("Added " + regionInfo.getRegionNameAsString());
  }

  /**
   * Adds a hbase:meta row for the specified new region to the given catalog table. The
   * Table is not flushed or closed.
   * @param meta the Table for META
   * @param regionInfo region information
   * @throws IOException if problem connecting or updating meta
   */
  public static void addRegionToMeta(Table meta, HRegionInfo regionInfo) throws IOException {
    addRegionToMeta(meta, regionInfo, null, null);
  }

  /**
   * Adds a (single) hbase:meta row for the specified new region and its daughters. Note that this
   * does not add its daughter's as different rows, but adds information about the daughters
   * in the same row as the parent. Use
   * {@link #splitRegion(Connection, HRegionInfo, HRegionInfo, HRegionInfo, ServerName, int)}
   * if you want to do that.
   * @param meta the Table for META
   * @param regionInfo region information
   * @param splitA first split daughter of the parent regionInfo
   * @param splitB second split daughter of the parent regionInfo
   * @throws IOException if problem connecting or updating meta
   */
  public static void addRegionToMeta(Table meta, HRegionInfo regionInfo,
                                     HRegionInfo splitA, HRegionInfo splitB) throws IOException {
    Put put = makePutFromRegionInfo(regionInfo);
    addDaughtersToPut(put, splitA, splitB);
    meta.put(put);
    if (METALOG.isDebugEnabled()) {
      METALOG.debug(mutationToString(put));
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Added " + regionInfo.getRegionNameAsString());
    }
  }

  /**
   * Adds a (single) hbase:meta row for the specified new region and its daughters. Note that this
   * does not add its daughter's as different rows, but adds information about the daughters
   * in the same row as the parent. Use
   * {@link #splitRegion(Connection, HRegionInfo, HRegionInfo, HRegionInfo, ServerName, int)}
   * if you want to do that.
   * @param connection connection we're using
   * @param regionInfo region information
   * @param splitA first split daughter of the parent regionInfo
   * @param splitB second split daughter of the parent regionInfo
   * @throws IOException if problem connecting or updating meta
   */
  public static void addRegionToMeta(Connection connection, HRegionInfo regionInfo,
                                     HRegionInfo splitA, HRegionInfo splitB) throws IOException {
    Table meta = getMetaHTable(connection);
    try {
      addRegionToMeta(meta, regionInfo, splitA, splitB);
    } finally {
      meta.close();
    }
  }

  /**
   * Adds a hbase:meta row for each of the specified new regions.
   * @param connection connection we're using
   * @param regionInfos region information list
   * @throws IOException if problem connecting or updating meta
   */
  public static void addRegionsToMeta(Connection connection,
                                      List<HRegionInfo> regionInfos, int regionReplication)
    throws IOException {
    addRegionsToMeta(connection, regionInfos, regionReplication, HConstants.LATEST_TIMESTAMP);
  }
  /**
   * Adds a hbase:meta row for each of the specified new regions.
   * @param connection connection we're using
   * @param regionInfos region information list
   * @param regionReplication
   * @param ts desired timestamp
   * @throws IOException if problem connecting or updating meta
   */
  public static void addRegionsToMeta(Connection connection,
      List<HRegionInfo> regionInfos, int regionReplication, long ts)
          throws IOException {
    List<Put> puts = new ArrayList<Put>();
    for (HRegionInfo regionInfo : regionInfos) {
      if (RegionReplicaUtil.isDefaultReplica(regionInfo)) {
        Put put = makePutFromRegionInfo(regionInfo, ts);
        // Add empty locations for region replicas so that number of replicas can be cached
        // whenever the primary region is looked up from meta
        for (int i = 1; i < regionReplication; i++) {
          addEmptyLocation(put, i);
        }
        puts.add(put);
      }
    }
    putsToMetaTable(connection, puts);
    LOG.info("Added " + puts.size());
  }

  /**
   * Adds a daughter region entry to meta.
   * @param regionInfo the region to put
   * @param sn the location of the region
   * @param openSeqNum the latest sequence number obtained when the region was open
   */
  public static void addDaughter(final Connection connection,
      final HRegionInfo regionInfo, final ServerName sn, final long openSeqNum)
      throws NotAllMetaRegionsOnlineException, IOException {
    long now = EnvironmentEdgeManager.currentTime();
    Put put = new Put(regionInfo.getRegionName(), now);
    addRegionInfo(put, regionInfo);
    if (sn != null) {
      addLocation(put, sn, openSeqNum, -1, regionInfo.getReplicaId());
    }
    putToMetaTable(connection, put);
    LOG.info("Added daughter " + regionInfo.getEncodedName() +
      (sn == null? ", serverName=null": ", serverName=" + sn.toString()));
  }

  /**
   * Merge the two regions into one in an atomic operation. Deletes the two
   * merging regions in hbase:meta and adds the merged region with the information of
   * two merging regions.
   * @param connection connection we're using
   * @param mergedRegion the merged region
   * @param regionA
   * @param regionB
   * @param sn the location of the region
   * @param masterSystemTime
   * @throws IOException
   */
  public static void mergeRegions(final Connection connection, HRegionInfo mergedRegion,
      HRegionInfo regionA, HRegionInfo regionB, ServerName sn, int regionReplication,
      long masterSystemTime)
          throws IOException {
    Table meta = getMetaHTable(connection);
    try {
      HRegionInfo copyOfMerged = new HRegionInfo(mergedRegion);

      // use the maximum of what master passed us vs local time.
      long time = Math.max(EnvironmentEdgeManager.currentTime(), masterSystemTime);

      // Put for parent
      Put putOfMerged = makePutFromRegionInfo(copyOfMerged, time);
      putOfMerged.addImmutable(HConstants.CATALOG_FAMILY, HConstants.MERGEA_QUALIFIER,
        regionA.toByteArray());
      putOfMerged.addImmutable(HConstants.CATALOG_FAMILY, HConstants.MERGEB_QUALIFIER,
        regionB.toByteArray());

      // Deletes for merging regions
      Delete deleteA = makeDeleteFromRegionInfo(regionA, time);
      Delete deleteB = makeDeleteFromRegionInfo(regionB, time);

      // The merged is a new region, openSeqNum = 1 is fine.
      addLocation(putOfMerged, sn, 1, -1, mergedRegion.getReplicaId());

      // Add empty locations for region replicas of the merged region so that number of replicas can
      // be cached whenever the primary region is looked up from meta
      for (int i = 1; i < regionReplication; i++) {
        addEmptyLocation(putOfMerged, i);
      }

      byte[] tableRow = Bytes.toBytes(mergedRegion.getRegionNameAsString()
        + HConstants.DELIMITER);
      multiMutate(meta, tableRow, putOfMerged, deleteA, deleteB);
    } finally {
      meta.close();
    }
  }

  /**
   * Splits the region into two in an atomic operation. Offlines the parent
   * region with the information that it is split into two, and also adds
   * the daughter regions. Does not add the location information to the daughter
   * regions since they are not open yet.
   * @param connection connection we're using
   * @param parent the parent region which is split
   * @param splitA Split daughter region A
   * @param splitB Split daughter region A
   * @param sn the location of the region
   */
  public static void splitRegion(final Connection connection,
                                 HRegionInfo parent, HRegionInfo splitA, HRegionInfo splitB,
                                 ServerName sn, int regionReplication) throws IOException {
    Table meta = getMetaHTable(connection);
    try {
      HRegionInfo copyOfParent = new HRegionInfo(parent);
      copyOfParent.setOffline(true);
      copyOfParent.setSplit(true);

      //Put for parent
      Put putParent = makePutFromRegionInfo(copyOfParent);
      addDaughtersToPut(putParent, splitA, splitB);

      //Puts for daughters
      Put putA = makePutFromRegionInfo(splitA);
      Put putB = makePutFromRegionInfo(splitB);

      addLocation(putA, sn, 1, -1, splitA.getReplicaId()); //new regions, openSeqNum = 1 is fine.
      addLocation(putB, sn, 1, -1, splitB.getReplicaId());

      // Add empty locations for region replicas of daughters so that number of replicas can be
      // cached whenever the primary region is looked up from meta
      for (int i = 1; i < regionReplication; i++) {
        addEmptyLocation(putA, i);
        addEmptyLocation(putB, i);
      }

      byte[] tableRow = Bytes.toBytes(parent.getRegionNameAsString() + HConstants.DELIMITER);
      multiMutate(meta, tableRow, putParent, putA, putB);
    } finally {
      meta.close();
    }
  }

  /**
   * Update state of the table in meta.
   * @param connection what we use for update
   * @param state new state
   * @throws IOException
   */
  public static void updateTableState(Connection connection, TableState state)
      throws IOException {
    Put put = makePutFromTableState(state);
    putToMetaTable(connection, put);
    LOG.info(
        "Updated table " + state.getTableName() + " state to " + state.getState() + " in META");
  }

  /**
   * Construct PUT for given state
   * @param state new state
   */
  public static Put makePutFromTableState(TableState state) {
    long time = EnvironmentEdgeManager.currentTime();
    Put put = new Put(state.getTableName().getName(), time);
    put.addColumn(getTableFamily(), getStateColumn(), state.convert().toByteArray());
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
    delete.addColumns(getTableFamily(), getStateColumn(), time);
    deleteFromMetaTable(connection, delete);
    LOG.info("Deleted table " + table + " state from META");
  }

  /**
   * Performs an atomic multi-Mutate operation against the given table.
   */
  private static void multiMutate(Table table, byte[] row, Mutation... mutations)
      throws IOException {
    CoprocessorRpcChannel channel = table.coprocessorService(row);
    MultiRowMutationProtos.MutateRowsRequest.Builder mmrBuilder
      = MultiRowMutationProtos.MutateRowsRequest.newBuilder();
    if (METALOG.isDebugEnabled()) {
      METALOG.debug(mutationsToString(mutations));
    }
    for (Mutation mutation : mutations) {
      if (mutation instanceof Put) {
        mmrBuilder.addMutationRequest(ProtobufUtil.toMutation(
          ClientProtos.MutationProto.MutationType.PUT, mutation));
      } else if (mutation instanceof Delete) {
        mmrBuilder.addMutationRequest(ProtobufUtil.toMutation(
          ClientProtos.MutationProto.MutationType.DELETE, mutation));
      } else {
        throw new DoNotRetryIOException("multi in MetaEditor doesn't support "
          + mutation.getClass().getName());
      }
    }

    MultiRowMutationProtos.MultiRowMutationService.BlockingInterface service =
      MultiRowMutationProtos.MultiRowMutationService.newBlockingStub(channel);
    try {
      service.mutateRows(null, mmrBuilder.build());
    } catch (ServiceException ex) {
      ProtobufUtil.toIOException(ex);
    }
  }

  /**
   * Updates the location of the specified region in hbase:meta to be the specified
   * server hostname and startcode.
   * <p>
   * Uses passed catalog tracker to get a connection to the server hosting
   * hbase:meta and makes edits to that region.
   *
   * @param connection connection we're using
   * @param regionInfo region to update location of
   * @param openSeqNum the latest sequence number obtained when the region was open
   * @param sn Server name
   * @param masterSystemTime wall clock time from master if passed in the open region RPC or -1
   * @throws IOException
   */
  public static void updateRegionLocation(Connection connection,
                                          HRegionInfo regionInfo, ServerName sn, long openSeqNum,
                                          long masterSystemTime)
    throws IOException {
    updateLocation(connection, regionInfo, sn, openSeqNum, masterSystemTime);
  }

  /**
   * Updates the location of the specified region to be the specified server.
   * <p>
   * Connects to the specified server which should be hosting the specified
   * catalog region name to perform the edit.
   *
   * @param connection connection we're using
   * @param regionInfo region to update location of
   * @param sn Server name
   * @param openSeqNum the latest sequence number obtained when the region was open
   * @param masterSystemTime wall clock time from master if passed in the open region RPC or -1
   * @throws IOException In particular could throw {@link java.net.ConnectException}
   * if the server is down on other end.
   */
  private static void updateLocation(final Connection connection,
                                     HRegionInfo regionInfo, ServerName sn, long openSeqNum,
                                     long masterSystemTime)
    throws IOException {

    // use the maximum of what master passed us vs local time.
    long time = Math.max(EnvironmentEdgeManager.currentTime(), masterSystemTime);

    // region replicas are kept in the primary region's row
    Put put = new Put(getMetaKeyForRegion(regionInfo), time);
    addLocation(put, sn, openSeqNum, time, regionInfo.getReplicaId());
    putToMetaTable(connection, put);
    LOG.info("Updated row " + regionInfo.getRegionNameAsString() +
      " with server=" + sn);
  }

  /**
   * Deletes the specified region from META.
   * @param connection connection we're using
   * @param regionInfo region to be deleted from META
   * @throws IOException
   */
  public static void deleteRegion(Connection connection,
                                  HRegionInfo regionInfo)
    throws IOException {
    long time = EnvironmentEdgeManager.currentTime();
    Delete delete = new Delete(regionInfo.getRegionName());
    delete.addFamily(getCatalogFamily(), time);
    deleteFromMetaTable(connection, delete);
    LOG.info("Deleted " + regionInfo.getRegionNameAsString());
  }

  /**
   * Deletes the specified regions from META.
   * @param connection connection we're using
   * @param regionsInfo list of regions to be deleted from META
   * @throws IOException
   */
  public static void deleteRegions(Connection connection,
                                   List<HRegionInfo> regionsInfo) throws IOException {
    deleteRegions(connection, regionsInfo, EnvironmentEdgeManager.currentTime());
  }
  /**
   * Deletes the specified regions from META.
   * @param connection connection we're using
   * @param regionsInfo list of regions to be deleted from META
   * @throws IOException
   */
  public static void deleteRegions(Connection connection,
                                   List<HRegionInfo> regionsInfo, long ts) throws IOException {
    List<Delete> deletes = new ArrayList<Delete>(regionsInfo.size());
    for (HRegionInfo hri: regionsInfo) {
      Delete e = new Delete(hri.getRegionName());
      e.addFamily(getCatalogFamily(), ts);
      deletes.add(e);
    }
    deleteFromMetaTable(connection, deletes);
    LOG.info("Deleted " + regionsInfo);
  }

  /**
   * Adds and Removes the specified regions from hbase:meta
   * @param connection connection we're using
   * @param regionsToRemove list of regions to be deleted from META
   * @param regionsToAdd list of regions to be added to META
   * @throws IOException
   */
  public static void mutateRegions(Connection connection,
                                   final List<HRegionInfo> regionsToRemove,
                                   final List<HRegionInfo> regionsToAdd)
    throws IOException {
    List<Mutation> mutation = new ArrayList<Mutation>();
    if (regionsToRemove != null) {
      for (HRegionInfo hri: regionsToRemove) {
        mutation.add(makeDeleteFromRegionInfo(hri));
      }
    }
    if (regionsToAdd != null) {
      for (HRegionInfo hri: regionsToAdd) {
        mutation.add(makePutFromRegionInfo(hri));
      }
    }
    mutateMetaTable(connection, mutation);
    if (regionsToRemove != null && regionsToRemove.size() > 0) {
      LOG.debug("Deleted " + regionsToRemove);
    }
    if (regionsToAdd != null && regionsToAdd.size() > 0) {
      LOG.debug("Added " + regionsToAdd);
    }
  }

  /**
   * Overwrites the specified regions from hbase:meta
   * @param connection connection we're using
   * @param regionInfos list of regions to be added to META
   * @throws IOException
   */
  public static void overwriteRegions(Connection connection,
      List<HRegionInfo> regionInfos, int regionReplication) throws IOException {
    // use master time for delete marker and the Put
    long now = EnvironmentEdgeManager.currentTime();
    deleteRegions(connection, regionInfos, now);
    // Why sleep? This is the easiest way to ensure that the previous deletes does not
    // eclipse the following puts, that might happen in the same ts from the server.
    // See HBASE-9906, and HBASE-9879. Once either HBASE-9879, HBASE-8770 is fixed,
    // or HBASE-9905 is fixed and meta uses seqIds, we do not need the sleep.
    //
    // HBASE-13875 uses master timestamp for the mutations. The 20ms sleep is not needed
    addRegionsToMeta(connection, regionInfos, regionReplication, now+1);
    LOG.info("Overwritten " + regionInfos);
  }

  /**
   * Deletes merge qualifiers for the specified merged region.
   * @param connection connection we're using
   * @param mergedRegion
   * @throws IOException
   */
  public static void deleteMergeQualifiers(Connection connection,
                                           final HRegionInfo mergedRegion) throws IOException {
    long time = EnvironmentEdgeManager.currentTime();
    Delete delete = new Delete(mergedRegion.getRegionName());
    delete.addColumns(getCatalogFamily(), HConstants.MERGEA_QUALIFIER, time);
    delete.addColumns(getCatalogFamily(), HConstants.MERGEB_QUALIFIER, time);
    deleteFromMetaTable(connection, delete);
    LOG.info("Deleted references in merged region "
      + mergedRegion.getRegionNameAsString() + ", qualifier="
      + Bytes.toStringBinary(HConstants.MERGEA_QUALIFIER) + " and qualifier="
      + Bytes.toStringBinary(HConstants.MERGEB_QUALIFIER));
  }

  private static Put addRegionInfo(final Put p, final HRegionInfo hri)
    throws IOException {
    p.addImmutable(getCatalogFamily(), HConstants.REGIONINFO_QUALIFIER,
      hri.toByteArray());
    return p;
  }

  public static Put addLocation(final Put p, final ServerName sn, long openSeqNum,
      long time, int replicaId){
    if (time <= 0) {
      time = EnvironmentEdgeManager.currentTime();
    }
    p.addImmutable(getCatalogFamily(), getServerColumn(replicaId), time,
      Bytes.toBytes(sn.getHostAndPort()));
    p.addImmutable(getCatalogFamily(), getStartCodeColumn(replicaId), time,
      Bytes.toBytes(sn.getStartcode()));
    p.addImmutable(getCatalogFamily(), getSeqNumColumn(replicaId), time,
      Bytes.toBytes(openSeqNum));
    return p;
  }

  public static Put addEmptyLocation(final Put p, int replicaId) {
    long now = EnvironmentEdgeManager.currentTime();
    p.addImmutable(getCatalogFamily(), getServerColumn(replicaId), now, null);
    p.addImmutable(getCatalogFamily(), getStartCodeColumn(replicaId), now, null);
    p.addImmutable(getCatalogFamily(), getSeqNumColumn(replicaId), now, null);
    return p;
  }

  private static String mutationsToString(Mutation ... mutations) throws IOException {
    StringBuilder sb = new StringBuilder();
    String prefix = "";
    for (Mutation mutation : mutations) {
      sb.append(prefix).append(mutationToString(mutation));
      prefix = ", ";
    }
    return sb.toString();
  }

  private static String mutationsToString(List<? extends Mutation> mutations) throws IOException {
    StringBuilder sb = new StringBuilder();
    String prefix = "";
    for (Mutation mutation : mutations) {
      sb.append(prefix).append(mutationToString(mutation));
      prefix = ", ";
    }
    return sb.toString();
  }

  private static String mutationToString(Mutation p) throws IOException {
    return p.getClass().getSimpleName() + p.toJSON();
  }
}
