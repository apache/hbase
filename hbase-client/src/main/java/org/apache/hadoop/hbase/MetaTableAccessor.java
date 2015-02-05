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

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ServiceException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.protobuf.generated.MultiRowMutationProtos;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.PairOfSameType;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.zookeeper.MetaTableLocator;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

  static final byte [] META_REGION_PREFIX;
  static {
    // Copy the prefix from FIRST_META_REGIONINFO into META_REGION_PREFIX.
    // FIRST_META_REGIONINFO == 'hbase:meta,,1'.  META_REGION_PREFIX == 'hbase:meta,'
    int len = HRegionInfo.FIRST_META_REGIONINFO.getRegionName().length - 2;
    META_REGION_PREFIX = new byte [len];
    System.arraycopy(HRegionInfo.FIRST_META_REGIONINFO.getRegionName(), 0,
      META_REGION_PREFIX, 0, len);
  }

  /** The delimiter for meta columns for replicaIds > 0 */
  protected static final char META_REPLICA_ID_DELIMITER = '_';

  /** A regex for parsing server columns from meta. See above javadoc for meta layout */
  private static final Pattern SERVER_COLUMN_PATTERN
    = Pattern.compile("^server(_[0-9a-fA-F]{4})?$");

  ////////////////////////
  // Reading operations //
  ////////////////////////

 /**
   * Performs a full scan of a <code>hbase:meta</code> table.
   * @return List of {@link org.apache.hadoop.hbase.client.Result}
   * @throws IOException
   */
  public static List<Result> fullScanOfMeta(Connection connection)
  throws IOException {
    CollectAllVisitor v = new CollectAllVisitor();
    fullScan(connection, v, null);
    return v.getResults();
  }

  /**
   * Performs a full scan of <code>hbase:meta</code>.
   * @param connection connection we're using
   * @param visitor Visitor invoked against each row.
   * @throws IOException
   */
  public static void fullScan(Connection connection,
      final Visitor visitor)
  throws IOException {
    fullScan(connection, visitor, null);
  }

  /**
   * Performs a full scan of <code>hbase:meta</code>.
   * @param connection connection we're using
   * @return List of {@link Result}
   * @throws IOException
   */
  public static List<Result> fullScan(Connection connection)
    throws IOException {
    CollectAllVisitor v = new CollectAllVisitor();
    fullScan(connection, v, null);
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
    if (connection == null || connection.isClosed()) {
      throw new NullPointerException("No connection");
    }
    // If the passed in 'connection' is 'managed' -- i.e. every second test uses
    // a Table or an HBaseAdmin with managed connections -- then doing
    // connection.getTable will throw an exception saying you are NOT to use
    // managed connections getting tables.  Leaving this as it is for now. Will
    // revisit when inclined to change all tests.  User code probaby makes use of
    // managed connections too so don't change it till post hbase 1.0.
    //
    // There should still be a way to use this method with an unmanaged connection.
    if (connection instanceof ClusterConnection) {
      if (((ClusterConnection) connection).isManaged()) {
        return new HTable(TableName.META_TABLE_NAME, (ClusterConnection) connection);
      }
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
    if (tableName.equals(TableName.META_TABLE_NAME)) {
      // Catalog tables always exist.
      return true;
    }
    // Make a version of ResultCollectingVisitor that only collects the first
    CollectingVisitor<HRegionInfo> visitor = new CollectingVisitor<HRegionInfo>() {
      private HRegionInfo current = null;

      @Override
      public boolean visit(Result r) throws IOException {
        RegionLocations locations = getRegionLocations(r);
        if (locations == null || locations.getRegionLocation().getRegionInfo() == null) {
          LOG.warn("No serialized HRegionInfo in " + r);
          return true;
        }
        this.current = locations.getRegionLocation().getRegionInfo();
        if (this.current == null) {
          LOG.warn("No serialized HRegionInfo in " + r);
          return true;
        }
        if (!isInsideTable(this.current, tableName)) return false;
        // Else call super and add this Result to the collection.
        super.visit(r);
        // Stop collecting regions from table after we get one.
        return false;
      }

      @Override
      void add(Result r) {
        // Add the current HRI.
        this.results.add(this.current);
      }
    };
    fullScan(connection, visitor, getTableStartRowForMeta(tableName));
    // If visitor has results >= 1 then table exists.
    return visitor.getResults().size() >= 1;
  }

  /**
   * Gets all of the regions of the specified table.
   * @param zkw zookeeper connection to access meta table
   * @param connection connection we're using
   * @param tableName table we're looking for
   * @return Ordered list of {@link HRegionInfo}.
   * @throws IOException
   */
  public static List<HRegionInfo> getTableRegions(ZooKeeperWatcher zkw,
      Connection connection, TableName tableName)
  throws IOException {
    return getTableRegions(zkw, connection, tableName, false);
  }

  /**
   * Gets all of the regions of the specified table.
   * @param zkw zookeeper connection to access meta table
   * @param connection connection we're using
   * @param tableName table we're looking for
   * @param excludeOfflinedSplitParents If true, do not include offlined split
   * parents in the return.
   * @return Ordered list of {@link HRegionInfo}.
   * @throws IOException
   */
  public static List<HRegionInfo> getTableRegions(ZooKeeperWatcher zkw,
      Connection connection, TableName tableName, final boolean excludeOfflinedSplitParents)
        throws IOException {
    List<Pair<HRegionInfo, ServerName>> result = null;
      result = getTableRegionsAndLocations(zkw, connection, tableName,
        excludeOfflinedSplitParents);
    return getListOfHRegionInfos(result);
  }

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
   * @return Place to start Scan in <code>hbase:meta</code> when passed a
   * <code>tableName</code>; returns &lt;tableName&rt; &lt;,&rt; &lt;,&rt;
   */
  static byte [] getTableStartRowForMeta(TableName tableName) {
    byte [] startRow = new byte[tableName.getName().length + 2];
    System.arraycopy(tableName.getName(), 0, startRow, 0, tableName.getName().length);
    startRow[startRow.length - 2] = HConstants.DELIMITER;
    startRow[startRow.length - 1] = HConstants.DELIMITER;
    return startRow;
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
  public static Scan getScanForTableName(TableName tableName) {
    String strName = tableName.getNameAsString();
    // Start key is just the table name with delimiters
    byte[] startKey = Bytes.toBytes(strName + ",,");
    // Stop key appends the smallest possible char to the table name
    byte[] stopKey = Bytes.toBytes(strName + " ,,");

    Scan scan = new Scan(startKey);
    scan.setStopRow(stopKey);
    return scan;
  }

  /**
   * @param zkw zookeeper connection to access meta table
   * @param connection connection we're using
   * @param tableName table we're looking for
   * @return Return list of regioninfos and server.
   * @throws IOException
   */
  public static List<Pair<HRegionInfo, ServerName>>
  getTableRegionsAndLocations(ZooKeeperWatcher zkw,
                              Connection connection, TableName tableName)
  throws IOException {
    return getTableRegionsAndLocations(zkw, connection, tableName, true);
  }

  /**
   * @param zkw ZooKeeperWatcher instance we're using to get hbase:meta location
   * @param connection connection we're using
   * @param tableName table to work with
   * @return Return list of regioninfos and server addresses.
   * @throws IOException
   */
  public static List<Pair<HRegionInfo, ServerName>> getTableRegionsAndLocations(
      ZooKeeperWatcher zkw, Connection connection, final TableName tableName,
      final boolean excludeOfflinedSplitParents) throws IOException {

    if (tableName.equals(TableName.META_TABLE_NAME)) {
      // If meta, do a bit of special handling.
      ServerName serverName = new MetaTableLocator().getMetaRegionLocation(zkw);
      List<Pair<HRegionInfo, ServerName>> list =
        new ArrayList<Pair<HRegionInfo, ServerName>>();
      list.add(new Pair<HRegionInfo, ServerName>(HRegionInfo.FIRST_META_REGIONINFO,
        serverName));
      return list;
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
          if (!isInsideTable(hri, tableName)) return false;
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
    fullScan(connection, visitor, getTableStartRowForMeta(tableName));
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
    fullScan(connection, v);
    return hris;
  }

  public static void fullScanMetaAndPrint(Connection connection)
    throws IOException {
    Visitor v = new Visitor() {
      @Override
      public boolean visit(Result r) throws IOException {
        if (r ==  null || r.isEmpty()) return true;
        LOG.info("fullScanMetaAndPrint.Current Meta Row: " + r);
        RegionLocations locations = getRegionLocations(r);
        if (locations == null) return true;
        for (HRegionLocation loc : locations.getRegionLocations()) {
          if (loc != null) {
            LOG.info("fullScanMetaAndPrint.HRI Print= " + loc.getRegionInfo());
          }
        }
        return true;
      }
    };
    fullScan(connection, v);
  }

  /**
   * Performs a full scan of a catalog table.
   * @param connection connection we're using
   * @param visitor Visitor invoked against each row.
   * @param startrow Where to start the scan. Pass null if want to begin scan
   * at first row.
   * <code>hbase:meta</code>, the default (pass false to scan hbase:meta)
   * @throws IOException
   */
  public static void fullScan(Connection connection,
    final Visitor visitor, final byte [] startrow)
  throws IOException {
    Scan scan = new Scan();
    if (startrow != null) scan.setStartRow(startrow);
    if (startrow == null) {
      int caching = connection.getConfiguration()
          .getInt(HConstants.HBASE_META_SCANNER_CACHING, 100);
      scan.setCaching(caching);
    }
    scan.addFamily(HConstants.CATALOG_FAMILY);
    Table metaTable = getMetaHTable(connection);
    ResultScanner scanner = null;
    try {
      scanner = metaTable.getScanner(scan);
      Result data;
      while((data = scanner.next()) != null) {
        if (data.isEmpty()) continue;
        // Break if visit returns false.
        if (!visitor.visit(data)) break;
      }
    } finally {
      if (scanner != null) scanner.close();
      metaTable.close();
    }
  }

  /**
   * Returns the column family used for meta columns.
   * @return HConstants.CATALOG_FAMILY.
   */
  protected static byte[] getFamily() {
    return HConstants.CATALOG_FAMILY;
  }

  /**
   * Returns the column qualifier for serialized region info
   * @return HConstants.REGIONINFO_QUALIFIER
   */
  protected static byte[] getRegionInfoColumn() {
    return HConstants.REGIONINFO_QUALIFIER;
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
  private static ServerName getServerName(final Result r, final int replicaId) {
    byte[] serverColumn = getServerColumn(replicaId);
    Cell cell = r.getColumnLatestCell(getFamily(), serverColumn);
    if (cell == null || cell.getValueLength() == 0) return null;
    String hostAndPort = Bytes.toString(
      cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
    byte[] startcodeColumn = getStartCodeColumn(replicaId);
    cell = r.getColumnLatestCell(getFamily(), startcodeColumn);
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
    Cell cell = r.getColumnLatestCell(getFamily(), getSeqNumColumn(replicaId));
    if (cell == null || cell.getValueLength() == 0) return HConstants.NO_SEQNUM;
    return Bytes.toLong(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
  }

  /**
   * Returns an HRegionLocationList extracted from the result.
   * @return an HRegionLocationList containing all locations for the region range or null if
   *  we can't deserialize the result.
   */
  public static RegionLocations getRegionLocations(final Result r) {
    if (r == null) return null;
    HRegionInfo regionInfo = getHRegionInfo(r, getRegionInfoColumn());
    if (regionInfo == null) return null;

    List<HRegionLocation> locations = new ArrayList<HRegionLocation>(1);
    NavigableMap<byte[],NavigableMap<byte[],byte[]>> familyMap = r.getNoVersionMap();

    locations.add(getRegionLocation(r, regionInfo, 0));

    NavigableMap<byte[], byte[]> infoMap = familyMap.get(getFamily());
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

      locations.add(getRegionLocation(r, regionInfo, replicaId));
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
  private static HRegionInfo getHRegionInfo(final Result r, byte [] qualifier) {
    Cell cell = r.getColumnLatestCell(getFamily(), qualifier);
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
    Put put = new Put(regionInfo.getRegionName());
    addRegionInfo(put, regionInfo);
    return put;
  }

  /**
   * Generates and returns a Delete containing the region info for the catalog
   * table
   */
  public static Delete makeDeleteFromRegionInfo(HRegionInfo regionInfo) {
    if (regionInfo == null) {
      throw new IllegalArgumentException("Can't make a delete for null region");
    }
    Delete delete = new Delete(regionInfo.getRegionName());
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
      Delete deleteReplicaLocations = new Delete(row);
      for (int i = replicaIndexToDeleteFrom; i < absoluteIndex; i++) {
        deleteReplicaLocations.deleteColumns(HConstants.CATALOG_FAMILY,
          getServerColumn(i));
        deleteReplicaLocations.deleteColumns(HConstants.CATALOG_FAMILY,
          getSeqNumColumn(i));
        deleteReplicaLocations.deleteColumns(HConstants.CATALOG_FAMILY,
          getStartCodeColumn(i));
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
      t.batch(mutations);
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
   * {@link #splitRegion(org.apache.hadoop.hbase.client.Connection,
   *   HRegionInfo, HRegionInfo, HRegionInfo, ServerName)}
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
    if (LOG.isDebugEnabled()) {
      LOG.debug("Added " + regionInfo.getRegionNameAsString());
    }
  }

  /**
   * Adds a (single) hbase:meta row for the specified new region and its daughters. Note that this
   * does not add its daughter's as different rows, but adds information about the daughters
   * in the same row as the parent. Use
   * {@link #splitRegion(Connection, HRegionInfo, HRegionInfo, HRegionInfo, ServerName)}
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
                                      List<HRegionInfo> regionInfos)
    throws IOException {
    List<Put> puts = new ArrayList<Put>();
    for (HRegionInfo regionInfo : regionInfos) {
      if (RegionReplicaUtil.isDefaultReplica(regionInfo)) {
        puts.add(makePutFromRegionInfo(regionInfo));
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
    Put put = new Put(regionInfo.getRegionName());
    addRegionInfo(put, regionInfo);
    if (sn != null) {
      addLocation(put, sn, openSeqNum, regionInfo.getReplicaId());
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
   * @throws IOException
   */
  public static void mergeRegions(final Connection connection, HRegionInfo mergedRegion,
      HRegionInfo regionA, HRegionInfo regionB, ServerName sn) throws IOException {
    Table meta = getMetaHTable(connection);
    try {
      HRegionInfo copyOfMerged = new HRegionInfo(mergedRegion);

      // Put for parent
      Put putOfMerged = makePutFromRegionInfo(copyOfMerged);
      putOfMerged.addImmutable(HConstants.CATALOG_FAMILY, HConstants.MERGEA_QUALIFIER,
        regionA.toByteArray());
      putOfMerged.addImmutable(HConstants.CATALOG_FAMILY, HConstants.MERGEB_QUALIFIER,
        regionB.toByteArray());

      // Deletes for merging regions
      Delete deleteA = makeDeleteFromRegionInfo(regionA);
      Delete deleteB = makeDeleteFromRegionInfo(regionB);

      // The merged is a new region, openSeqNum = 1 is fine.
      addLocation(putOfMerged, sn, 1, mergedRegion.getReplicaId());

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
                                 ServerName sn) throws IOException {
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

      addLocation(putA, sn, 1, splitA.getReplicaId()); //new regions, openSeqNum = 1 is fine.
      addLocation(putB, sn, 1, splitB.getReplicaId());

      byte[] tableRow = Bytes.toBytes(parent.getRegionNameAsString() + HConstants.DELIMITER);
      multiMutate(meta, tableRow, putParent, putA, putB);
    } finally {
      meta.close();
    }
  }

  /**
   * Performs an atomic multi-Mutate operation against the given table.
   */
  private static void multiMutate(Table table, byte[] row, Mutation... mutations)
      throws IOException {
    CoprocessorRpcChannel channel = table.coprocessorService(row);
    MultiRowMutationProtos.MutateRowsRequest.Builder mmrBuilder
      = MultiRowMutationProtos.MutateRowsRequest.newBuilder();
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
   * @param sn Server name
   * @throws IOException
   */
  public static void updateRegionLocation(Connection connection,
                                          HRegionInfo regionInfo, ServerName sn, long updateSeqNum)
    throws IOException {
    updateLocation(connection, regionInfo, sn, updateSeqNum);
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
   * @throws IOException In particular could throw {@link java.net.ConnectException}
   * if the server is down on other end.
   */
  private static void updateLocation(final Connection connection,
                                     HRegionInfo regionInfo, ServerName sn, long openSeqNum)
    throws IOException {
    // region replicas are kept in the primary region's row
    Put put = new Put(getMetaKeyForRegion(regionInfo));
    addLocation(put, sn, openSeqNum, regionInfo.getReplicaId());
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
    Delete delete = new Delete(regionInfo.getRegionName());
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
    List<Delete> deletes = new ArrayList<Delete>(regionsInfo.size());
    for (HRegionInfo hri: regionsInfo) {
      deletes.add(new Delete(hri.getRegionName()));
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
        mutation.add(new Delete(hri.getRegionName()));
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
                                      List<HRegionInfo> regionInfos) throws IOException {
    deleteRegions(connection, regionInfos);
    // Why sleep? This is the easiest way to ensure that the previous deletes does not
    // eclipse the following puts, that might happen in the same ts from the server.
    // See HBASE-9906, and HBASE-9879. Once either HBASE-9879, HBASE-8770 is fixed,
    // or HBASE-9905 is fixed and meta uses seqIds, we do not need the sleep.
    Threads.sleep(20);
    addRegionsToMeta(connection, regionInfos);
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
    Delete delete = new Delete(mergedRegion.getRegionName());
    delete.deleteColumns(HConstants.CATALOG_FAMILY, HConstants.MERGEA_QUALIFIER);
    delete.deleteColumns(HConstants.CATALOG_FAMILY, HConstants.MERGEB_QUALIFIER);
    deleteFromMetaTable(connection, delete);
    LOG.info("Deleted references in merged region "
      + mergedRegion.getRegionNameAsString() + ", qualifier="
      + Bytes.toStringBinary(HConstants.MERGEA_QUALIFIER) + " and qualifier="
      + Bytes.toStringBinary(HConstants.MERGEB_QUALIFIER));
  }

  private static Put addRegionInfo(final Put p, final HRegionInfo hri)
    throws IOException {
    p.addImmutable(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER,
      hri.toByteArray());
    return p;
  }

  public static Put addLocation(final Put p, final ServerName sn, long openSeqNum, int replicaId){
    // using regionserver's local time as the timestamp of Put.
    // See: HBASE-11536
    long now = EnvironmentEdgeManager.currentTime();
    p.addImmutable(HConstants.CATALOG_FAMILY, getServerColumn(replicaId), now,
      Bytes.toBytes(sn.getHostAndPort()));
    p.addImmutable(HConstants.CATALOG_FAMILY, getStartCodeColumn(replicaId), now,
      Bytes.toBytes(sn.getStartcode()));
    p.addImmutable(HConstants.CATALOG_FAMILY, getSeqNumColumn(replicaId), now,
      Bytes.toBytes(openSeqNum));
    return p;
  }
}
