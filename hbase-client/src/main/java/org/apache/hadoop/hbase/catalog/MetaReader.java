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
package org.apache.hadoop.hbase.catalog;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.PairOfSameType;

import com.google.common.annotations.VisibleForTesting;

/**
 * Reads region and assignment information from <code>hbase:meta</code>.
 */
@InterfaceAudience.Private
public class MetaReader {

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
   * The actual layout of meta should be encapsulated inside MetaReader and MetaEditor methods,
   * and should not leak out of those (through Result objects, etc)
   */

  // TODO: Strip CatalogTracker from this class.  Its all over and in the end
  // its only used to get its Configuration so we can get associated
  // Connection.
  private static final Log LOG = LogFactory.getLog(MetaReader.class);

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

  /**
   * Performs a full scan of <code>hbase:meta</code>.
   * @return List of {@link Result}
   * @throws IOException
   */
  public static List<Result> fullScan(CatalogTracker catalogTracker)
  throws IOException {
    CollectAllVisitor v = new CollectAllVisitor();
    fullScan(catalogTracker, v, null);
    return v.getResults();
  }

  /**
   * Performs a full scan of a <code>hbase:meta</code> table.
   * @return List of {@link Result}
   * @throws IOException
   */
  public static List<Result> fullScanOfMeta(CatalogTracker catalogTracker)
  throws IOException {
    CollectAllVisitor v = new CollectAllVisitor();
    fullScan(catalogTracker, v, null);
    return v.getResults();
  }

  /**
   * Performs a full scan of <code>hbase:meta</code>.
   * @param catalogTracker
   * @param visitor Visitor invoked against each row.
   * @throws IOException
   */
  public static void fullScan(CatalogTracker catalogTracker,
      final Visitor visitor)
  throws IOException {
    fullScan(catalogTracker, visitor, null);
  }

  /**
   * Callers should call close on the returned {@link HTable} instance.
   * @param catalogTracker We'll use this catalogtracker's connection
   * @param tableName Table to get an {@link HTable} against.
   * @return An {@link HTable} for <code>tableName</code>
   * @throws IOException
   */
  @SuppressWarnings("deprecation")
  private static HTable getHTable(final CatalogTracker catalogTracker,
      final TableName tableName)
  throws IOException {
    // Passing the CatalogTracker's connection ensures this
    // HTable instance uses the CatalogTracker's connection.
    org.apache.hadoop.hbase.client.HConnection c = catalogTracker.getConnection();
    if (c == null) throw new NullPointerException("No connection");
    return new HTable(tableName, c);
  }

  /**
   * Callers should call close on the returned {@link HTable} instance.
   * @param catalogTracker
   * @return An {@link HTable} for <code>hbase:meta</code>
   * @throws IOException
   */
  static HTable getCatalogHTable(final CatalogTracker catalogTracker)
  throws IOException {
    return getMetaHTable(catalogTracker);
  }

  /**
   * Callers should call close on the returned {@link HTable} instance.
   * @param ct
   * @return An {@link HTable} for <code>hbase:meta</code>
   * @throws IOException
   */
  static HTable getMetaHTable(final CatalogTracker ct)
  throws IOException {
    return getHTable(ct, TableName.META_TABLE_NAME);
  }

  /**
   * @param t Table to use (will be closed when done).
   * @param g Get to run
   * @throws IOException
   */
  private static Result get(final HTable t, final Get g) throws IOException {
    try {
      return t.get(g);
    } finally {
      t.close();
    }
  }

  /**
   * Gets the region info and assignment for the specified region.
   * @param catalogTracker
   * @param regionName Region to lookup.
   * @return Location and HRegionInfo for <code>regionName</code>
   * @throws IOException
   * @deprecated use {@link #getRegionLocation(CatalogTracker, byte[])} instead
   */
  @Deprecated
  public static Pair<HRegionInfo, ServerName> getRegion(
      CatalogTracker catalogTracker, byte [] regionName)
  throws IOException {
    HRegionLocation location = getRegionLocation(catalogTracker, regionName);
    return location == null
        ? null
        : new Pair<HRegionInfo, ServerName>(location.getRegionInfo(), location.getServerName());
  }

  /**
   * Returns the HRegionLocation from meta for the given region
   * @param catalogTracker
   * @param regionName
   * @return HRegionLocation for the given region
   * @throws IOException
   */
  public static HRegionLocation getRegionLocation(CatalogTracker catalogTracker,
      byte[] regionName) throws IOException {
    byte[] row = regionName;
    HRegionInfo parsedInfo = null;
    try {
      parsedInfo = parseRegionInfoFromRegionName(regionName);
      row = getMetaKeyForRegion(parsedInfo);
    } catch (Exception parseEx) {
      LOG.warn("Received parse exception:" + parseEx);
    }
    Get get = new Get(row);
    get.addFamily(HConstants.CATALOG_FAMILY);
    Result r = get(getCatalogHTable(catalogTracker), get);
    RegionLocations locations = getRegionLocations(r);
    return locations == null
        ? null
        : locations.getRegionLocation(parsedInfo == null ? 0 : parsedInfo.getReplicaId());
  }

  /**
   * Returns the HRegionLocation from meta for the given region
   * @param catalogTracker
   * @param regionInfo
   * @return HRegionLocation for the given region
   * @throws IOException
   */
  public static HRegionLocation getRegionLocation(CatalogTracker catalogTracker,
      HRegionInfo regionInfo) throws IOException {
    byte[] row = getMetaKeyForRegion(regionInfo);
    Get get = new Get(row);
    get.addFamily(HConstants.CATALOG_FAMILY);
    Result r = get(getCatalogHTable(catalogTracker), get);
    return getRegionLocation(r, regionInfo, regionInfo.getReplicaId());
  }

  /** Returns the row key to use for this regionInfo */
  protected static byte[] getMetaKeyForRegion(HRegionInfo regionInfo) {
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
   * @param catalogTracker
   * @param regionName
   * @return result of the specified region
   * @throws IOException
   */
  public static Result getRegionResult(CatalogTracker catalogTracker,
      byte[] regionName) throws IOException {
    Get get = new Get(regionName);
    get.addFamily(HConstants.CATALOG_FAMILY);
    return get(getCatalogHTable(catalogTracker), get);
  }

  /**
   * Get regions from the merge qualifier of the specified merged region
   * @return null if it doesn't contain merge qualifier, else two merge regions
   * @throws IOException
   */
  public static Pair<HRegionInfo, HRegionInfo> getRegionsFromMergeQualifier(
      CatalogTracker catalogTracker, byte[] regionName) throws IOException {
    Result result = getRegionResult(catalogTracker, regionName);
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
   * @param catalogTracker
   * @param tableName table to check
   * @return true if the table exists in meta, false if not
   * @throws IOException
   */
  public static boolean tableExists(CatalogTracker catalogTracker,
      final TableName tableName)
  throws IOException {
    if (tableName.equals(HTableDescriptor.META_TABLEDESC.getTableName())) {
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
    fullScan(catalogTracker, visitor, getTableStartRowForMeta(tableName));
    // If visitor has results >= 1 then table exists.
    return visitor.getResults().size() >= 1;
  }

  /**
   * Gets all of the regions of the specified table.
   * @param catalogTracker
   * @param tableName
   * @return Ordered list of {@link HRegionInfo}.
   * @throws IOException
   */
  public static List<HRegionInfo> getTableRegions(CatalogTracker catalogTracker,
      TableName tableName)
  throws IOException {
    return getTableRegions(catalogTracker, tableName, false);
  }

  /**
   * Gets all of the regions of the specified table.
   * @param catalogTracker
   * @param tableName
   * @param excludeOfflinedSplitParents If true, do not include offlined split
   * parents in the return.
   * @return Ordered list of {@link HRegionInfo}.
   * @throws IOException
   */
  public static List<HRegionInfo> getTableRegions(CatalogTracker catalogTracker,
      TableName tableName, final boolean excludeOfflinedSplitParents)
  throws IOException {
    List<Pair<HRegionInfo, ServerName>> result = null;
    try {
      result = getTableRegionsAndLocations(catalogTracker, tableName,
        excludeOfflinedSplitParents);
    } catch (InterruptedException e) {
      throw (InterruptedIOException)new InterruptedIOException().initCause(e);
    }
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
   * @param current
   * @param tableName
   * @return True if <code>current</code> tablename is equal to
   * <code>tableName</code>
   */
  static boolean isInsideTable(final HRegionInfo current, final TableName tableName) {
    return tableName.equals(current.getTable());
  }

  /**
   * @param tableName
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
   * @param catalogTracker
   * @param tableName
   * @return Return list of regioninfos and server.
   * @throws IOException
   * @throws InterruptedException
   */
  public static List<Pair<HRegionInfo, ServerName>>
  getTableRegionsAndLocations(CatalogTracker catalogTracker, TableName tableName)
  throws IOException, InterruptedException {
    return getTableRegionsAndLocations(catalogTracker, tableName,
      true);
  }

  /**
   * @param catalogTracker
   * @param tableName
   * @return Return list of regioninfos and server addresses.
   * @throws IOException
   * @throws InterruptedException
   */
  public static List<Pair<HRegionInfo, ServerName>>
  getTableRegionsAndLocations(final CatalogTracker catalogTracker,
      final TableName tableName, final boolean excludeOfflinedSplitParents)
  throws IOException, InterruptedException {
    if (tableName.equals(TableName.META_TABLE_NAME)) {
      // If meta, do a bit of special handling.
      ServerName serverName = catalogTracker.getMetaLocation();
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
    fullScan(catalogTracker, visitor, getTableStartRowForMeta(tableName));
    return visitor.getResults();
  }

  /**
   * @param catalogTracker
   * @param serverName
   * @return List of user regions installed on this server (does not include
   * catalog regions).
   * @throws IOException
   */
  public static NavigableMap<HRegionInfo, Result>
  getServerUserRegions(CatalogTracker catalogTracker, final ServerName serverName)
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
    fullScan(catalogTracker, v);
    return hris;
  }

  public static void fullScanMetaAndPrint(final CatalogTracker catalogTracker)
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
    fullScan(catalogTracker, v);
  }

  /**
   * Performs a full scan of a catalog table.
   * @param catalogTracker
   * @param visitor Visitor invoked against each row.
   * @param startrow Where to start the scan. Pass null if want to begin scan
   * at first row.
   * <code>hbase:meta</code>, the default (pass false to scan hbase:meta)
   * @throws IOException
   */
  public static void fullScan(CatalogTracker catalogTracker,
    final Visitor visitor, final byte [] startrow)
  throws IOException {
    Scan scan = new Scan();
    if (startrow != null) scan.setStartRow(startrow);
    if (startrow == null) {
      int caching = catalogTracker.getConnection().getConfiguration()
          .getInt(HConstants.HBASE_META_SCANNER_CACHING, 100);
      scan.setCaching(caching);
    }
    scan.addFamily(HConstants.CATALOG_FAMILY);
    HTable metaTable = getMetaHTable(catalogTracker);
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
    return;
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
  protected static byte[] getServerColumn(int replicaId) {
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
  protected static byte[] getStartCodeColumn(int replicaId) {
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
  protected static byte[] getSeqNumColumn(int replicaId) {
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
   * E.g. the seqNum when the result of {@link #getServerName(Result)} was written.
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
   * @return an HRegionLocationList containing all locations for the region range
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

    for (Entry<byte[], byte[]> entry : serverMap.entrySet()) {
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
  public static PairOfSameType<HRegionInfo> getDaughterRegions(Result data) throws IOException {
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
  public static PairOfSameType<HRegionInfo> getMergeRegions(Result data) throws IOException {
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
   * @param c
   * @param tableName
   * @return Count or regions in table <code>tableName</code>
   * @throws IOException
   */
  public static int getRegionCount(final Configuration c, final String tableName) throws IOException {
    HTable t = new HTable(c, tableName);
    try {
      return t.getRegionLocations().size();
    } finally {
      t.close();
    }
  }
}
