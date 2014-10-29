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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

/**
 * Reads region and assignment information from <code>hbase:meta</code>.
 */
@InterfaceAudience.Private
public class MetaReader {
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

  /**
   * Performs a full scan of <code>hbase:meta</code>, skipping regions from any
   * tables in the specified set of disabled tables.
   * @param catalogTracker
   * @param disabledTables set of disabled tables that will not be returned
   * @return Returns a map of every region to it's currently assigned server,
   * according to META.  If the region does not have an assignment it will have
   * a null value in the map.
   * @throws IOException
   */
  public static Map<HRegionInfo, ServerName> fullScan(
      CatalogTracker catalogTracker, final Set<TableName> disabledTables)
  throws IOException {
    return fullScan(catalogTracker, disabledTables, false);
  }

  /**
   * Performs a full scan of <code>hbase:meta</code>, skipping regions from any
   * tables in the specified set of disabled tables.
   * @param catalogTracker
   * @param disabledTables set of disabled tables that will not be returned
   * @param excludeOfflinedSplitParents If true, do not include offlined split
   * parents in the return.
   * @return Returns a map of every region to it's currently assigned server,
   * according to META.  If the region does not have an assignment it will have
   * a null value in the map.
   * @throws IOException
   */
  public static Map<HRegionInfo, ServerName> fullScan(
      CatalogTracker catalogTracker, final Set<TableName> disabledTables,
      final boolean excludeOfflinedSplitParents)
  throws IOException {
    final Map<HRegionInfo, ServerName> regions =
      new TreeMap<HRegionInfo, ServerName>();
    Visitor v = new Visitor() {
      @Override
      public boolean visit(Result r) throws IOException {
        if (r ==  null || r.isEmpty()) return true;
        Pair<HRegionInfo, ServerName> region = HRegionInfo.getHRegionInfoAndServerName(r);
        HRegionInfo hri = region.getFirst();
        if (hri  == null) return true;
        if (hri.getTable() == null) return true;
        if (disabledTables.contains(
            hri.getTable())) return true;
        // Are we to include split parents in the list?
        if (excludeOfflinedSplitParents && hri.isSplitParent()) return true;
        regions.put(hri, region.getSecond());
        return true;
      }
    };
    fullScan(catalogTracker, v);
    return regions;
  }

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
   * Reads the location of the specified region
   * @param catalogTracker
   * @param regionName region whose location we are after
   * @return location of region as a {@link ServerName} or null if not found
   * @throws IOException
   */
  static ServerName readRegionLocation(CatalogTracker catalogTracker,
      byte [] regionName)
  throws IOException {
    Pair<HRegionInfo, ServerName> pair = getRegion(catalogTracker, regionName);
    return (pair == null || pair.getSecond() == null)? null: pair.getSecond();
  }

  /**
   * Gets the region info and assignment for the specified region.
   * @param catalogTracker
   * @param regionName Region to lookup.
   * @return Location and HRegionInfo for <code>regionName</code>
   * @throws IOException
   */
  public static Pair<HRegionInfo, ServerName> getRegion(
      CatalogTracker catalogTracker, byte [] regionName)
  throws IOException {
    Get get = new Get(regionName);
    get.addFamily(HConstants.CATALOG_FAMILY);
    Result r = get(getCatalogHTable(catalogTracker), get);
    return (r == null || r.isEmpty())? null: HRegionInfo.getHRegionInfoAndServerName(r);
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
    HRegionInfo mergeA = HRegionInfo.getHRegionInfo(result,
        HConstants.MERGEA_QUALIFIER);
    HRegionInfo mergeB = HRegionInfo.getHRegionInfo(result,
        HConstants.MERGEB_QUALIFIER);
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
    if (tableName.equals(TableName.META_TABLE_NAME)) {
      // Catalog tables always exist.
      return true;
    }
    // Make a version of ResultCollectingVisitor that only collects the first
    CollectingVisitor<HRegionInfo> visitor = new CollectingVisitor<HRegionInfo>() {
      private HRegionInfo current = null;

      @Override
      public boolean visit(Result r) throws IOException {
        this.current =
          HRegionInfo.getHRegionInfo(r, HConstants.REGIONINFO_QUALIFIER);
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
      private Pair<HRegionInfo, ServerName> current = null;

      @Override
      public boolean visit(Result r) throws IOException {
        HRegionInfo hri =
          HRegionInfo.getHRegionInfo(r, HConstants.REGIONINFO_QUALIFIER);
        if (hri == null) {
          LOG.warn("No serialized HRegionInfo in " + r);
          return true;
        }
        if (!isInsideTable(hri, tableName)) return false;
        if (excludeOfflinedSplitParents && hri.isSplitParent()) return true;
        ServerName sn = HRegionInfo.getServerName(r);
        // Populate this.current so available when we call #add
        this.current = new Pair<HRegionInfo, ServerName>(hri, sn);
        // Else call super and add this Result to the collection.
        return super.visit(r);
      }

      @Override
      void add(Result r) {
        this.results.add(this.current);
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
        if (HRegionInfo.getHRegionInfo(r) == null) return;
        ServerName sn = HRegionInfo.getServerName(r);
        if (sn != null && sn.equals(serverName)) {
          this.results.add(r);
        }
      }
    };
    fullScan(catalogTracker, v);
    List<Result> results = v.getResults();
    if (results != null && !results.isEmpty()) {
      // Convert results to Map keyed by HRI
      for (Result r: results) {
        HRegionInfo hri = HRegionInfo.getHRegionInfo(r);
        if (hri != null) hris.put(hri, r);
      }
    }
    return hris;
  }

  public static void fullScanMetaAndPrint(final CatalogTracker catalogTracker)
  throws IOException {
    Visitor v = new Visitor() {
      @Override
      public boolean visit(Result r) throws IOException {
        if (r ==  null || r.isEmpty()) return true;
        LOG.info("fullScanMetaAndPrint.Current Meta Row: " + r);
        HRegionInfo hrim = HRegionInfo.getHRegionInfo(r);
        LOG.info("fullScanMetaAndPrint.HRI Print= " + hrim);
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
