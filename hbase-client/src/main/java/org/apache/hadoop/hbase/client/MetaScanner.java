/**
 *
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

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.exceptions.TableNotFoundException;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Scanner class that contains the <code>.META.</code> table scanning logic.
 * Provided visitors will be called for each row.
 *
 * Although public visibility, this is not a public-facing API and may evolve in
 * minor releases.
 *
 * <p> Note that during concurrent region splits, the scanner might not see
 * META changes across rows (for parent and daughter entries) consistently.
 * see HBASE-5986, and {@link DefaultMetaScannerVisitor} for details. </p>
 */
@InterfaceAudience.Private
public class MetaScanner {
  private static final Log LOG = LogFactory.getLog(MetaScanner.class);
  /**
   * Scans the meta table and calls a visitor on each RowResult and uses a empty
   * start row value as table name.
   *
   * @param configuration conf
   * @param visitor A custom visitor
   * @throws IOException e
   */
  public static void metaScan(Configuration configuration,
      MetaScannerVisitor visitor)
  throws IOException {
    metaScan(configuration, visitor, null);
  }

  /**
   * Scans the meta table and calls a visitor on each RowResult. Uses a table
   * name to locate meta regions.
   *
   * @param configuration config
   * @param visitor visitor object
   * @param userTableName User table name in meta table to start scan at.  Pass
   * null if not interested in a particular table.
   * @throws IOException e
   */
  public static void metaScan(Configuration configuration,
      MetaScannerVisitor visitor, byte [] userTableName)
  throws IOException {
    metaScan(configuration, visitor, userTableName, null, Integer.MAX_VALUE);
  }

  /**
   * Scans the meta table and calls a visitor on each RowResult. Uses a table
   * name and a row name to locate meta regions. And it only scans at most
   * <code>rowLimit</code> of rows.
   *
   * @param configuration HBase configuration.
   * @param visitor Visitor object.
   * @param userTableName User table name in meta table to start scan at.  Pass
   * null if not interested in a particular table.
   * @param row Name of the row at the user table. The scan will start from
   * the region row where the row resides.
   * @param rowLimit Max of processed rows. If it is less than 0, it
   * will be set to default value <code>Integer.MAX_VALUE</code>.
   * @throws IOException e
   */
  public static void metaScan(Configuration configuration,
      MetaScannerVisitor visitor, byte [] userTableName, byte[] row,
      int rowLimit)
  throws IOException {
    metaScan(configuration, visitor, userTableName, row, rowLimit,
      HConstants.META_TABLE_NAME);
  }

  /**
   * Scans the meta table and calls a visitor on each RowResult. Uses a table
   * name and a row name to locate meta regions. And it only scans at most
   * <code>rowLimit</code> of rows.
   *
   * @param configuration HBase configuration.
   * @param visitor Visitor object. Closes the visitor before returning.
   * @param tableName User table name in meta table to start scan at.  Pass
   * null if not interested in a particular table.
   * @param row Name of the row at the user table. The scan will start from
   * the region row where the row resides.
   * @param rowLimit Max of processed rows. If it is less than 0, it
   * will be set to default value <code>Integer.MAX_VALUE</code>.
   * @param metaTableName Meta table to scan, root or meta.
   * @throws IOException e
   */
  public static void metaScan(Configuration configuration,
      final MetaScannerVisitor visitor, final byte[] tableName,
      final byte[] row, final int rowLimit, final byte[] metaTableName)
  throws IOException {
    int rowUpperLimit = rowLimit > 0 ? rowLimit: Integer.MAX_VALUE;
    HTable metaTable = new HTable(configuration, HConstants.META_TABLE_NAME);
    // Calculate startrow for scan.
    byte[] startRow;
    try {
      if (row != null) {
        // Scan starting at a particular row in a particular table
        byte[] searchRow = HRegionInfo.createRegionName(tableName, row, HConstants.NINES, false);
        Result startRowResult = metaTable.getRowOrBefore(searchRow, HConstants.CATALOG_FAMILY);
        if (startRowResult == null) {
          throw new TableNotFoundException("Cannot find row in .META. for table: " +
            Bytes.toString(tableName) + ", row=" + Bytes.toStringBinary(searchRow));
        }
        HRegionInfo regionInfo = getHRegionInfo(startRowResult);
        if (regionInfo == null) {
          throw new IOException("HRegionInfo was null or empty in Meta for " +
            Bytes.toString(tableName) + ", row=" + Bytes.toStringBinary(searchRow));
        }
        byte[] rowBefore = regionInfo.getStartKey();
        startRow = HRegionInfo.createRegionName(tableName, rowBefore, HConstants.ZEROES, false);
      } else if (tableName == null || tableName.length == 0) {
        // Full META scan
        startRow = HConstants.EMPTY_START_ROW;
      } else {
        // Scan META for an entire table
        startRow = HRegionInfo.createRegionName(tableName, HConstants.EMPTY_START_ROW,
          HConstants.ZEROES, false);
      }
      final Scan scan = new Scan(startRow).addFamily(HConstants.CATALOG_FAMILY);
      int rows = Math.min(rowLimit, configuration.getInt(HConstants.HBASE_META_SCANNER_CACHING,
        HConstants.DEFAULT_HBASE_META_SCANNER_CACHING));
      scan.setCaching(rows);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Scanning " + Bytes.toString(metaTableName) + " starting at row=" +
          Bytes.toStringBinary(startRow) + " for max=" + rowUpperLimit + " with caching=" + rows);
      }
      // Run the scan
      ResultScanner scanner = metaTable.getScanner(scan);
      Result result = null;
      int processedRows = 0;
      while ((result = scanner.next()) != null) {
        if (visitor != null) {
          if (!visitor.processRow(result)) break;
        }
        processedRows++;
        if (processedRows >= rowUpperLimit) break;
      }
    } finally {
      if (visitor != null) visitor.close();
      if (metaTable != null) metaTable.close();
    }
  }

  /**
   * Returns HRegionInfo object from the column
   * HConstants.CATALOG_FAMILY:HConstants.REGIONINFO_QUALIFIER of the catalog
   * table Result.
   * @param data a Result object from the catalog table scan
   * @return HRegionInfo or null
   */
  public static HRegionInfo getHRegionInfo(Result data) {
    byte [] bytes =
      data.getValue(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER);
    if (bytes == null) return null;
    HRegionInfo info = HRegionInfo.parseFromOrNull(bytes);
    if (LOG.isTraceEnabled()) {
      LOG.trace("Current INFO from scan results = " + info);
    }
    return info;
  }

  /**
   * Lists all of the regions currently in META.
   * @param conf
   * @return List of all user-space regions.
   * @throws IOException
   */
  public static List<HRegionInfo> listAllRegions(Configuration conf)
  throws IOException {
    return listAllRegions(conf, true);
  }

  /**
   * Lists all of the regions currently in META.
   * @param conf
   * @param offlined True if we are to include offlined regions, false and we'll
   * leave out offlined regions from returned list.
   * @return List of all user-space regions.
   * @throws IOException
   */
  public static List<HRegionInfo> listAllRegions(Configuration conf, final boolean offlined)
  throws IOException {
    final List<HRegionInfo> regions = new ArrayList<HRegionInfo>();
    MetaScannerVisitor visitor = new DefaultMetaScannerVisitor() {
        @Override
        public boolean processRowInternal(Result result) throws IOException {
          if (result == null || result.isEmpty()) {
            return true;
          }

          HRegionInfo regionInfo = getHRegionInfo(result);
          if (regionInfo == null) {
            LOG.warn("Null REGIONINFO_QUALIFIER: " + result);
            return true;
          }

          // If region offline AND we are not to include offlined regions, return.
          if (regionInfo.isOffline() && !offlined) return true;
          regions.add(regionInfo);
          return true;
        }
    };
    metaScan(conf, visitor);
    return regions;
  }

  /**
   * Lists all of the table regions currently in META.
   * @param conf
   * @param offlined True if we are to include offlined regions, false and we'll
   * leave out offlined regions from returned list.
   * @return Map of all user-space regions to servers
   * @throws IOException
   */
  public static NavigableMap<HRegionInfo, ServerName> allTableRegions(Configuration conf,
      final byte [] tablename, final boolean offlined) throws IOException {
    final NavigableMap<HRegionInfo, ServerName> regions =
      new TreeMap<HRegionInfo, ServerName>();
    MetaScannerVisitor visitor = new TableMetaScannerVisitor(tablename) {
      @Override
      public boolean processRowInternal(Result rowResult) throws IOException {
        HRegionInfo info = getHRegionInfo(rowResult);
        ServerName serverName = HRegionInfo.getServerName(rowResult);
        regions.put(new UnmodifyableHRegionInfo(info), serverName);
        return true;
      }
    };
    metaScan(conf, visitor, tablename);
    return regions;
  }

  /**
   * Visitor class called to process each row of the .META. table
   */
  public interface MetaScannerVisitor extends Closeable {
    /**
     * Visitor method that accepts a RowResult and the meta region location.
     * Implementations can return false to stop the region's loop if it becomes
     * unnecessary for some reason.
     *
     * @param rowResult result
     * @return A boolean to know if it should continue to loop in the region
     * @throws IOException e
     */
    public boolean processRow(Result rowResult) throws IOException;
  }

  public static abstract class MetaScannerVisitorBase implements MetaScannerVisitor {
    @Override
    public void close() throws IOException {
    }
  }

  /**
   * A MetaScannerVisitor that skips offline regions and split parents
   */
  public static abstract class DefaultMetaScannerVisitor
    extends MetaScannerVisitorBase {

    public DefaultMetaScannerVisitor() {
      super();
    }

    public abstract boolean processRowInternal(Result rowResult) throws IOException;

    @Override
    public boolean processRow(Result rowResult) throws IOException {
      HRegionInfo info = getHRegionInfo(rowResult);
      if (info == null) {
        return true;
      }

      //skip over offline and split regions
      if (!(info.isOffline() || info.isSplit())) {
        return processRowInternal(rowResult);
      }
      return true;
    }
  }

  /**
   * A MetaScannerVisitor for a table. Provides a consistent view of the table's
   * META entries during concurrent splits (see HBASE-5986 for details). This class
   * does not guarantee ordered traversal of meta entries, and can block until the
   * META entries for daughters are available during splits.
   */
  public static abstract class TableMetaScannerVisitor extends DefaultMetaScannerVisitor {
    private byte[] tableName;

    public TableMetaScannerVisitor(byte[] tableName) {
      super();
      this.tableName = tableName;
    }

    @Override
    public final boolean processRow(Result rowResult) throws IOException {
      HRegionInfo info = getHRegionInfo(rowResult);
      if (info == null) {
        return true;
      }
      if (!(Bytes.equals(info.getTableName(), tableName))) {
        return false;
      }
      return super.processRow(rowResult);
    }
  }
}
