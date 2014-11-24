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
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ExceptionUtil;

/**
 * Scanner class that contains the <code>hbase:meta</code> table scanning logic.
 * Provided visitors will be called for each row.
 *
 * Although public visibility, this is not a public-facing API and may evolve in
 * minor releases.
 *
 * <p> Note that during concurrent region splits, the scanner might not see
 * hbase:meta changes across rows (for parent and daughter entries) consistently.
 * see HBASE-5986, and {@link DefaultMetaScannerVisitor} for details. </p>
 */
@InterfaceAudience.Private
//TODO: merge this to MetaTableAccessor, get rid of it.
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
    metaScan(configuration, visitor, null, null, Integer.MAX_VALUE);
  }

  /**
   * Scans the meta table and calls a visitor on each RowResult. Uses a table
   * name to locate meta regions.
   *
   * @param configuration config
   * @param connection connection to use internally (null to use a new instance)
   * @param visitor visitor object
   * @param userTableName User table name in meta table to start scan at.  Pass
   * null if not interested in a particular table.
   * @throws IOException e
   */
  public static void metaScan(Configuration configuration, Connection connection,
      MetaScannerVisitor visitor, TableName userTableName) throws IOException {
    metaScan(configuration, connection, visitor, userTableName, null, Integer.MAX_VALUE,
        TableName.META_TABLE_NAME);
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
      MetaScannerVisitor visitor, TableName userTableName, byte[] row,
      int rowLimit)
  throws IOException {
    metaScan(configuration, null, visitor, userTableName, row, rowLimit,
      TableName.META_TABLE_NAME);
  }

  /**
   * Scans the meta table and calls a visitor on each RowResult. Uses a table
   * name and a row name to locate meta regions. And it only scans at most
   * <code>rowLimit</code> of rows.
   *
   * @param configuration HBase configuration.
   * @param connection connection to use internally (null to use a new instance)
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
  static void metaScan(Configuration configuration, Connection connection,
      final MetaScannerVisitor visitor, final TableName tableName,
      final byte[] row, final int rowLimit, final TableName metaTableName)
    throws IOException {

    boolean closeConnection = false;
    if (connection == null){
      connection = ConnectionFactory.createConnection(configuration);
      closeConnection = true;
    }

    int rowUpperLimit = rowLimit > 0 ? rowLimit: Integer.MAX_VALUE;
    // Calculate startrow for scan.
    byte[] startRow;
    ResultScanner scanner = null;
    HTable metaTable = null;
    try {
      metaTable = new HTable(TableName.META_TABLE_NAME, connection, null);
      if (row != null) {
        // Scan starting at a particular row in a particular table
        byte[] searchRow = HRegionInfo.createRegionName(tableName, row, HConstants.NINES, false);

        Result startRowResult = metaTable.getRowOrBefore(searchRow, HConstants.CATALOG_FAMILY);

        if (startRowResult == null) {
          throw new TableNotFoundException("Cannot find row in "+ TableName
              .META_TABLE_NAME.getNameAsString()+" for table: "
              + tableName + ", row=" + Bytes.toStringBinary(searchRow));
        }
        HRegionInfo regionInfo = getHRegionInfo(startRowResult);
        if (regionInfo == null) {
          throw new IOException("HRegionInfo was null or empty in Meta for " +
            tableName + ", row=" + Bytes.toStringBinary(searchRow));
        }
        byte[] rowBefore = regionInfo.getStartKey();
        startRow = HRegionInfo.createRegionName(tableName, rowBefore, HConstants.ZEROES, false);
      } else if (tableName == null || tableName.getName().length == 0) {
        // Full hbase:meta scan
        startRow = HConstants.EMPTY_START_ROW;
      } else {
        // Scan hbase:meta for an entire table
        startRow = HRegionInfo.createRegionName(tableName, HConstants.EMPTY_START_ROW,
          HConstants.ZEROES, false);
      }
      final Scan scan = new Scan(startRow).addFamily(HConstants.CATALOG_FAMILY);
      int scannerCaching = configuration.getInt(HConstants.HBASE_META_SCANNER_CACHING,
        HConstants.DEFAULT_HBASE_META_SCANNER_CACHING);
      if (rowUpperLimit <= scannerCaching) {
          scan.setSmall(true);
      }
      int rows = Math.min(rowLimit, scannerCaching);
      scan.setCaching(rows);
      if (LOG.isTraceEnabled()) {
        LOG.trace("Scanning " + metaTableName.getNameAsString() + " starting at row=" +
          Bytes.toStringBinary(startRow) + " for max=" + rowUpperLimit + " with caching=" + rows);
      }
      // Run the scan
      scanner = metaTable.getScanner(scan);
      Result result;
      int processedRows = 0;
      while ((result = scanner.next()) != null) {
        if (visitor != null) {
          if (!visitor.processRow(result)) break;
        }
        processedRows++;
        if (processedRows >= rowUpperLimit) break;
      }
    } finally {
      if (scanner != null) {
        try {
          scanner.close();
        } catch (Throwable t) {
          ExceptionUtil.rethrowIfInterrupt(t);
          LOG.debug("Got exception in closing the result scanner", t);
        }
      }
      if (visitor != null) {
        try {
          visitor.close();
        } catch (Throwable t) {
          ExceptionUtil.rethrowIfInterrupt(t);
          LOG.debug("Got exception in closing the meta scanner visitor", t);
        }
      }
      if (metaTable != null) {
        try {
          metaTable.close();
        } catch (Throwable t) {
          ExceptionUtil.rethrowIfInterrupt(t);
          LOG.debug("Got exception in closing meta table", t);
        }
      }
      if (closeConnection) {
        connection.close();
      }
    }
  }

  /**
   * Returns HRegionInfo object from the column
   * HConstants.CATALOG_FAMILY:HConstants.REGIONINFO_QUALIFIER of the catalog
   * table Result.
   * @param data a Result object from the catalog table scan
   * @return HRegionInfo or null
   * @deprecated Use {@link org.apache.hadoop.hbase.MetaTableAccessor#getRegionLocations(Result)}
   */
  @Deprecated
  public static HRegionInfo getHRegionInfo(Result data) {
    return HRegionInfo.getHRegionInfo(data);
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
    MetaScannerVisitor visitor = new MetaScannerVisitorBase() {
        @Override
        public boolean processRow(Result result) throws IOException {
          if (result == null || result.isEmpty()) {
            return true;
          }

          RegionLocations locations = MetaTableAccessor.getRegionLocations(result);
          if (locations == null) return true;
          for (HRegionLocation loc : locations.getRegionLocations()) {
            if (loc != null) {
              HRegionInfo regionInfo = loc.getRegionInfo();
              // If region offline AND we are not to include offlined regions, return.
              if (regionInfo.isOffline() && !offlined) continue;
              regions.add(regionInfo);
            }
          }
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
   * @deprecated Use {@link #allTableRegions(Configuration, Connection, TableName)} instead
   */
  @Deprecated
  public static NavigableMap<HRegionInfo, ServerName> allTableRegions(Configuration conf,
      Connection connection, final TableName tableName, boolean offlined) throws IOException {
    return allTableRegions(conf, connection, tableName);
  }

  /**
   * Lists all of the table regions currently in META.
   * @param conf
   * @param connection
   * @param tableName
   * @return Map of all user-space regions to servers
   * @throws IOException
   */
  public static NavigableMap<HRegionInfo, ServerName> allTableRegions(Configuration conf,
      Connection connection, final TableName tableName) throws IOException {
    final NavigableMap<HRegionInfo, ServerName> regions =
      new TreeMap<HRegionInfo, ServerName>();
    MetaScannerVisitor visitor = new TableMetaScannerVisitor(tableName) {
      @Override
      public boolean processRowInternal(Result result) throws IOException {
        RegionLocations locations = MetaTableAccessor.getRegionLocations(result);
        if (locations == null) return true;
        for (HRegionLocation loc : locations.getRegionLocations()) {
          if (loc != null) {
            HRegionInfo regionInfo = loc.getRegionInfo();
            regions.put(new UnmodifyableHRegionInfo(regionInfo), loc.getServerName());
          }
        }
        return true;
      }
    };
    metaScan(conf, connection, visitor, tableName);
    return regions;
  }

  /**
   * Lists table regions and locations grouped by region range from META.
   */
  public static List<RegionLocations> listTableRegionLocations(Configuration conf,
      Connection connection, final TableName tableName) throws IOException {
    final List<RegionLocations> regions = new ArrayList<RegionLocations>();
    MetaScannerVisitor visitor = new TableMetaScannerVisitor(tableName) {
      @Override
      public boolean processRowInternal(Result result) throws IOException {
        RegionLocations locations = MetaTableAccessor.getRegionLocations(result);
        if (locations == null) return true;
        regions.add(locations);
        return true;
      }
    };
    metaScan(conf, connection, visitor, tableName);
    return regions;
  }

  /**
   * Visitor class called to process each row of the hbase:meta table
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
    boolean processRow(Result rowResult) throws IOException;
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
   * hbase:meta entries during concurrent splits (see HBASE-5986 for details). This class
   * does not guarantee ordered traversal of meta entries, and can block until the
   * hbase:meta entries for daughters are available during splits.
   */
  public static abstract class TableMetaScannerVisitor extends DefaultMetaScannerVisitor {
    private TableName tableName;

    public TableMetaScannerVisitor(TableName tableName) {
      super();
      this.tableName = tableName;
    }

    @Override
    public final boolean processRow(Result rowResult) throws IOException {
      HRegionInfo info = getHRegionInfo(rowResult);
      if (info == null) {
        return true;
      }
      if (!(info.getTable().equals(tableName))) {
        return false;
      }
      return super.processRow(rowResult);
    }
  }
}
