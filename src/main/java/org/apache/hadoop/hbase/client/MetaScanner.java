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

package org.apache.hadoop.hbase.client;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.errorhandling.TimeoutException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Writables;

/**
 * Scanner class that contains the <code>.META.</code> table scanning logic
 * and uses a Retryable scanner. Provided visitors will be called
 * for each row.
 *
 * Although public visibility, this is not a public-facing API and may evolve in
 * minor releases.
 *
 * <p> Note that during concurrent region splits, the scanner might not see
 * META changes across rows (for parent and daughter entries) consistently.
 * see HBASE-5986, and {@link BlockingMetaScannerVisitor} for details. </p>
 */
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
    metaScan(configuration, null, visitor, null);
  }

  /**
   * Scans the meta table and calls a visitor on each RowResult and uses a empty
   * start row value as table name.
   *
   * @param configuration conf
   * @param connection connection to be used internally (null not allowed)
   * @param visitor A custom visitor
   * @throws IOException e
   */
  public static void metaScan(Configuration configuration, HConnection connection,
      MetaScannerVisitor visitor, byte [] userTableName)
  throws IOException {
    metaScan(configuration, connection, visitor, userTableName, null, Integer.MAX_VALUE,
        HConstants.META_TABLE_NAME);
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
    metaScan(configuration, null, visitor, userTableName, row, rowLimit,
      HConstants.META_TABLE_NAME);
  }

  /**
   * Scans the meta table and calls a visitor on each RowResult. Uses a table
   * name and a row name to locate meta regions. And it only scans at most
   * <code>rowLimit</code> of rows.
   *
   * @param configuration HBase configuration.
   * @param connection connection to be used internally (null not allowed)
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
  public static void metaScan(Configuration configuration, HConnection connection,
      MetaScannerVisitor visitor, byte [] tableName, byte[] row,
      int rowLimit, final byte [] metaTableName)
  throws IOException {
    HTable metaTable = null;
    try {
      if (connection == null) {
        metaTable = new HTable(configuration, HConstants.META_TABLE_NAME, null);
      } else {
        metaTable = new HTable(HConstants.META_TABLE_NAME, connection, null);
      }
      int rowUpperLimit = rowLimit > 0 ? rowLimit: Integer.MAX_VALUE;

      // if row is not null, we want to use the startKey of the row's region as
      // the startRow for the meta scan.
      byte[] startRow;
      if (row != null) {
        // Scan starting at a particular row in a particular table
        assert tableName != null;
        byte[] searchRow =
          HRegionInfo.createRegionName(tableName, row, HConstants.NINES,
            false);
        Result startRowResult = metaTable.getRowOrBefore(searchRow,
            HConstants.CATALOG_FAMILY);
        if (startRowResult == null) {
          throw new TableNotFoundException("Cannot find row in .META. for table: "
              + Bytes.toString(tableName) + ", row=" + Bytes.toStringBinary(searchRow));
        }
        byte[] value = startRowResult.getValue(HConstants.CATALOG_FAMILY,
            HConstants.REGIONINFO_QUALIFIER);
        if (value == null || value.length == 0) {
          throw new IOException("HRegionInfo was null or empty in Meta for " +
            Bytes.toString(tableName) + ", row=" + Bytes.toStringBinary(searchRow));
        }
        HRegionInfo regionInfo = Writables.getHRegionInfo(value);

        byte[] rowBefore = regionInfo.getStartKey();
        startRow = HRegionInfo.createRegionName(tableName, rowBefore,
            HConstants.ZEROES, false);
      } else if (tableName == null || tableName.length == 0) {
        // Full META scan
        startRow = HConstants.EMPTY_START_ROW;
      } else {
        // Scan META for an entire table
        startRow = HRegionInfo.createRegionName(
            tableName, HConstants.EMPTY_START_ROW, HConstants.ZEROES, false);
      }

      // Scan over each meta region
      ScannerCallable callable;
      int rows = Math.min(rowLimit, configuration.getInt(
          HConstants.HBASE_META_SCANNER_CACHING,
          HConstants.DEFAULT_HBASE_META_SCANNER_CACHING));
      do {
        final Scan scan = new Scan(startRow).addFamily(HConstants.CATALOG_FAMILY);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Scanning " + Bytes.toString(metaTableName) +
            " starting at row=" + Bytes.toStringBinary(startRow) + " for max=" +
            rowUpperLimit + " rows using " + metaTable.getConnection().toString());
        }
        callable = new ScannerCallable(metaTable.getConnection(), metaTableName, scan, null);
        // Open scanner
        callable.withRetries();

        int processedRows = 0;
        try {
          callable.setCaching(rows);
          done: do {
            if (processedRows >= rowUpperLimit) {
              break;
            }
            //we have all the rows here
            Result [] rrs = callable.withRetries();
            if (rrs == null || rrs.length == 0 || rrs[0].size() == 0) {
              break; //exit completely
            }
            for (Result rr : rrs) {
              if (processedRows >= rowUpperLimit) {
                break done;
              }
              if (!visitor.processRow(rr))
                break done; //exit completely
              processedRows++;
            }
            //here, we didn't break anywhere. Check if we have more rows
          } while(true);
          // Advance the startRow to the end key of the current region
          startRow = callable.getHRegionInfo().getEndKey();
        } finally {
          // Close scanner
          callable.setClose();
          callable.withRetries();
        }
      } while (Bytes.compareTo(startRow, HConstants.LAST_ROW) != 0);
    } finally {
      visitor.close();
      if (metaTable != null) {
        metaTable.close();
      }
    }
  }

  /**
   * Used in tests.
   *
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
    MetaScannerVisitor visitor = new BlockingMetaScannerVisitor(conf) {
        @Override
        public boolean processRowInternal(Result result) throws IOException {
          if (result == null || result.isEmpty()) {
            return true;
          }
          byte [] bytes = result.getValue(HConstants.CATALOG_FAMILY,
            HConstants.REGIONINFO_QUALIFIER);
          if (bytes == null) {
            LOG.warn("Null REGIONINFO_QUALIFIER: " + result);
            return true;
          }
          HRegionInfo regionInfo = Writables.getHRegionInfo(bytes);
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
   * @param connection connection to be used internally (null to create a new connection)
   * @param offlined True if we are to include offlined regions, false and we'll
   * leave out offlined regions from returned list.
   * @return Map of all user-space regions to servers
   * @throws IOException
   * @Deprecated Use {@link #allTableRegions(Configuration, HConnection, byte[], boolean)}
   * instead
   */
  @Deprecated
  public static NavigableMap<HRegionInfo, ServerName> allTableRegions(Configuration conf,
      final byte[] tablename, final boolean offlined) throws IOException {
    return allTableRegions(conf, null, tablename, offlined);
  }
  /**
   * Lists all of the table regions currently in META.
   * @param conf
   * @param connection connection to be used internally (null to create a new connection)
   * @param offlined True if we are to include offlined regions, false and we'll
   * leave out offlined regions from returned list.
   * @return Map of all user-space regions to servers
   * @throws IOException
   */
  public static NavigableMap<HRegionInfo, ServerName> allTableRegions(Configuration conf,
      HConnection connection, final byte[] tablename, final boolean offlined) throws IOException {
    final NavigableMap<HRegionInfo, ServerName> regions =
      new TreeMap<HRegionInfo, ServerName>();
    MetaScannerVisitor visitor = new TableMetaScannerVisitor(conf, tablename) {
      @Override
      public boolean processRowInternal(Result rowResult) throws IOException {
        HRegionInfo info = Writables.getHRegionInfo(
            rowResult.getValue(HConstants.CATALOG_FAMILY,
                HConstants.REGIONINFO_QUALIFIER));
        byte [] value = rowResult.getValue(HConstants.CATALOG_FAMILY,
          HConstants.SERVER_QUALIFIER);
        String hostAndPort = null;
        if (value != null && value.length > 0) {
          hostAndPort = Bytes.toString(value);
        }
        value = rowResult.getValue(HConstants.CATALOG_FAMILY,
          HConstants.STARTCODE_QUALIFIER);
        long startcode = -1L;
        if (value != null && value.length > 0) startcode = Bytes.toLong(value);
        if (!(info.isOffline() || info.isSplit())) {
          ServerName sn = null;
          if (hostAndPort != null && hostAndPort.length() > 0) {
            sn = new ServerName(hostAndPort, startcode);
          }
          regions.put(new UnmodifyableHRegionInfo(info), sn);
        }
        return true;
      }
    };
    metaScan(conf, connection, visitor, tablename);
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
   * A MetaScannerVisitor that provides a consistent view of the table's
   * META entries during concurrent splits (see HBASE-5986 for details). This class
   * does not guarantee ordered traversal of meta entries, and can block until the
   * META entries for daughters are available during splits.
   */
  public static abstract class BlockingMetaScannerVisitor
    extends MetaScannerVisitorBase {

    private static final int DEFAULT_BLOCKING_TIMEOUT = 10000;
    private Configuration conf;
    private TreeSet<byte[]> daughterRegions = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);
    private int blockingTimeout;
    private HTable metaTable;

    public BlockingMetaScannerVisitor(Configuration conf) {
      this.conf = conf;
      this.blockingTimeout = conf.getInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT,
          DEFAULT_BLOCKING_TIMEOUT);
    }

    public abstract boolean processRowInternal(Result rowResult) throws IOException;

    @Override
    public void close() throws IOException {
      super.close();
      if (metaTable != null) {
        metaTable.close();
        metaTable = null;
      }
    }

    public HTable getMetaTable() throws IOException {
      if (metaTable == null) {
        metaTable = new HTable(conf, HConstants.META_TABLE_NAME);
      }
      return metaTable;
    }

    @Override
    public boolean processRow(Result rowResult) throws IOException {
      HRegionInfo info = Writables.getHRegionInfoOrNull(
          rowResult.getValue(HConstants.CATALOG_FAMILY,
              HConstants.REGIONINFO_QUALIFIER));
      if (info == null) {
        return true;
      }

      if (daughterRegions.remove(info.getRegionName())) {
        return true; //we have already processed this row
      }

      if (info.isSplitParent()) {
        /* we have found a parent region which was split. We have to ensure that it's daughters are
         * seen by this scanner as well, so we block until they are added to the META table. Even
         * though we are waiting for META entries, ACID semantics in HBase indicates that this
         * scanner might not see the new rows. So we manually query the daughter rows */
        HRegionInfo splitA = Writables.getHRegionInfoOrNull(rowResult.getValue(HConstants.CATALOG_FAMILY,
            HConstants.SPLITA_QUALIFIER));
        HRegionInfo splitB = Writables.getHRegionInfoOrNull(rowResult.getValue(HConstants.CATALOG_FAMILY,
            HConstants.SPLITB_QUALIFIER));

        HTable metaTable = getMetaTable();
        long start = System.currentTimeMillis();
        if (splitA != null) {
          try {
            Result resultA = getRegionResultBlocking(metaTable, blockingTimeout,
              info.getRegionName(), splitA.getRegionName());
            if (resultA != null) {
              processRow(resultA);
              daughterRegions.add(splitA.getRegionName());
            }
            // else parent is gone, so skip this daughter
          } catch (TimeoutException e) {
            throw new RegionOfflineException("Split daughter region " +
                splitA.getRegionNameAsString() + " cannot be found in META. Parent:" +
                info.getRegionNameAsString());
          }
        }
        long rem = blockingTimeout - (System.currentTimeMillis() - start);

        if (splitB != null) {
          try {
            Result resultB = getRegionResultBlocking(metaTable, rem,
              info.getRegionName(), splitB.getRegionName());
            if (resultB != null) {
              processRow(resultB);
              daughterRegions.add(splitB.getRegionName());
            }
            // else parent is gone, so skip this daughter
          } catch (TimeoutException e) {
            throw new RegionOfflineException("Split daughter region " +
                splitB.getRegionNameAsString() + " cannot be found in META. Parent:" +
                info.getRegionNameAsString());
          }
        }
      }

      return processRowInternal(rowResult);
    }

    /**
     * Returns region Result by querying the META table for regionName. It will block until
     * the region is found in META. It will also check for parent in META to make sure that
     * if parent is deleted, we no longer have to wait, and should continue (HBASE-8590)
     * @return Result object is daughter is found, or null if parent is gone from META
     * @throws TimeoutException if timeout is reached
     */
    private Result getRegionResultBlocking(HTable metaTable, long timeout, byte[] parentRegionName, byte[] regionName)
        throws IOException, TimeoutException {
      boolean logged = false;
      long start = System.currentTimeMillis();
      while (System.currentTimeMillis() - start < timeout) {
        Get get = new Get(regionName);
        Result result = metaTable.get(get);
        HRegionInfo info = Writables.getHRegionInfoOrNull(
            result.getValue(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER));
        if (info != null) {
          return result;
        }

        // check whether parent is still there, if not it means we do not need to wait
        Get parentGet = new Get(parentRegionName);
        Result parentResult = metaTable.get(parentGet);
        HRegionInfo parentInfo = Writables.getHRegionInfoOrNull(
            parentResult.getValue(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER));
        if (parentInfo == null) {
          // this means that parent is no more (catalog janitor or somebody else deleted it)
          return null;
        }

        try {
          if (!logged) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("blocking until region is in META: " + Bytes.toStringBinary(regionName));
            }
            logged = true;
          }
          Thread.sleep(10);
        } catch (InterruptedException ex) {
          Thread.currentThread().interrupt();
          break;
        }
      }
      throw new TimeoutException("getRegionResultBlocking", start, System.currentTimeMillis(),
        timeout);
    }
  }

  /**
   * A MetaScannerVisitor for a table. Provides a consistent view of the table's
   * META entries during concurrent splits (see HBASE-5986 for details). This class
   * does not guarantee ordered traversal of meta entries, and can block until the
   * META entries for daughters are available during splits.
   */
  public static abstract class TableMetaScannerVisitor extends BlockingMetaScannerVisitor {
    private byte[] tableName;

    public TableMetaScannerVisitor(Configuration conf, byte[] tableName) {
      super(conf);
      this.tableName = tableName;
    }

    @Override
    public final boolean processRow(Result rowResult) throws IOException {
      HRegionInfo info = Writables.getHRegionInfoOrNull(
          rowResult.getValue(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER));
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
