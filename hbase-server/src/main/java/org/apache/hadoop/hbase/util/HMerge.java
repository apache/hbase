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
package org.apache.hadoop.hbase.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.RemoteExceptionHandler;
import org.apache.hadoop.hbase.TableNotDisabledException;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnectable;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.wal.WALFactory;

/**
 * A non-instantiable class that has a static method capable of compacting
 * a table by merging adjacent regions.
 */
@InterfaceAudience.Private
class HMerge {
  // TODO: Where is this class used?  How does it relate to Merge in same package?
  private static final Log LOG = LogFactory.getLog(HMerge.class);
  static final Random rand = new Random();

  /*
   * Not instantiable
   */
  private HMerge() {
    super();
  }

  /**
   * Scans the table and merges two adjacent regions if they are small. This
   * only happens when a lot of rows are deleted.
   *
   * When merging the hbase:meta region, the HBase instance must be offline.
   * When merging a normal table, the HBase instance must be online, but the
   * table must be disabled.
   *
   * @param conf        - configuration object for HBase
   * @param fs          - FileSystem where regions reside
   * @param tableName   - Table to be compacted
   * @throws IOException
   */
  public static void merge(Configuration conf, FileSystem fs,
    final TableName tableName)
  throws IOException {
    merge(conf, fs, tableName, true);
  }

  /**
   * Scans the table and merges two adjacent regions if they are small. This
   * only happens when a lot of rows are deleted.
   *
   * When merging the hbase:meta region, the HBase instance must be offline.
   * When merging a normal table, the HBase instance must be online, but the
   * table must be disabled.
   *
   * @param conf        - configuration object for HBase
   * @param fs          - FileSystem where regions reside
   * @param tableName   - Table to be compacted
   * @param testMasterRunning True if we are to verify master is down before
   * running merge
   * @throws IOException
   */
  public static void merge(Configuration conf, FileSystem fs,
    final TableName tableName, final boolean testMasterRunning)
  throws IOException {
    boolean masterIsRunning = false;
    if (testMasterRunning) {
      masterIsRunning = HConnectionManager
          .execute(new HConnectable<Boolean>(conf) {
            @Override
            public Boolean connect(HConnection connection) throws IOException {
              return connection.isMasterRunning();
            }
          });
    }
    if (tableName.equals(TableName.META_TABLE_NAME)) {
      if (masterIsRunning) {
        throw new IllegalStateException(
            "Can not compact hbase:meta table if instance is on-line");
      }
      // TODO reenable new OfflineMerger(conf, fs).process();
    } else {
      if(!masterIsRunning) {
        throw new IllegalStateException(
            "HBase instance must be running to merge a normal table");
      }
      Admin admin = new HBaseAdmin(conf);
      try {
        if (!admin.isTableDisabled(tableName)) {
          throw new TableNotDisabledException(tableName);
        }
      } finally {
        admin.close();
      }
      new OnlineMerger(conf, fs, tableName).process();
    }
  }

  private static abstract class Merger {
    protected final Configuration conf;
    protected final FileSystem fs;
    protected final Path rootDir;
    protected final HTableDescriptor htd;
    protected final WALFactory walFactory;
    private final long maxFilesize;


    protected Merger(Configuration conf, FileSystem fs, final TableName tableName)
    throws IOException {
      this.conf = conf;
      this.fs = fs;
      this.maxFilesize = conf.getLong(HConstants.HREGION_MAX_FILESIZE,
          HConstants.DEFAULT_MAX_FILE_SIZE);

      this.rootDir = FSUtils.getRootDir(conf);
      Path tabledir = FSUtils.getTableDir(this.rootDir, tableName);
      this.htd = FSTableDescriptors.getTableDescriptorFromFs(this.fs, tabledir);
      String logname = "merge_" + System.currentTimeMillis() + HConstants.HREGION_LOGDIR_NAME;

      final Configuration walConf = new Configuration(conf);
      FSUtils.setRootDir(walConf, tabledir);
      this.walFactory = new WALFactory(walConf, null, logname);
    }

    void process() throws IOException {
      try {
        for (HRegionInfo[] regionsToMerge = next();
            regionsToMerge != null;
            regionsToMerge = next()) {
          if (!merge(regionsToMerge)) {
            return;
          }
        }
      } finally {
        try {
          walFactory.close();
        } catch(IOException e) {
          LOG.error(e);
        }
      }
    }

    protected boolean merge(final HRegionInfo[] info) throws IOException {
      if (info.length < 2) {
        LOG.info("only one region - nothing to merge");
        return false;
      }

      HRegion currentRegion = null;
      long currentSize = 0;
      HRegion nextRegion = null;
      long nextSize = 0;
      for (int i = 0; i < info.length - 1; i++) {
        if (currentRegion == null) {
          currentRegion = HRegion.openHRegion(conf, fs, this.rootDir, info[i], this.htd,
              walFactory.getWAL(info[i].getEncodedNameAsBytes(),
                info[i].getTable().getNamespace()));
          currentSize = currentRegion.getLargestHStoreSize();
        }
        nextRegion = HRegion.openHRegion(conf, fs, this.rootDir, info[i + 1], this.htd,
            walFactory.getWAL(info[i + 1].getEncodedNameAsBytes(),
              info[i + 1].getTable().getNamespace()));
        nextSize = nextRegion.getLargestHStoreSize();

        if ((currentSize + nextSize) <= (maxFilesize / 2)) {
          // We merge two adjacent regions if their total size is less than
          // one half of the desired maximum size
          LOG.info("Merging regions " + currentRegion.getRegionInfo().getRegionNameAsString() +
            " and " + nextRegion.getRegionInfo().getRegionNameAsString());
          HRegion mergedRegion =
            HRegion.mergeAdjacent(currentRegion, nextRegion);
          updateMeta(currentRegion.getRegionInfo().getRegionName(),
            nextRegion.getRegionInfo().getRegionName(), mergedRegion);
          break;
        }
        LOG.info("not merging regions " +
          Bytes.toStringBinary(currentRegion.getRegionInfo().getRegionName()) +
            " and " + Bytes.toStringBinary(nextRegion.getRegionInfo().getRegionName()));
        currentRegion.close();
        currentRegion = nextRegion;
        currentSize = nextSize;
      }
      if(currentRegion != null) {
        currentRegion.close();
      }
      return true;
    }

    protected abstract HRegionInfo[] next() throws IOException;

    protected abstract void updateMeta(final byte [] oldRegion1,
      final byte [] oldRegion2, HRegion newRegion)
    throws IOException;

  }

  /** Instantiated to compact a normal user table */
  private static class OnlineMerger extends Merger {
    private final TableName tableName;
    private final Table table;
    private final ResultScanner metaScanner;
    private HRegionInfo latestRegion;

    OnlineMerger(Configuration conf, FileSystem fs,
      final TableName tableName)
    throws IOException {
      super(conf, fs, tableName);
      this.tableName = tableName;
      this.table = new HTable(conf, TableName.META_TABLE_NAME);
      this.metaScanner = table.getScanner(HConstants.CATALOG_FAMILY,
          HConstants.REGIONINFO_QUALIFIER);
      this.latestRegion = null;
    }

    private HRegionInfo nextRegion() throws IOException {
      try {
        Result results = getMetaRow();
        if (results == null) {
          return null;
        }
        HRegionInfo region = HRegionInfo.getHRegionInfo(results);
        if (region == null) {
          throw new NoSuchElementException("meta region entry missing " +
              Bytes.toString(HConstants.CATALOG_FAMILY) + ":" +
              Bytes.toString(HConstants.REGIONINFO_QUALIFIER));
        }
        if (!region.getTable().equals(this.tableName)) {
          return null;
        }
        return region;
      } catch (IOException e) {
        e = RemoteExceptionHandler.checkIOException(e);
        LOG.error("meta scanner error", e);
        metaScanner.close();
        throw e;
      }
    }

    /*
     * Check current row has a HRegionInfo.  Skip to next row if HRI is empty.
     * @return A Map of the row content else null if we are off the end.
     * @throws IOException
     */
    private Result getMetaRow() throws IOException {
      Result currentRow = metaScanner.next();
      boolean foundResult = false;
      while (currentRow != null) {
        LOG.info("Row: <" + Bytes.toStringBinary(currentRow.getRow()) + ">");
        byte[] regionInfoValue = currentRow.getValue(HConstants.CATALOG_FAMILY,
            HConstants.REGIONINFO_QUALIFIER);
        if (regionInfoValue == null || regionInfoValue.length == 0) {
          currentRow = metaScanner.next();
          continue;
        }
        HRegionInfo region = HRegionInfo.getHRegionInfo(currentRow);
        if (!region.getTable().equals(this.tableName)) {
          currentRow = metaScanner.next();
          continue;
        }
        foundResult = true;
        break;
      }
      return foundResult ? currentRow : null;
    }

    @Override
    protected HRegionInfo[] next() throws IOException {
      List<HRegionInfo> regions = new ArrayList<HRegionInfo>();
      if(latestRegion == null) {
        latestRegion = nextRegion();
      }
      if(latestRegion != null) {
        regions.add(latestRegion);
      }
      latestRegion = nextRegion();
      if(latestRegion != null) {
        regions.add(latestRegion);
      }
      return regions.toArray(new HRegionInfo[regions.size()]);
    }

    @Override
    protected void updateMeta(final byte [] oldRegion1,
        final byte [] oldRegion2,
      HRegion newRegion)
    throws IOException {
      byte[][] regionsToDelete = {oldRegion1, oldRegion2};
      for (int r = 0; r < regionsToDelete.length; r++) {
        if(Bytes.equals(regionsToDelete[r], latestRegion.getRegionName())) {
          latestRegion = null;
        }
        Delete delete = new Delete(regionsToDelete[r]);
        table.delete(delete);
        if(LOG.isDebugEnabled()) {
          LOG.debug("updated columns in row: " + Bytes.toStringBinary(regionsToDelete[r]));
        }
      }
      newRegion.getRegionInfo().setOffline(true);

      MetaTableAccessor.addRegionToMeta(table, newRegion.getRegionInfo());

      if(LOG.isDebugEnabled()) {
        LOG.debug("updated columns in row: "
            + Bytes.toStringBinary(newRegion.getRegionInfo().getRegionName()));
      }
    }
  }
}
