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

package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.LargeTests;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.HFileLink;
import org.apache.hadoop.hbase.util.JVMClusterUtil.RegionServerThread;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.FSVisitor;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Category(LargeTests.class)
public class TestCorruptedRegionStoreFile {
  private static final Log LOG = LogFactory.getLog(TestCorruptedRegionStoreFile.class);

  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  private static final String FAMILY_NAME_STR = "f";
  private static final byte[] FAMILY_NAME = Bytes.toBytes(FAMILY_NAME_STR);

  private static final int NUM_FILES = 25;
  private static final int ROW_PER_FILE = 2000;
  private static final int NUM_ROWS = NUM_FILES * ROW_PER_FILE;

  private final ArrayList<Path> storeFiles = new ArrayList<Path>();
  private Path tableDir;
  private int rowCount;

  private static void setupConf(Configuration conf) {
    conf.setLong("hbase.hstore.compaction.min", 20);
    conf.setLong("hbase.hstore.compaction.max", 39);
    conf.setLong("hbase.hstore.blockingStoreFiles", 40);
  }

  private void setupTable(final String tableName) throws Exception {
    // load the table
    HTable table = UTIL.createTable(Bytes.toBytes(tableName), FAMILY_NAME);
    try {
      rowCount = 0;
      byte[] value = new byte[1024];
      byte[] q = Bytes.toBytes("q");
      while (rowCount < NUM_ROWS) {
        Put put = new Put(Bytes.toBytes(String.format("%010d", rowCount)));
        put.setDurability(Durability.SKIP_WAL);
        put.add(FAMILY_NAME, q, value);
        table.put(put);

        if ((rowCount++ % ROW_PER_FILE) == 0) {
          // flush it
          UTIL.getHBaseAdmin().flush(tableName);
        }
      }
    } finally {
      UTIL.getHBaseAdmin().flush(tableName);
      table.close();
    }

    assertEquals(NUM_ROWS, rowCount);

    // get the store file paths
    storeFiles.clear();
    tableDir = FSUtils.getTablePath(getRootDir(), tableName);
    FSVisitor.visitTableStoreFiles(getFileSystem(), tableDir, new FSVisitor.StoreFileVisitor() {
      @Override
      public void storeFile(final String region, final String family, final String hfile)
          throws IOException {
        HFileLink link = HFileLink.create(UTIL.getConfiguration(), tableName, region, family, hfile);
        storeFiles.add(link.getOriginPath());
      }
    });
    assertTrue("expected at least 1 store file", storeFiles.size() > 0);
    LOG.info("store-files: " + storeFiles);
  }

  @Before
  public void setup() throws Exception {
    setupConf(UTIL.getConfiguration());
    UTIL.startMiniCluster(2, 3);
  }

  @After
  public void tearDown() throws Exception {
    try {
      UTIL.shutdownMiniCluster();
    } catch (Exception e) {
      LOG.warn("failure shutting down cluster", e);
    }
  }

  @Test(timeout=90000)
  public void testLosingFileDuringScan() throws Exception {
    final String tableName = "testLosingFileDuringScan";
    setupTable(tableName);
    assertEquals(rowCount, fullScanAndCount(tableName));

    final FileSystem fs = getFileSystem();
    final Path tmpStoreFilePath = new Path(UTIL.getDataTestDir(), "corruptedHFile");

    // try to query with the missing file
    int count = fullScanAndCount(tableName, new ScanInjector() {
      private boolean hasFile = true;

      @Override
      public void beforeScanNext(HTable table) throws Exception {
        // move the path away (now the region is corrupted)
        if (hasFile) {
          fs.copyToLocalFile(true, storeFiles.get(0), tmpStoreFilePath);
          LOG.info("Move file to local");
          evictHFileCache(storeFiles.get(0));
          hasFile = false;
        }
      }
    });
    assertTrue("expected one file lost: rowCount=" + count, count >= (NUM_ROWS - ROW_PER_FILE));
  }

  @Test(timeout=90000)
  public void testLosingFileAfterScannerInit() throws Exception {
    final String tableName = "testLosingFileAfterScannerInit";
    setupTable(tableName);
    assertEquals(rowCount, fullScanAndCount(tableName));

    final FileSystem fs = getFileSystem();
    final Path tmpStoreFilePath = new Path(UTIL.getDataTestDir(), "corruptedHFile");

    // try to query with the missing file
    int count = fullScanAndCount(tableName, new ScanInjector() {
      private boolean hasFile = true;

      @Override
      public void beforeScan(HTable table, Scan scan) throws Exception {
        // move the path away (now the region is corrupted)
        if (hasFile) {
          fs.copyToLocalFile(true, storeFiles.get(0), tmpStoreFilePath);
          LOG.info("Move file to local");
          evictHFileCache(storeFiles.get(0));
          hasFile = false;
        }
      }
    });
    assertTrue("expected one file lost: rowCount=" + count, count >= (NUM_ROWS - ROW_PER_FILE));
  }

  // ==========================================================================
  //  Helpers
  // ==========================================================================
  private FileSystem getFileSystem() {
    return UTIL.getHBaseCluster().getMaster().getMasterFileSystem().getFileSystem();
  }

  private Path getRootDir() {
    return UTIL.getHBaseCluster().getMaster().getMasterFileSystem().getRootDir();
  }

  private void evictHFileCache(final Path hfile) throws Exception {
    for (RegionServerThread rst: UTIL.getMiniHBaseCluster().getRegionServerThreads()) {
      HRegionServer rs = rst.getRegionServer();
      rs.getCacheConfig().getBlockCache().evictBlocksByHfileName(hfile.getName());
    }
    Thread.sleep(6000);
  }

  private int fullScanAndCount(final String tableName) throws Exception {
    return fullScanAndCount(tableName, new ScanInjector());
  }

  private int fullScanAndCount(final String tableName, final ScanInjector injector)
      throws Exception {
    HTable table = new HTable(UTIL.getConfiguration(), tableName);
    int count = 0;
    try {
      Scan scan = new Scan();
      scan.setCaching(1);
      scan.setCacheBlocks(false);
      injector.beforeScan(table, scan);
      ResultScanner scanner = table.getScanner(scan);
      try {
        while (true) {
          injector.beforeScanNext(table);
          Result result = scanner.next();
          injector.afterScanNext(table, result);
          if (result == null) break;
          if ((count++ % 1000) == 0) {
            LOG.debug("scan next " + count);
          }
        }
      } finally {
        scanner.close();
        injector.afterScan(table);
      }
    } finally {
      table.close();
    }
    return count;
  }

  private class ScanInjector {
    protected void beforeScan(HTable table, Scan scan) throws Exception {}
    protected void beforeScanNext(HTable table) throws Exception {}
    protected void afterScanNext(HTable table, Result result) throws Exception {}
    protected void afterScan(HTable table) throws Exception {}
  }
}
