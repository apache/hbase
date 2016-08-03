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

import java.util.Collection;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.regionserver.throttle.CompactionThroughputControllerFactory;
import org.apache.hadoop.hbase.regionserver.throttle.NoLimitThroughputController;
import org.apache.hadoop.hbase.snapshot.SnapshotTestingUtils;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category(MediumTests.class)
public class TestCompactSplitThread {
  private static final Log LOG = LogFactory.getLog(TestCompactSplitThread.class);
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private final TableName tableName = TableName.valueOf(getClass().getSimpleName());
  private final byte[] family = Bytes.toBytes("f");
  private static final int NUM_RS = 1;
  private static final int blockingStoreFiles = 3;
  private static Path rootDir;
  private static FileSystem fs;



  /**
   * Setup the config for the cluster
   */
  @BeforeClass
  public static void setupCluster() throws Exception {
    setupConf(TEST_UTIL.getConfiguration());
    TEST_UTIL.startMiniCluster(NUM_RS);
    fs = TEST_UTIL.getDFSCluster().getFileSystem();
    rootDir = TEST_UTIL.getMiniHBaseCluster().getMaster().getMasterFileSystem().getRootDir();

  }

  private static void setupConf(Configuration conf) {
    // disable the ui
    conf.setInt("hbase.regionsever.info.port", -1);
    // so make sure we get a compaction when doing a load, but keep around some
    // files in the store
    conf.setInt("hbase.hstore.compaction.min", 2);
    conf.setInt("hbase.hstore.compactionThreshold", 5);
    // change the flush size to a small amount, regulating number of store files
    conf.setInt("hbase.hregion.memstore.flush.size", 25000);

    // block writes if we get to blockingStoreFiles store files
    conf.setInt("hbase.hstore.blockingStoreFiles", blockingStoreFiles);
    // Ensure no extra cleaners on by default (e.g. TimeToLiveHFileCleaner)
    conf.setInt(CompactSplitThread.LARGE_COMPACTION_THREADS, 3);
    conf.setInt(CompactSplitThread.SMALL_COMPACTION_THREADS, 4);
    conf.setInt(CompactSplitThread.SPLIT_THREADS, 5);
    conf.setInt(CompactSplitThread.MERGE_THREADS, 6);
  }

  @After
  public void tearDown() throws Exception {
    TEST_UTIL.deleteTable(tableName);
  }

  @AfterClass
  public static void cleanupTest() throws Exception {
    try {
      TEST_UTIL.shutdownMiniCluster();
    } catch (Exception e) {
      // NOOP;
    }
  }

  @Test
  public void testThreadPoolSizeTuning() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    Connection conn = ConnectionFactory.createConnection(conf);
    try {
      HTableDescriptor htd = new HTableDescriptor(tableName);
      htd.addFamily(new HColumnDescriptor(family));
      htd.setCompactionEnabled(false);
      TEST_UTIL.getHBaseAdmin().createTable(htd);
      TEST_UTIL.waitTableAvailable(tableName);
      HRegionServer regionServer = TEST_UTIL.getRSForFirstRegionInTable(tableName);

      // check initial configuration of thread pool sizes
      assertEquals(3, regionServer.compactSplitThread.getLargeCompactionThreadNum());
      assertEquals(4, regionServer.compactSplitThread.getSmallCompactionThreadNum());
      assertEquals(5, regionServer.compactSplitThread.getSplitThreadNum());
      assertEquals(6, regionServer.compactSplitThread.getMergeThreadNum());

      // change bigger configurations and do online update
      conf.setInt(CompactSplitThread.LARGE_COMPACTION_THREADS, 4);
      conf.setInt(CompactSplitThread.SMALL_COMPACTION_THREADS, 5);
      conf.setInt(CompactSplitThread.SPLIT_THREADS, 6);
      conf.setInt(CompactSplitThread.MERGE_THREADS, 7);
      try {
        regionServer.compactSplitThread.onConfigurationChange(conf);
      } catch (IllegalArgumentException iae) {
        Assert.fail("Update bigger configuration failed!");
      }

      // check again after online update
      assertEquals(4, regionServer.compactSplitThread.getLargeCompactionThreadNum());
      assertEquals(5, regionServer.compactSplitThread.getSmallCompactionThreadNum());
      assertEquals(6, regionServer.compactSplitThread.getSplitThreadNum());
      assertEquals(7, regionServer.compactSplitThread.getMergeThreadNum());

      // change smaller configurations and do online update
      conf.setInt(CompactSplitThread.LARGE_COMPACTION_THREADS, 2);
      conf.setInt(CompactSplitThread.SMALL_COMPACTION_THREADS, 3);
      conf.setInt(CompactSplitThread.SPLIT_THREADS, 4);
      conf.setInt(CompactSplitThread.MERGE_THREADS, 5);
      try {
        regionServer.compactSplitThread.onConfigurationChange(conf);
      } catch (IllegalArgumentException iae) {
        Assert.fail("Update smaller configuration failed!");
      }

      // check again after online update
      assertEquals(2, regionServer.compactSplitThread.getLargeCompactionThreadNum());
      assertEquals(3, regionServer.compactSplitThread.getSmallCompactionThreadNum());
      assertEquals(4, regionServer.compactSplitThread.getSplitThreadNum());
      assertEquals(5, regionServer.compactSplitThread.getMergeThreadNum());
    } finally {
      conn.close();
    }
  }

  @Test(timeout = 60000)
  public void testFlushWithTableCompactionDisabled() throws Exception {
    Admin admin = TEST_UTIL.getHBaseAdmin();

    HTableDescriptor htd = new HTableDescriptor(tableName);
    htd.setCompactionEnabled(false);
    TEST_UTIL.createTable(htd, new byte[][] { family }, null);

    // load the table
    for (int i = 0; i < blockingStoreFiles + 1; i ++) {
      TEST_UTIL.loadTable(TEST_UTIL.getConnection().getTable(tableName), family);
      TEST_UTIL.flush(tableName);
    }

    // Make sure that store file number is greater than blockingStoreFiles + 1
    Path tableDir = FSUtils.getTableDir(rootDir, tableName);
    Collection<String> hfiles =  SnapshotTestingUtils.listHFileNames(fs, tableDir);
    assert(hfiles.size() > blockingStoreFiles + 1);
  }
}
