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
package org.apache.hadoop.hbase.regionserver.wal;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.NavigableMap;
import java.util.TreeMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.log.HBaseMarkers;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.MultiVersionConcurrencyControl;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.wal.AbstractFSWALProvider;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.apache.hadoop.hbase.wal.WALKeyImpl;
import org.apache.hadoop.hbase.wal.WALSplitter;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests for conditions that should trigger RegionServer aborts when
 * rolling the current WAL fails.
 */
@Category({RegionServerTests.class, MediumTests.class})
public class TestLogRollAbort {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestLogRollAbort.class);

  private static final Logger LOG = LoggerFactory.getLogger(AbstractTestLogRolling.class);
  private static MiniDFSCluster dfsCluster;
  private static Admin admin;
  private static MiniHBaseCluster cluster;
  protected final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  /* For the split-then-roll test */
  private static final Path HBASEDIR = new Path("/hbase");
  private static final Path HBASELOGDIR = new Path("/hbaselog");
  private static final Path OLDLOGDIR = new Path(HBASELOGDIR, HConstants.HREGION_OLDLOGDIR_NAME);

  // Need to override this setup so we can edit the config before it gets sent
  // to the HDFS & HBase cluster startup.
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    // Tweak default timeout values down for faster recovery
    TEST_UTIL.getConfiguration().setInt(
        "hbase.regionserver.logroll.errors.tolerated", 2);
    TEST_UTIL.getConfiguration().setInt("hbase.rpc.timeout", 10 * 1000);

    // Increase the amount of time between client retries
    TEST_UTIL.getConfiguration().setLong("hbase.client.pause", 5 * 1000);

    // lower the namenode & datanode heartbeat so the namenode
    // quickly detects datanode failures
    TEST_UTIL.getConfiguration().setInt("dfs.namenode.heartbeat.recheck-interval", 5000);
    TEST_UTIL.getConfiguration().setInt("dfs.heartbeat.interval", 1);
    // the namenode might still try to choose the recently-dead datanode
    // for a pipeline, so try to a new pipeline multiple times
    TEST_UTIL.getConfiguration().setInt("dfs.client.block.write.retries", 10);
    TEST_UTIL.getConfiguration().set(WALFactory.WAL_PROVIDER, "filesystem");
  }

  private Configuration conf;
  private FileSystem fs;

  @Before
  public void setUp() throws Exception {
    TEST_UTIL.startMiniCluster(2);

    cluster = TEST_UTIL.getHBaseCluster();
    dfsCluster = TEST_UTIL.getDFSCluster();
    admin = TEST_UTIL.getAdmin();
    conf = TEST_UTIL.getConfiguration();
    fs = TEST_UTIL.getDFSCluster().getFileSystem();

    // disable region rebalancing (interferes with log watching)
    cluster.getMaster().balanceSwitch(false);
    FSUtils.setRootDir(conf, HBASEDIR);
    FSUtils.setWALRootDir(conf, HBASELOGDIR);
  }

  @After
  public void tearDown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  /**
   * Tests that RegionServer aborts if we hit an error closing the WAL when
   * there are unsynced WAL edits.  See HBASE-4282.
   */
  @Test
  public void testRSAbortWithUnflushedEdits() throws Exception {
    LOG.info("Starting testRSAbortWithUnflushedEdits()");

    // When the hbase:meta table can be opened, the region servers are running
    TEST_UTIL.getConnection().getTable(TableName.META_TABLE_NAME).close();

    // Create the test table and open it
    TableName tableName = TableName.valueOf(this.getClass().getSimpleName());
    TableDescriptor desc = TableDescriptorBuilder.newBuilder(tableName)
        .setColumnFamily(ColumnFamilyDescriptorBuilder.of(HConstants.CATALOG_FAMILY)).build();

    admin.createTable(desc);
    Table table = TEST_UTIL.getConnection().getTable(tableName);
    try {
      HRegionServer server = TEST_UTIL.getRSForFirstRegionInTable(tableName);
      WAL log = server.getWAL(null);

      Put p = new Put(Bytes.toBytes("row2001"));
      p.addColumn(HConstants.CATALOG_FAMILY, Bytes.toBytes("col"), Bytes.toBytes(2001));
      table.put(p);

      log.sync();

      p = new Put(Bytes.toBytes("row2002"));
      p.addColumn(HConstants.CATALOG_FAMILY, Bytes.toBytes("col"), Bytes.toBytes(2002));
      table.put(p);

      dfsCluster.restartDataNodes();
      LOG.info("Restarted datanodes");

      try {
        log.rollWriter(true);
      } catch (FailedLogCloseException flce) {
        // Expected exception.  We used to expect that there would be unsynced appends but this
        // not reliable now that sync plays a roll in wall rolling.  The above puts also now call
        // sync.
      } catch (Throwable t) {
        LOG.error(HBaseMarkers.FATAL, "FAILED TEST: Got wrong exception", t);
      }
    } finally {
      table.close();
    }
  }

  /**
   * Tests the case where a RegionServer enters a GC pause,
   * comes back online after the master declared it dead and started to split.
   * Want log rolling after a master split to fail. See HBASE-2312.
   */
  @Test
  public void testLogRollAfterSplitStart() throws IOException {
    LOG.info("Verify wal roll after split starts will fail.");
    String logName = ServerName.valueOf("testLogRollAfterSplitStart",
        16010, System.currentTimeMillis()).toString();
    Path thisTestsDir = new Path(HBASELOGDIR, AbstractFSWALProvider.getWALDirectoryName(logName));
    final WALFactory wals = new WALFactory(conf, logName);

    try {
      // put some entries in an WAL
      TableName tableName =
          TableName.valueOf(this.getClass().getName());
      RegionInfo regionInfo = RegionInfoBuilder.newBuilder(tableName).build();
      WAL log = wals.getWAL(regionInfo);
      MultiVersionConcurrencyControl mvcc = new MultiVersionConcurrencyControl(1);

      int total = 20;
      for (int i = 0; i < total; i++) {
        WALEdit kvs = new WALEdit();
        kvs.add(new KeyValue(Bytes.toBytes(i), tableName.getName(), tableName.getName()));
        NavigableMap<byte[], Integer> scopes = new TreeMap<>(Bytes.BYTES_COMPARATOR);
        scopes.put(Bytes.toBytes("column"), 0);
        log.appendData(regionInfo, new WALKeyImpl(regionInfo.getEncodedNameAsBytes(), tableName,
          System.currentTimeMillis(), mvcc, scopes), kvs);
      }
      // Send the data to HDFS datanodes and close the HDFS writer
      log.sync();
      ((AbstractFSWAL<?>) log).replaceWriter(((FSHLog)log).getOldPath(), null, null);

      // code taken from MasterFileSystem.getLogDirs(), which is called from
      // MasterFileSystem.splitLog() handles RS shutdowns (as observed by the splitting process)
      // rename the directory so a rogue RS doesn't create more WALs
      Path rsSplitDir = thisTestsDir.suffix(AbstractFSWALProvider.SPLITTING_EXT);
      if (!fs.rename(thisTestsDir, rsSplitDir)) {
        throw new IOException("Failed fs.rename for log split: " + thisTestsDir);
      }
      LOG.debug("Renamed region directory: " + rsSplitDir);

      LOG.debug("Processing the old log files.");
      WALSplitter.split(HBASELOGDIR, rsSplitDir, OLDLOGDIR, fs, conf, wals);

      LOG.debug("Trying to roll the WAL.");
      try {
        log.rollWriter();
        Assert.fail("rollWriter() did not throw any exception.");
      } catch (IOException ioe) {
        if (ioe.getCause() instanceof FileNotFoundException) {
          LOG.info("Got the expected exception: ", ioe.getCause());
        } else {
          Assert.fail("Unexpected exception: " + ioe);
        }
      }
    } finally {
      wals.close();
      if (fs.exists(thisTestsDir)) {
        fs.delete(thisTestsDir, true);
      }
    }
  }
}
