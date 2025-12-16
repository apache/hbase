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
package org.apache.hadoop.hbase.regionserver.wal;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MetaTableName;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.SingleProcessHBaseCluster;
import org.apache.hadoop.hbase.StartTestingClusterOption;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.wal.AbstractFSWALProvider;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.apache.hadoop.hbase.wal.WALProvider;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * Test log deletion as logs are rolled.
 */
public abstract class AbstractTestLogRolling {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractTestLogRolling.class);
  protected HRegionServer server;
  protected String tableName;
  protected byte[] value;
  protected FileSystem fs;
  protected MiniDFSCluster dfsCluster;
  protected Admin admin;
  protected SingleProcessHBaseCluster cluster;
  protected static final HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();
  @Rule
  public final TestName name = new TestName();
  protected static int syncLatencyMillis;
  private static int rowNum = 1;
  private static final AtomicBoolean slowSyncHookCalled = new AtomicBoolean();
  protected static ScheduledExecutorService EXECUTOR;

  public AbstractTestLogRolling() {
    this.server = null;
    this.tableName = null;

    String className = this.getClass().getName();
    StringBuilder v = new StringBuilder(className);
    while (v.length() < 1000) {
      v.append(className);
    }
    this.value = Bytes.toBytes(v.toString());
  }

  // Need to override this setup so we can edit the config before it gets sent
  // to the HDFS & HBase cluster startup.
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    /**** configuration for testLogRolling ****/
    // Force a region split after every 768KB
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setLong(HConstants.HREGION_MAX_FILESIZE, 768L * 1024L);

    // We roll the log after every 32 writes
    conf.setInt("hbase.regionserver.maxlogentries", 32);

    conf.setInt("hbase.regionserver.logroll.errors.tolerated", 2);
    conf.setInt("hbase.rpc.timeout", 10 * 1000);

    // For less frequently updated regions flush after every 2 flushes
    conf.setInt("hbase.hregion.memstore.optionalflushcount", 2);

    // We flush the cache after every 8192 bytes
    conf.setInt(HConstants.HREGION_MEMSTORE_FLUSH_SIZE, 8192);

    // Increase the amount of time between client retries
    conf.setLong("hbase.client.pause", 10 * 1000);

    // Reduce thread wake frequency so that other threads can get
    // a chance to run.
    conf.setInt(HConstants.THREAD_WAKE_FREQUENCY, 2 * 1000);

    // disable low replication check for log roller to get a more stable result
    // TestWALOpenAfterDNRollingStart will test this option.
    conf.setLong("hbase.regionserver.hlog.check.lowreplication.interval", 24L * 60 * 60 * 1000);

    // For slow sync threshold test: roll after 5 slow syncs in 10 seconds
    conf.setInt(FSHLog.SLOW_SYNC_ROLL_THRESHOLD, 5);
    conf.setInt(FSHLog.SLOW_SYNC_ROLL_INTERVAL_MS, 10 * 1000);
    // For slow sync threshold test: roll once after a sync above this threshold
    conf.setInt(FSHLog.ROLL_ON_SYNC_TIME_MS, 5000);

    // Slow sync executor.
    EXECUTOR = Executors
      .newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setNameFormat("Slow-sync-%d")
        .setDaemon(true).setUncaughtExceptionHandler(Threads.LOGGING_EXCEPTION_HANDLER).build());
  }

  @Before
  public void setUp() throws Exception {
    // Use 2 DataNodes and default values for other StartMiniCluster options.
    TEST_UTIL.startMiniCluster(StartTestingClusterOption.builder().numDataNodes(2).build());

    cluster = TEST_UTIL.getHBaseCluster();
    dfsCluster = TEST_UTIL.getDFSCluster();
    fs = TEST_UTIL.getTestFileSystem();
    admin = TEST_UTIL.getAdmin();

    // disable region rebalancing (interferes with log watching)
    cluster.getMaster().balanceSwitch(false);
  }

  @After
  public void tearDown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @AfterClass
  public static void tearDownAfterClass() {
    EXECUTOR.shutdownNow();
  }

  private void startAndWriteData() throws IOException, InterruptedException {
    this.server = cluster.getRegionServerThreads().get(0).getRegionServer();

    Table table = createTestTable(this.tableName);

    server = TEST_UTIL.getRSForFirstRegionInTable(table.getName());
    for (int i = 1; i <= 256; i++) { // 256 writes should cause 8 log rolls
      doPut(table, i);
      if (i % 32 == 0) {
        // After every 32 writes sleep to let the log roller run
        try {
          Thread.sleep(2000);
        } catch (InterruptedException e) {
          // continue
        }
      }
    }
  }

  private static void setSyncLatencyMillis(int latency) {
    syncLatencyMillis = latency;
  }

  protected final AbstractFSWAL<?> getWALAndRegisterSlowSyncHook(RegionInfo region)
    throws IOException {
    // Get a reference to the wal.
    final AbstractFSWAL<?> log = (AbstractFSWAL<?>) server.getWAL(region);

    // Register a WALActionsListener to observe if a SLOW_SYNC roll is requested
    log.registerWALActionsListener(new WALActionsListener() {
      @Override
      public void logRollRequested(RollRequestReason reason) {
        switch (reason) {
          case SLOW_SYNC:
            slowSyncHookCalled.lazySet(true);
            break;
          default:
            break;
        }
      }
    });
    return log;
  }

  protected final void checkSlowSync(AbstractFSWAL<?> log, Table table, int slowSyncLatency,
    int writeCount, boolean slowSync) throws Exception {
    if (slowSyncLatency > 0) {
      setSyncLatencyMillis(slowSyncLatency);
      setSlowLogWriter(log.conf);
    } else {
      setDefaultLogWriter(log.conf);
    }

    // Set up for test
    log.rollWriter(true);
    slowSyncHookCalled.set(false);

    final WALProvider.WriterBase oldWriter = log.getWriter();

    // Write some data
    for (int i = 0; i < writeCount; i++) {
      writeData(table, rowNum++);
    }

    if (slowSync) {
      TEST_UTIL.waitFor(10000, 100, new Waiter.ExplainingPredicate<Exception>() {
        @Override
        public boolean evaluate() throws Exception {
          return log.getWriter() != oldWriter;
        }

        @Override
        public String explainFailure() throws Exception {
          return "Waited too long for our test writer to get rolled out";
        }
      });

      assertTrue("Should have triggered log roll due to SLOW_SYNC", slowSyncHookCalled.get());
    } else {
      assertFalse("Should not have triggered log roll due to SLOW_SYNC", slowSyncHookCalled.get());
    }
  }

  protected abstract void setSlowLogWriter(Configuration conf);

  protected abstract void setDefaultLogWriter(Configuration conf);

  /**
   * Tests that log rolling doesn't hang when no data is written.
   */
  @Test
  public void testLogRollOnNothingWritten() throws Exception {
    final Configuration conf = TEST_UTIL.getConfiguration();
    final WALFactory wals =
      new WALFactory(conf, ServerName.valueOf("test.com", 8080, 1).toString());
    final WAL newLog = wals.getWAL(null);
    try {
      // Now roll the log before we write anything.
      newLog.rollWriter(true);
    } finally {
      wals.close();
    }
  }

  /**
   * Tests that logs are deleted
   */
  @Test
  public void testLogRolling() throws Exception {
    this.tableName = getName();
    // TODO: Why does this write data take for ever?
    startAndWriteData();
    RegionInfo region = server.getRegions(TableName.valueOf(tableName)).get(0).getRegionInfo();
    final WAL log = server.getWAL(region);
    LOG.info(
      "after writing there are " + AbstractFSWALProvider.getNumRolledLogFiles(log) + " log files");

    // roll the log, so we should have at least one rolled file and the log file size should be
    // greater than 0, in case in the above method we rolled in the last round and then flushed so
    // all the old wal files are deleted and cause the below assertion to fail
    log.rollWriter();

    assertThat(AbstractFSWALProvider.getLogFileSize(log), greaterThan(0L));

    // flush all regions
    for (HRegion r : server.getOnlineRegionsLocalContext()) {
      r.flush(true);
    }

    // Now roll the log the again
    log.rollWriter();

    // should have deleted all the rolled wal files
    TEST_UTIL.waitFor(5000, () -> AbstractFSWALProvider.getNumRolledLogFiles(log) == 0);
    assertEquals(0, AbstractFSWALProvider.getLogFileSize(log));
  }

  protected String getName() {
    return "TestLogRolling-" + name.getMethodName();
  }

  void writeData(Table table, int rownum) throws IOException {
    doPut(table, rownum);

    // sleep to let the log roller run (if it needs to)
    try {
      Thread.sleep(2000);
    } catch (InterruptedException e) {
      // continue
    }
  }

  void validateData(Table table, int rownum) throws IOException {
    String row = "row" + String.format("%1$04d", rownum);
    Get get = new Get(Bytes.toBytes(row));
    get.addFamily(HConstants.CATALOG_FAMILY);
    Result result = table.get(get);
    assertTrue(result.size() == 1);
    assertTrue(Bytes.equals(value, result.getValue(HConstants.CATALOG_FAMILY, null)));
    LOG.info("Validated row " + row);
  }

  /**
   * Tests that logs are deleted when some region has a compaction record in WAL and no other
   * records. See HBASE-8597.
   */
  @Test
  public void testCompactionRecordDoesntBlockRolling() throws Exception {

    // When the hbase:meta table can be opened, the region servers are running
    try (Table t = TEST_UTIL.getConnection().getTable(MetaTableName.getInstance());
      Table table = createTestTable(getName())) {

      server = TEST_UTIL.getRSForFirstRegionInTable(table.getName());
      HRegion region = server.getRegions(table.getName()).get(0);
      final WAL log = server.getWAL(region.getRegionInfo());
      Store s = region.getStore(HConstants.CATALOG_FAMILY);

      // Put some stuff into table, to make sure we have some files to compact.
      for (int i = 1; i <= 2; ++i) {
        doPut(table, i);
        admin.flush(table.getName());
      }
      doPut(table, 3); // don't flush yet, or compaction might trigger before we roll WAL
      assertEquals("Should have no WAL after initial writes", 0,
        AbstractFSWALProvider.getNumRolledLogFiles(log));
      assertEquals(2, s.getStorefilesCount());

      // Roll the log and compact table, to have compaction record in the 2nd WAL.
      log.rollWriter();
      assertEquals("Should have WAL; one table is not flushed", 1,
        AbstractFSWALProvider.getNumRolledLogFiles(log));
      admin.flush(table.getName());
      region.compact(false);
      // Wait for compaction in case if flush triggered it before us.
      Assert.assertNotNull(s);
      for (int waitTime = 3000; s.getStorefilesCount() > 1 && waitTime > 0; waitTime -= 200) {
        Threads.sleepWithoutInterrupt(200);
      }
      assertEquals("Compaction didn't happen", 1, s.getStorefilesCount());

      // Write some value to the table so the WAL cannot be deleted until table is flushed.
      doPut(table, 0); // Now 2nd WAL will have both compaction and put record for table.
      log.rollWriter(); // 1st WAL deleted, 2nd not deleted yet.
      assertEquals("Should have WAL; one table is not flushed", 1,
        AbstractFSWALProvider.getNumRolledLogFiles(log));

      // Flush table to make latest WAL obsolete; write another record, and roll again.
      admin.flush(table.getName());
      doPut(table, 1);
      log.rollWriter(); // Now 2nd WAL is deleted and 3rd is added.
      assertEquals("Should have 1 WALs at the end", 1,
        AbstractFSWALProvider.getNumRolledLogFiles(log));
    }
  }

  protected void doPut(Table table, int i) throws IOException {
    Put put = new Put(Bytes.toBytes("row" + String.format("%1$04d", i)));
    put.addColumn(HConstants.CATALOG_FAMILY, null, value);
    table.put(put);
  }

  protected Table createTestTable(String tableName) throws IOException {
    // Create the test table and open it
    TableDescriptor desc = TableDescriptorBuilder.newBuilder(TableName.valueOf(getName()))
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(HConstants.CATALOG_FAMILY)).build();
    admin.createTable(desc);
    return TEST_UTIL.getConnection().getTable(desc.getTableName());
  }
}
