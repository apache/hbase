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
package org.apache.hadoop.hbase.regionserver.wal;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.wal.AbstractFSWALProvider;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

/**
 * Test log deletion as logs are rolled.
 */
public abstract class AbstractTestLogRolling  {
  private static final Log LOG = LogFactory.getLog(AbstractTestLogRolling.class);
  protected HRegionServer server;
  protected String tableName;
  protected byte[] value;
  protected FileSystem fs;
  protected MiniDFSCluster dfsCluster;
  protected Admin admin;
  protected MiniHBaseCluster cluster;
  protected static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  @Rule public final TestName name = new TestName();

  public AbstractTestLogRolling()  {
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
    TEST_UTIL.getConfiguration().setLong(HConstants.HREGION_MAX_FILESIZE, 768L * 1024L);

    // We roll the log after every 32 writes
    TEST_UTIL.getConfiguration().setInt("hbase.regionserver.maxlogentries", 32);

    TEST_UTIL.getConfiguration().setInt("hbase.regionserver.logroll.errors.tolerated", 2);
    TEST_UTIL.getConfiguration().setInt("hbase.rpc.timeout", 10 * 1000);

    // For less frequently updated regions flush after every 2 flushes
    TEST_UTIL.getConfiguration().setInt("hbase.hregion.memstore.optionalflushcount", 2);

    // We flush the cache after every 8192 bytes
    TEST_UTIL.getConfiguration().setInt(
        HConstants.HREGION_MEMSTORE_FLUSH_SIZE, 8192);

    // Increase the amount of time between client retries
    TEST_UTIL.getConfiguration().setLong("hbase.client.pause", 10 * 1000);

    // Reduce thread wake frequency so that other threads can get
    // a chance to run.
    TEST_UTIL.getConfiguration().setInt(HConstants.THREAD_WAKE_FREQUENCY, 2 * 1000);
  }

  @Before
  public void setUp() throws Exception {
    TEST_UTIL.startMiniCluster(1, 1, 2);

    cluster = TEST_UTIL.getHBaseCluster();
    dfsCluster = TEST_UTIL.getDFSCluster();
    fs = TEST_UTIL.getTestFileSystem();
    admin = TEST_UTIL.getHBaseAdmin();

    // disable region rebalancing (interferes with log watching)
    cluster.getMaster().balanceSwitch(false);
  }

  @After
  public void tearDown() throws Exception  {
    TEST_UTIL.shutdownMiniCluster();
  }

  protected void startAndWriteData() throws IOException, InterruptedException {
    // When the hbase:meta table can be opened, the region servers are running
    TEST_UTIL.getConnection().getTable(TableName.META_TABLE_NAME);
    this.server = cluster.getRegionServerThreads().get(0).getRegionServer();

    Table table = createTestTable(this.tableName);

    server = TEST_UTIL.getRSForFirstRegionInTable(table.getName());
    for (int i = 1; i <= 256; i++) {    // 256 writes should cause 8 log rolls
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

  /**
   * Tests that log rolling doesn't hang when no data is written.
   */
  @Test(timeout=120000)
  public void testLogRollOnNothingWritten() throws Exception {
    final Configuration conf = TEST_UTIL.getConfiguration();
    final WALFactory wals = new WALFactory(conf, null,
        ServerName.valueOf("test.com",8080, 1).toString());
    final WAL newLog = wals.getWAL(new byte[]{}, null);
    try {
      // Now roll the log before we write anything.
      newLog.rollWriter(true);
    } finally {
      wals.close();
    }
  }

  private void assertLogFileSize(WAL log) {
    if (AbstractFSWALProvider.getNumRolledLogFiles(log) > 0) {
      assertTrue(AbstractFSWALProvider.getLogFileSize(log) > 0);
    } else {
      assertEquals(0, AbstractFSWALProvider.getLogFileSize(log));
    }
  }

  /**
   * Tests that logs are deleted
   * @throws IOException
   * @throws org.apache.hadoop.hbase.regionserver.wal.FailedLogCloseException
   */
  @Test
  public void testLogRolling() throws Exception {
    this.tableName = getName();
    // TODO: Why does this write data take for ever?
    startAndWriteData();
    HRegionInfo region = server.getOnlineRegions(TableName.valueOf(tableName)).get(0)
        .getRegionInfo();
    final WAL log = server.getWAL(region);
    LOG.info("after writing there are " + AbstractFSWALProvider.getNumRolledLogFiles(log) + " log files");
    assertLogFileSize(log);

    // flush all regions
    for (Region r : server.getOnlineRegionsLocalContext()) {
      r.flush(true);
    }

    // Now roll the log
    log.rollWriter();

    int count = AbstractFSWALProvider.getNumRolledLogFiles(log);
    LOG.info("after flushing all regions and rolling logs there are " + count + " log files");
    assertTrue(("actual count: " + count), count <= 2);
    assertLogFileSize(log);
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
    assertTrue(Bytes.equals(value,
                result.getValue(HConstants.CATALOG_FAMILY, null)));
    LOG.info("Validated row " + row);
  }

  /**
   * Tests that logs are deleted when some region has a compaction
   * record in WAL and no other records. See HBASE-8597.
   */
  @Test
  public void testCompactionRecordDoesntBlockRolling() throws Exception {
    Table table = null;

    // When the hbase:meta table can be opened, the region servers are running
    Table t = TEST_UTIL.getConnection().getTable(TableName.META_TABLE_NAME);
    try {
      table = createTestTable(getName());

      server = TEST_UTIL.getRSForFirstRegionInTable(table.getName());
      Region region = server.getOnlineRegions(table.getName()).get(0);
      final WAL log = server.getWAL(region.getRegionInfo());
      Store s = region.getStore(HConstants.CATALOG_FAMILY);

      //have to flush namespace to ensure it doesn't affect wall tests
      admin.flush(TableName.NAMESPACE_TABLE_NAME);

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
    } finally {
      if (t != null) t.close();
      if (table != null) table.close();
    }
  }

  protected void doPut(Table table, int i) throws IOException {
    Put put = new Put(Bytes.toBytes("row" + String.format("%1$04d", i)));
    put.addColumn(HConstants.CATALOG_FAMILY, null, value);
    table.put(put);
  }

  protected Table createTestTable(String tableName) throws IOException {
    // Create the test table and open it
    HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(tableName));
    desc.addFamily(new HColumnDescriptor(HConstants.CATALOG_FAMILY));
    admin.createTable(desc);
    return TEST_UTIL.getConnection().getTable(desc.getTableName());
  }
}
