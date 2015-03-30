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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.fs.HFileSystem;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.wal.DefaultWALProvider;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test log deletion as logs are rolled.
 */
@Category(LargeTests.class)
public class TestLogRolling  {
  private static final Log LOG = LogFactory.getLog(TestLogRolling.class);
  private HRegionServer server;
  private String tableName;
  private byte[] value;
  private FileSystem fs;
  private MiniDFSCluster dfsCluster;
  private Admin admin;
  private MiniHBaseCluster cluster;
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  /**
   * constructor
   * @throws Exception
   */
  public TestLogRolling()  {
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
    // TODO: testLogRollOnDatanodeDeath fails if short circuit reads are on under the hadoop2
    // profile. See HBASE-9337 for related issues.
    System.setProperty("hbase.tests.use.shortcircuit.reads", "false");

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

   /**** configuration for testLogRollOnDatanodeDeath ****/
   // make sure log.hflush() calls syncFs() to open a pipeline
    TEST_UTIL.getConfiguration().setBoolean("dfs.support.append", true);
   // lower the namenode & datanode heartbeat so the namenode
   // quickly detects datanode failures
    TEST_UTIL.getConfiguration().setInt("dfs.namenode.heartbeat.recheck-interval", 5000);
    TEST_UTIL.getConfiguration().setInt("dfs.heartbeat.interval", 1);
    // the namenode might still try to choose the recently-dead datanode
    // for a pipeline, so try to a new pipeline multiple times
     TEST_UTIL.getConfiguration().setInt("dfs.client.block.write.retries", 30);
    TEST_UTIL.getConfiguration().setInt("hbase.regionserver.hlog.tolerable.lowreplication", 2);
    TEST_UTIL.getConfiguration().setInt("hbase.regionserver.hlog.lowreplication.rolllimit", 3);
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

  private void startAndWriteData() throws IOException, InterruptedException {
    // When the hbase:meta table can be opened, the region servers are running
    new HTable(TEST_UTIL.getConfiguration(), TableName.META_TABLE_NAME);
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
    final WAL newLog = wals.getWAL(new byte[]{});
    try {
      // Now roll the log before we write anything.
      newLog.rollWriter(true);
    } finally {
      wals.close();
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
    final WAL log = server.getWAL(null);
    LOG.info("after writing there are " + DefaultWALProvider.getNumRolledLogFiles(log) +
        " log files");

      // flush all regions
      for (Region r: server.getOnlineRegionsLocalContext()) {
        r.flush(true);
      }

      // Now roll the log
      log.rollWriter();

    int count = DefaultWALProvider.getNumRolledLogFiles(log);
    LOG.info("after flushing all regions and rolling logs there are " + count + " log files");
      assertTrue(("actual count: " + count), count <= 2);
  }

  private static String getName() {
    return "TestLogRolling";
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

  void batchWriteAndWait(Table table, final FSHLog log, int start, boolean expect, int timeout)
      throws IOException {
    for (int i = 0; i < 10; i++) {
      Put put = new Put(Bytes.toBytes("row"
          + String.format("%1$04d", (start + i))));
      put.add(HConstants.CATALOG_FAMILY, null, value);
      table.put(put);
    }
    Put tmpPut = new Put(Bytes.toBytes("tmprow"));
    tmpPut.add(HConstants.CATALOG_FAMILY, null, value);
    long startTime = System.currentTimeMillis();
    long remaining = timeout;
    while (remaining > 0) {
      if (log.isLowReplicationRollEnabled() == expect) {
        break;
      } else {
        // Trigger calling FSHlog#checkLowReplication()
        table.put(tmpPut);
        try {
          Thread.sleep(200);
        } catch (InterruptedException e) {
          // continue
        }
        remaining = timeout - (System.currentTimeMillis() - startTime);
      }
    }
  }

  /**
   * Tests that logs are rolled upon detecting datanode death
   * Requires an HDFS jar with HDFS-826 & syncFs() support (HDFS-200)
   */
  @Test
  public void testLogRollOnDatanodeDeath() throws Exception {
    TEST_UTIL.ensureSomeRegionServersAvailable(2);
    assertTrue("This test requires WAL file replication set to 2.",
      fs.getDefaultReplication(TEST_UTIL.getDataTestDirOnTestFS()) == 2);
    LOG.info("Replication=" +
      fs.getDefaultReplication(TEST_UTIL.getDataTestDirOnTestFS()));

    this.server = cluster.getRegionServer(0);

    // Create the test table and open it
    HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(getName()));
    desc.addFamily(new HColumnDescriptor(HConstants.CATALOG_FAMILY));

    admin.createTable(desc);
    Table table = TEST_UTIL.getConnection().getTable(desc.getTableName());
    assertTrue(((HTable) table).isAutoFlush());

    server = TEST_UTIL.getRSForFirstRegionInTable(desc.getTableName());
    final FSHLog log = (FSHLog) server.getWAL(null);
    final AtomicBoolean lowReplicationHookCalled = new AtomicBoolean(false);

    log.registerWALActionsListener(new WALActionsListener.Base() {
      @Override
      public void logRollRequested(boolean lowReplication) {
        if (lowReplication) {
          lowReplicationHookCalled.lazySet(true);
        }
      }
    });

    // don't run this test without append support (HDFS-200 & HDFS-142)
    assertTrue("Need append support for this test", FSUtils
        .isAppendSupported(TEST_UTIL.getConfiguration()));

    // add up the datanode count, to ensure proper replication when we kill 1
    // This function is synchronous; when it returns, the dfs cluster is active
    // We start 3 servers and then stop 2 to avoid a directory naming conflict
    //  when we stop/start a namenode later, as mentioned in HBASE-5163
    List<DataNode> existingNodes = dfsCluster.getDataNodes();
    int numDataNodes = 3;
    dfsCluster.startDataNodes(TEST_UTIL.getConfiguration(), numDataNodes, true,
        null, null);
    List<DataNode> allNodes = dfsCluster.getDataNodes();
    for (int i = allNodes.size()-1; i >= 0; i--) {
      if (existingNodes.contains(allNodes.get(i))) {
        dfsCluster.stopDataNode( i );
      }
    }

    assertTrue("DataNodes " + dfsCluster.getDataNodes().size() +
        " default replication " +
        fs.getDefaultReplication(TEST_UTIL.getDataTestDirOnTestFS()),
    dfsCluster.getDataNodes().size() >=
      fs.getDefaultReplication(TEST_UTIL.getDataTestDirOnTestFS()) + 1);

    writeData(table, 2);

    long curTime = System.currentTimeMillis();
    LOG.info("log.getCurrentFileName(): " + log.getCurrentFileName());
    long oldFilenum = DefaultWALProvider.extractFileNumFromWAL(log);
    assertTrue("Log should have a timestamp older than now",
        curTime > oldFilenum && oldFilenum != -1);

    assertTrue("The log shouldn't have rolled yet",
        oldFilenum == DefaultWALProvider.extractFileNumFromWAL(log));
    final DatanodeInfo[] pipeline = log.getPipeLine();
    assertTrue(pipeline.length ==
        fs.getDefaultReplication(TEST_UTIL.getDataTestDirOnTestFS()));

    // kill a datanode in the pipeline to force a log roll on the next sync()
    // This function is synchronous, when it returns the node is killed.
    assertTrue(dfsCluster.stopDataNode(pipeline[0].getName()) != null);

    // this write should succeed, but trigger a log roll
    writeData(table, 2);
    long newFilenum = DefaultWALProvider.extractFileNumFromWAL(log);

    assertTrue("Missing datanode should've triggered a log roll",
        newFilenum > oldFilenum && newFilenum > curTime);

    assertTrue("The log rolling hook should have been called with the low replication flag",
        lowReplicationHookCalled.get());

    // write some more log data (this should use a new hdfs_out)
    writeData(table, 3);
    assertTrue("The log should not roll again.",
        DefaultWALProvider.extractFileNumFromWAL(log) == newFilenum);
    // kill another datanode in the pipeline, so the replicas will be lower than
    // the configured value 2.
    assertTrue(dfsCluster.stopDataNode(pipeline[1].getName()) != null);

    batchWriteAndWait(table, log, 3, false, 14000);
    int replication = log.getLogReplication();
    assertTrue("LowReplication Roller should've been disabled, current replication="
            + replication, !log.isLowReplicationRollEnabled());

    dfsCluster
        .startDataNodes(TEST_UTIL.getConfiguration(), 1, true, null, null);

    // Force roll writer. The new log file will have the default replications,
    // and the LowReplication Roller will be enabled.
    log.rollWriter(true);
    batchWriteAndWait(table, log, 13, true, 10000);
    replication = log.getLogReplication();
    assertTrue("New log file should have the default replication instead of " +
      replication,
      replication == fs.getDefaultReplication(TEST_UTIL.getDataTestDirOnTestFS()));
    assertTrue("LowReplication Roller should've been enabled", log.isLowReplicationRollEnabled());
  }

  /**
   * Test that WAL is rolled when all data nodes in the pipeline have been
   * restarted.
   * @throws Exception
   */
  @Test
  public void testLogRollOnPipelineRestart() throws Exception {
    LOG.info("Starting testLogRollOnPipelineRestart");
    assertTrue("This test requires WAL file replication.",
      fs.getDefaultReplication(TEST_UTIL.getDataTestDirOnTestFS()) > 1);
    LOG.info("Replication=" +
      fs.getDefaultReplication(TEST_UTIL.getDataTestDirOnTestFS()));
    // When the hbase:meta table can be opened, the region servers are running
    Table t = new HTable(TEST_UTIL.getConfiguration(), TableName.META_TABLE_NAME);
    try {
      this.server = cluster.getRegionServer(0);

      // Create the test table and open it
      HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(getName()));
      desc.addFamily(new HColumnDescriptor(HConstants.CATALOG_FAMILY));

      admin.createTable(desc);
      Table table = new HTable(TEST_UTIL.getConfiguration(), desc.getTableName());

      server = TEST_UTIL.getRSForFirstRegionInTable(desc.getTableName());
      final WAL log = server.getWAL(null);
      final List<Path> paths = new ArrayList<Path>();
      final List<Integer> preLogRolledCalled = new ArrayList<Integer>();

      paths.add(DefaultWALProvider.getCurrentFileName(log));
      log.registerWALActionsListener(new WALActionsListener.Base() {

        @Override
        public void preLogRoll(Path oldFile, Path newFile)  {
          LOG.debug("preLogRoll: oldFile="+oldFile+" newFile="+newFile);
          preLogRolledCalled.add(new Integer(1));
        }
        @Override
        public void postLogRoll(Path oldFile, Path newFile) {
          paths.add(newFile);
        }
      });

      // don't run this test without append support (HDFS-200 & HDFS-142)
      assertTrue("Need append support for this test", FSUtils
          .isAppendSupported(TEST_UTIL.getConfiguration()));

      writeData(table, 1002);

      long curTime = System.currentTimeMillis();
      LOG.info("log.getCurrentFileName()): " + DefaultWALProvider.getCurrentFileName(log));
      long oldFilenum = DefaultWALProvider.extractFileNumFromWAL(log);
      assertTrue("Log should have a timestamp older than now",
          curTime > oldFilenum && oldFilenum != -1);

      assertTrue("The log shouldn't have rolled yet", oldFilenum ==
          DefaultWALProvider.extractFileNumFromWAL(log));

      // roll all datanodes in the pipeline
      dfsCluster.restartDataNodes();
      Thread.sleep(1000);
      dfsCluster.waitActive();
      LOG.info("Data Nodes restarted");
      validateData(table, 1002);

      // this write should succeed, but trigger a log roll
      writeData(table, 1003);
      long newFilenum = DefaultWALProvider.extractFileNumFromWAL(log);

      assertTrue("Missing datanode should've triggered a log roll",
          newFilenum > oldFilenum && newFilenum > curTime);
      validateData(table, 1003);

      writeData(table, 1004);

      // roll all datanode again
      dfsCluster.restartDataNodes();
      Thread.sleep(1000);
      dfsCluster.waitActive();
      LOG.info("Data Nodes restarted");
      validateData(table, 1004);

      // this write should succeed, but trigger a log roll
      writeData(table, 1005);

      // force a log roll to read back and verify previously written logs
      log.rollWriter(true);
      assertTrue("preLogRolledCalled has size of " + preLogRolledCalled.size(),
          preLogRolledCalled.size() >= 1);

      // read back the data written
      Set<String> loggedRows = new HashSet<String>();
      FSUtils fsUtils = FSUtils.getInstance(fs, TEST_UTIL.getConfiguration());
      for (Path p : paths) {
        LOG.debug("recovering lease for " + p);
        fsUtils.recoverFileLease(((HFileSystem)fs).getBackingFs(), p,
          TEST_UTIL.getConfiguration(), null);

        LOG.debug("Reading WAL "+FSUtils.getPath(p));
        WAL.Reader reader = null;
        try {
          reader = WALFactory.createReader(fs, p, TEST_UTIL.getConfiguration());
          WAL.Entry entry;
          while ((entry = reader.next()) != null) {
            LOG.debug("#"+entry.getKey().getLogSeqNum()+": "+entry.getEdit().getCells());
            for (Cell cell : entry.getEdit().getCells()) {
              loggedRows.add(Bytes.toStringBinary(cell.getRow()));
            }
          }
        } catch (EOFException e) {
          LOG.debug("EOF reading file "+FSUtils.getPath(p));
        } finally {
          if (reader != null) reader.close();
        }
      }

      // verify the written rows are there
      assertTrue(loggedRows.contains("row1002"));
      assertTrue(loggedRows.contains("row1003"));
      assertTrue(loggedRows.contains("row1004"));
      assertTrue(loggedRows.contains("row1005"));

      // flush all regions
      for (Region r: server.getOnlineRegionsLocalContext()) {
        r.flush(true);
      }

      ResultScanner scanner = table.getScanner(new Scan());
      try {
        for (int i=2; i<=5; i++) {
          Result r = scanner.next();
          assertNotNull(r);
          assertFalse(r.isEmpty());
          assertEquals("row100"+i, Bytes.toString(r.getRow()));
        }
      } finally {
        scanner.close();
      }

      // verify that no region servers aborted
      for (JVMClusterUtil.RegionServerThread rsThread:
        TEST_UTIL.getHBaseCluster().getRegionServerThreads()) {
        assertFalse(rsThread.getRegionServer().isAborted());
      }
    } finally {
      if (t != null) t.close();
    }
  }

  /**
   * Tests that logs are deleted when some region has a compaction
   * record in WAL and no other records. See HBASE-8597.
   */
  @Test
  public void testCompactionRecordDoesntBlockRolling() throws Exception {
    Table table = null;
    Table table2 = null;

    // When the hbase:meta table can be opened, the region servers are running
    Table t = new HTable(TEST_UTIL.getConfiguration(), TableName.META_TABLE_NAME);
    try {
      table = createTestTable(getName());
      table2 = createTestTable(getName() + "1");

      server = TEST_UTIL.getRSForFirstRegionInTable(table.getName());
      final WAL log = server.getWAL(null);
      Region region = server.getOnlineRegions(table2.getName()).get(0);
      Store s = region.getStore(HConstants.CATALOG_FAMILY);

      //have to flush namespace to ensure it doesn't affect wall tests
      admin.flush(TableName.NAMESPACE_TABLE_NAME);

      // Put some stuff into table2, to make sure we have some files to compact.
      for (int i = 1; i <= 2; ++i) {
        doPut(table2, i);
        admin.flush(table2.getName());
      }
      doPut(table2, 3); // don't flush yet, or compaction might trigger before we roll WAL
      assertEquals("Should have no WAL after initial writes", 0,
          DefaultWALProvider.getNumRolledLogFiles(log));
      assertEquals(2, s.getStorefilesCount());

      // Roll the log and compact table2, to have compaction record in the 2nd WAL.
      log.rollWriter();
      assertEquals("Should have WAL; one table is not flushed", 1,
          DefaultWALProvider.getNumRolledLogFiles(log));
      admin.flush(table2.getName());
      region.compact(false);
      // Wait for compaction in case if flush triggered it before us.
      Assert.assertNotNull(s);
      for (int waitTime = 3000; s.getStorefilesCount() > 1 && waitTime > 0; waitTime -= 200) {
        Threads.sleepWithoutInterrupt(200);
      }
      assertEquals("Compaction didn't happen", 1, s.getStorefilesCount());

      // Write some value to the table so the WAL cannot be deleted until table is flushed.
      doPut(table, 0); // Now 2nd WAL will have compaction record for table2 and put for table.
      log.rollWriter(); // 1st WAL deleted, 2nd not deleted yet.
      assertEquals("Should have WAL; one table is not flushed", 1,
          DefaultWALProvider.getNumRolledLogFiles(log));

      // Flush table to make latest WAL obsolete; write another record, and roll again.
      admin.flush(table.getName());
      doPut(table, 1);
      log.rollWriter(); // Now 2nd WAL is deleted and 3rd is added.
      assertEquals("Should have 1 WALs at the end", 1,
          DefaultWALProvider.getNumRolledLogFiles(log));
    } finally {
      if (t != null) t.close();
      if (table != null) table.close();
      if (table2 != null) table2.close();
    }
  }

  private void doPut(Table table, int i) throws IOException {
    Put put = new Put(Bytes.toBytes("row" + String.format("%1$04d", i)));
    put.add(HConstants.CATALOG_FAMILY, null, value);
    table.put(put);
  }

  private Table createTestTable(String tableName) throws IOException {
    // Create the test table and open it
    HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(tableName));
    desc.addFamily(new HColumnDescriptor(HConstants.CATALOG_FAMILY));
    admin.createTable(desc);
    return new HTable(TEST_UTIL.getConfiguration(), desc.getTableName());
  }
}

