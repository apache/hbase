/**
 * Copyright 2007 The Apache Software Foundation
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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.util.*;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.namenode.LeaseManager;
import org.apache.log4j.Level;
import org.junit.*;
import org.junit.experimental.categories.Category;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;

import static org.junit.Assert.*;

/**
 * Test log deletion as logs are rolled.
 */
@Category(LargeTests.class)
public class TestLogRolling  {
  private static final Log LOG = LogFactory.getLog(TestLogRolling.class);
  private HRegionServer server;
  private HLog log;
  private String tableName;
  private byte[] value;
  private FileSystem fs;
  private MiniDFSCluster dfsCluster;
  private HBaseAdmin admin;
  private MiniHBaseCluster cluster;
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

 // verbose logging on classes that are touched in these tests
 {
   ((Log4JLogger)DataNode.LOG).getLogger().setLevel(Level.ALL);
   ((Log4JLogger)LeaseManager.LOG).getLogger().setLevel(Level.ALL);
   ((Log4JLogger)LogFactory.getLog("org.apache.hadoop.hdfs.server.namenode.FSNamesystem"))
     .getLogger().setLevel(Level.ALL);
   ((Log4JLogger)DFSClient.LOG).getLogger().setLevel(Level.ALL);
   ((Log4JLogger)HRegionServer.LOG).getLogger().setLevel(Level.ALL);
   ((Log4JLogger)HRegion.LOG).getLogger().setLevel(Level.ALL);
   ((Log4JLogger)HLog.LOG).getLogger().setLevel(Level.ALL);
 }

  /**
   * constructor
   * @throws Exception
   */
  public TestLogRolling()  {
    this.server = null;
    this.log = null;
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

    TEST_UTIL.getConfiguration().setInt(
        "hbase.regionserver.logroll.errors.tolerated", 2);
    TEST_UTIL.getConfiguration().setInt("ipc.ping.interval", 10 * 1000);
    TEST_UTIL.getConfiguration().setInt("ipc.socket.timeout", 10 * 1000);
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
    TEST_UTIL.getConfiguration().setInt("heartbeat.recheck.interval", 5000);
    TEST_UTIL.getConfiguration().setInt("dfs.heartbeat.interval", 1);
   // the namenode might still try to choose the recently-dead datanode
   // for a pipeline, so try to a new pipeline multiple times
    TEST_UTIL.getConfiguration().setInt("dfs.client.block.write.retries", 30);
    TEST_UTIL.getConfiguration().setInt(
        "hbase.regionserver.hlog.tolerable.lowreplication", 2);
    TEST_UTIL.getConfiguration().setInt(
        "hbase.regionserver.hlog.lowreplication.rolllimit", 3);
  }

  @Before
  public void setUp() throws Exception {
    TEST_UTIL.startMiniCluster(2);

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

  private void startAndWriteData() throws IOException {
    // When the META table can be opened, the region servers are running
    new HTable(TEST_UTIL.getConfiguration(), HConstants.META_TABLE_NAME);
    this.server = cluster.getRegionServerThreads().get(0).getRegionServer();
    this.log = server.getWAL();

    // Create the test table and open it
    HTableDescriptor desc = new HTableDescriptor(tableName);
    desc.addFamily(new HColumnDescriptor(HConstants.CATALOG_FAMILY));
    admin.createTable(desc);
    HTable table = new HTable(TEST_UTIL.getConfiguration(), tableName);

    server = TEST_UTIL.getRSForFirstRegionInTable(Bytes.toBytes(tableName));
    this.log = server.getWAL();
    for (int i = 1; i <= 256; i++) {    // 256 writes should cause 8 log rolls
      Put put = new Put(Bytes.toBytes("row" + String.format("%1$04d", i)));
      put.add(HConstants.CATALOG_FAMILY, null, value);
      table.put(put);
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
   * Tests that logs are deleted
   * @throws IOException
   * @throws FailedLogCloseException
   */
  @Test
  public void testLogRolling() throws FailedLogCloseException, IOException {
    this.tableName = getName();
      startAndWriteData();
      LOG.info("after writing there are " + log.getNumLogFiles() + " log files");

      // flush all regions

      List<HRegion> regions =
        new ArrayList<HRegion>(server.getOnlineRegionsLocalContext());
      for (HRegion r: regions) {
        r.flushcache();
      }

      // Now roll the log
      log.rollWriter();

      int count = log.getNumLogFiles();
      LOG.info("after flushing all regions and rolling logs there are " +
          log.getNumLogFiles() + " log files");
      assertTrue(("actual count: " + count), count <= 2);
  }

  private static String getName() {
    return "TestLogRolling";
  }

  void writeData(HTable table, int rownum) throws IOException {
    Put put = new Put(Bytes.toBytes("row" + String.format("%1$04d", rownum)));
    put.add(HConstants.CATALOG_FAMILY, null, value);
    table.put(put);

    // sleep to let the log roller run (if it needs to)
    try {
      Thread.sleep(2000);
    } catch (InterruptedException e) {
      // continue
    }
  }

  void validateData(HTable table, int rownum) throws IOException {
    String row = "row" + String.format("%1$04d", rownum);
    Get get = new Get(Bytes.toBytes(row));
    get.addFamily(HConstants.CATALOG_FAMILY);
    Result result = table.get(get);
    assertTrue(result.size() == 1);
    assertTrue(Bytes.equals(value,
                result.getValue(HConstants.CATALOG_FAMILY, null)));
    LOG.info("Validated row " + row);
  }

  void batchWriteAndWait(HTable table, int start, boolean expect, int timeout)
      throws IOException {
    for (int i = 0; i < 10; i++) {
      Put put = new Put(Bytes.toBytes("row"
          + String.format("%1$04d", (start + i))));
      put.add(HConstants.CATALOG_FAMILY, null, value);
      table.put(put);
    }
    long startTime = System.currentTimeMillis();
    long remaining = timeout;
    while (remaining > 0) {
      if (log.isLowReplicationRollEnabled() == expect) {
        break;
      } else {
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
   * Give me the HDFS pipeline for this log file
   */
  DatanodeInfo[] getPipeline(HLog log) throws IllegalArgumentException,
      IllegalAccessException, InvocationTargetException {
    OutputStream stm = log.getOutputStream();
    Method getPipeline = null;
    for (Method m : stm.getClass().getDeclaredMethods()) {
      if (m.getName().endsWith("getPipeline")) {
        getPipeline = m;
        getPipeline.setAccessible(true);
        break;
      }
    }

    assertTrue("Need DFSOutputStream.getPipeline() for this test",
        null != getPipeline);
    Object repl = getPipeline.invoke(stm, new Object[] {} /* NO_ARGS */);
    return (DatanodeInfo[]) repl;
  }


  /**
   * Tests that logs are rolled upon detecting datanode death
   * Requires an HDFS jar with HDFS-826 & syncFs() support (HDFS-200)
   * @throws IOException
   * @throws InterruptedException
   * @throws InvocationTargetException
   * @throws IllegalAccessException
   * @throws IllegalArgumentException
    */
  @Test
  public void testLogRollOnDatanodeDeath() throws Exception {
    assertTrue("This test requires HLog file replication set to 2.",
      fs.getDefaultReplication() == 2);
    LOG.info("Replication=" + fs.getDefaultReplication());

    this.server = cluster.getRegionServer(0);
    this.log = server.getWAL();

    // Create the test table and open it
    String tableName = getName();
    HTableDescriptor desc = new HTableDescriptor(tableName);
    desc.addFamily(new HColumnDescriptor(HConstants.CATALOG_FAMILY));

    admin.createTable(desc);
    HTable table = new HTable(TEST_UTIL.getConfiguration(), tableName);
    assertTrue(table.isAutoFlush());

    server = TEST_UTIL.getRSForFirstRegionInTable(Bytes.toBytes(tableName));
    this.log = server.getWAL();

    assertTrue("Need HDFS-826 for this test", log.canGetCurReplicas());
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
        " default replication " + fs.getDefaultReplication(),
      dfsCluster.getDataNodes().size() >= fs.getDefaultReplication() + 1);

    writeData(table, 2);

    long curTime = System.currentTimeMillis();
    long oldFilenum = log.getFilenum();
    assertTrue("Log should have a timestamp older than now",
        curTime > oldFilenum && oldFilenum != -1);

    assertTrue("The log shouldn't have rolled yet",
      oldFilenum == log.getFilenum());
    final DatanodeInfo[] pipeline = getPipeline(log);
    assertTrue(pipeline.length == fs.getDefaultReplication());

    // kill a datanode in the pipeline to force a log roll on the next sync()
    // This function is synchronous, when it returns the node is killed.
    assertTrue(dfsCluster.stopDataNode(pipeline[0].getName()) != null);

    // this write should succeed, but trigger a log roll
    writeData(table, 2);
    long newFilenum = log.getFilenum();

    assertTrue("Missing datanode should've triggered a log roll",
        newFilenum > oldFilenum && newFilenum > curTime);

    // write some more log data (this should use a new hdfs_out)
    writeData(table, 3);
    assertTrue("The log should not roll again.",
      log.getFilenum() == newFilenum);
    // kill another datanode in the pipeline, so the replicas will be lower than
    // the configured value 2.
    assertTrue(dfsCluster.stopDataNode(pipeline[1].getName()) != null);

    batchWriteAndWait(table, 3, false, 10000);
    assertTrue("LowReplication Roller should've been disabled",
        !log.isLowReplicationRollEnabled());

    dfsCluster
        .startDataNodes(TEST_UTIL.getConfiguration(), 1, true, null, null);

    // Force roll writer. The new log file will have the default replications,
    // and the LowReplication Roller will be enabled.
    log.rollWriter(true);
    batchWriteAndWait(table, 13, true, 10000);
    assertTrue("New log file should have the default replication instead of " +
      log.getLogReplication(),
      log.getLogReplication() == fs.getDefaultReplication());
    assertTrue("LowReplication Roller should've been enabled",
        log.isLowReplicationRollEnabled());
  }

  /**
   * Test that HLog is rolled when all data nodes in the pipeline have been
   * restarted.
   * @throws Exception
   */
  //DISABLED BECAUSE FLAKEY @Test
  public void testLogRollOnPipelineRestart() throws Exception {
    LOG.info("Starting testLogRollOnPipelineRestart");
    assertTrue("This test requires HLog file replication.",
      fs.getDefaultReplication() > 1);
    LOG.info("Replication=" + fs.getDefaultReplication());
    // When the META table can be opened, the region servers are running
    new HTable(TEST_UTIL.getConfiguration(), HConstants.META_TABLE_NAME);

    this.server = cluster.getRegionServer(0);
    this.log = server.getWAL();

    // Create the test table and open it
    String tableName = getName();
    HTableDescriptor desc = new HTableDescriptor(tableName);
    desc.addFamily(new HColumnDescriptor(HConstants.CATALOG_FAMILY));

    admin.createTable(desc);
    HTable table = new HTable(TEST_UTIL.getConfiguration(), tableName);

    server = TEST_UTIL.getRSForFirstRegionInTable(Bytes.toBytes(tableName));
    this.log = server.getWAL();
    final List<Path> paths = new ArrayList<Path>();
    final List<Integer> preLogRolledCalled = new ArrayList<Integer>();
    paths.add(log.computeFilename());
    log.registerWALActionsListener(new WALActionsListener() {
      @Override
      public void preLogRoll(Path oldFile, Path newFile)  {
        LOG.debug("preLogRoll: oldFile="+oldFile+" newFile="+newFile);
        preLogRolledCalled.add(new Integer(1));
      }
      @Override
      public void postLogRoll(Path oldFile, Path newFile) {
        paths.add(newFile);
      }
      @Override
      public void preLogArchive(Path oldFile, Path newFile) {}
      @Override
      public void postLogArchive(Path oldFile, Path newFile) {}
      @Override
      public void logRollRequested() {}
      @Override
      public void logCloseRequested() {}
      @Override
      public void visitLogEntryBeforeWrite(HRegionInfo info, HLogKey logKey,
          WALEdit logEdit) {}
      @Override
      public void visitLogEntryBeforeWrite(HTableDescriptor htd, HLogKey logKey,
          WALEdit logEdit) {}
    });

    assertTrue("Need HDFS-826 for this test", log.canGetCurReplicas());
    // don't run this test without append support (HDFS-200 & HDFS-142)
    assertTrue("Need append support for this test", FSUtils
        .isAppendSupported(TEST_UTIL.getConfiguration()));

    writeData(table, 1002);

    table.setAutoFlush(true);

    long curTime = System.currentTimeMillis();
    long oldFilenum = log.getFilenum();
    assertTrue("Log should have a timestamp older than now",
        curTime > oldFilenum && oldFilenum != -1);

    assertTrue("The log shouldn't have rolled yet", oldFilenum == log.getFilenum());

    // roll all datanodes in the pipeline
    dfsCluster.restartDataNodes();
    Thread.sleep(1000);
    dfsCluster.waitActive();
    LOG.info("Data Nodes restarted");
    validateData(table, 1002);

    // this write should succeed, but trigger a log roll
    writeData(table, 1003);
    long newFilenum = log.getFilenum();

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
    for (Path p : paths) {
      LOG.debug("Reading HLog "+FSUtils.getPath(p));
      HLog.Reader reader = null;
      try {
        reader = HLog.getReader(fs, p, TEST_UTIL.getConfiguration());
        HLog.Entry entry;
        while ((entry = reader.next()) != null) {
          LOG.debug("#"+entry.getKey().getLogSeqNum()+": "+entry.getEdit().getKeyValues());
          for (KeyValue kv : entry.getEdit().getKeyValues()) {
            loggedRows.add(Bytes.toStringBinary(kv.getRow()));
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
    List<HRegion> regions =
        new ArrayList<HRegion>(server.getOnlineRegionsLocalContext());
    for (HRegion r: regions) {
      r.flushcache();
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
  }

  @org.junit.Rule
  public org.apache.hadoop.hbase.ResourceCheckerJUnitRule cu =
    new org.apache.hadoop.hbase.ResourceCheckerJUnitRule();
}

