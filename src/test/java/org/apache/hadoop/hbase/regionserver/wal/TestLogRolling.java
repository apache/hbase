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

import java.io.OutputStream;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.UnstableTests;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSCluster.DataNodeProperties;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.LeaseManager;
import org.apache.log4j.Level;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test log deletion as logs are rolled.
 */
@Category(MediumTests.class)
public class TestLogRolling {
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final Log LOG = LogFactory.getLog(TestLogRolling.class);
  private static HRegionServer server;
  private static HLog log;
  private static Configuration conf;
  private static String tableName;
  private static byte[] value;
  private static MiniHBaseCluster cluster;
  private static FileSystem fs;
  private static MiniDFSCluster dfsCluster;

  /**
   * constructor
   * @throws Exception
   */
  @BeforeClass
  public static void setUp() throws Exception {
    DataNode.LOG.getLogger().setLevel(Level.ALL);
    ((Log4JLogger)LeaseManager.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger)FSNamesystem.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger)DFSClient.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger)HRegionServer.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger)HRegion.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger)HLog.LOG).getLogger().setLevel(Level.ALL);


    // start one regionserver and a minidfs.
    try {
      server = null;
      log = null;
      tableName = null;
      value = null;

      String className = TestLogRolling.class.getName();
      StringBuilder v = new StringBuilder(className);
      while (v.length() < 1000) {
        v.append(className);
      }
      value = Bytes.toBytes(v.toString());

    } catch (Exception e) {
      LOG.fatal("error in constructor", e);
      throw e;
    }

    conf = TEST_UTIL.getConfiguration();

    /**** configuration for testLogRolling ****/
    // Force a region split after every 768KB
    conf.setLong("hbase.hregion.max.filesize", 768L * 1024L);

    // We roll the log after every 32 writes
    conf.setInt("hbase.regionserver.maxlogentries", 32);

    // For less frequently updated regions flush after every 2 flushes
    conf.setInt("hbase.hregion.memstore.optionalflushcount", 2);

    // We flush the cache after every 8192 bytes
    conf.setInt(HConstants.HREGION_MEMSTORE_FLUSH_SIZE, 8192);

    // Increase the amount of time between client retries
    conf.setLong("hbase.client.pause", 15 * 1000);

    // Reduce thread wake frequency so that other threads can get
    // a chance to run.
    conf.setInt(HConstants.THREAD_WAKE_FREQUENCY, 2 * 1000);

   /**** configuration for testLogRollOnDatanodeDeath ****/
   // make sure log.hflush() calls syncFs() to open a pipeline
   conf.setBoolean("dfs.support.append", true);
   // lower the namenode & datanode heartbeat so the namenode
   // quickly detects datanode failures
   conf.setInt("heartbeat.recheck.interval", 5000);
   conf.setInt("dfs.heartbeat.interval", 1);
   // the namenode might still try to choose the recently-dead datanode
   // for a pipeline, so try to a new pipeline multiple times
   conf.setInt("dfs.client.block.write.retries", 30);
   conf.setBoolean(HConstants.HLOG_FORMAT_BACKWARD_COMPATIBILITY, false);
   cluster = TEST_UTIL.startMiniCluster(4);
   dfsCluster = TEST_UTIL.getDFSCluster();
   fs = dfsCluster.getFileSystem();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  private void startAndWriteData() throws Exception {
    // When the META table can be opened, the region servers are running
    new HTable(conf, HConstants.META_TABLE_NAME);
    server = cluster.getRegionServerThreads().get(0).getRegionServer();
    log = server.getLog(0);

    // Create the test table and open it
    HTableDescriptor desc = new HTableDescriptor(tableName);
    desc.addFamily(new HColumnDescriptor(HConstants.CATALOG_FAMILY));
    HBaseAdmin admin = new HBaseAdmin(conf);
    admin.createTable(desc);
    HTable table = new HTable(conf, tableName);
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
   *
   * @throws Exception
   */
  // Marked as unstable and recored in #3896573
  @Category(UnstableTests.class)
  @Test(timeout = 180000)
  public void testLogRolling() throws Exception {
    tableName = "testLogRolling";
    try {
      startAndWriteData();
      LOG.info("after writing there are " + log.getNumLogFiles() + " log files");

      // flush all regions

      List<HRegion> regions =
        new ArrayList<HRegion>(server.getOnlineRegions());
      for (HRegion r: regions) {
        r.flushcache();
      }

      // Now roll the log
      log.rollWriter();

      int count = log.getNumLogFiles();
      LOG.info("after flushing all regions and rolling logs there are " +
          log.getNumLogFiles() + " log files");
      assertTrue(("actual count: " + count), count <= 2);
    } catch (Exception e) {
      LOG.fatal("unexpected exception", e);
      throw e;
    }
  }

  void writeData(HTable table, int rownum) throws Exception {
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

  /**
   * Tests that logs are rolled upon detecting datanode death
   * Requires an HDFS jar with HDFS-826 & syncFs() support (HDFS-200)
   *
   * @throws Exception
   */
  // Marked as unstable and recored in #3344583
  @Category(UnstableTests.class)
  @Test(timeout = 180000)
  public void testLogRollOnDatanodeDeath() throws Exception {
    assertTrue("This test requires HLog file replication.",
        fs.getDefaultReplication() > 1);

    // When the META table can be opened, the region servers are running
    new HTable(conf, HConstants.META_TABLE_NAME);
    server = cluster.getRegionServer(0);
    log = server.getLog(0);

    assertTrue("Need HDFS-826 for this test", log.canGetCurReplicas());
    // don't run this test without append support (HDFS-200 & HDFS-142)
    assertTrue("Need append support for this test", FSUtils.isAppendSupported(conf));

    // add up the datanode count, to ensure proper replication when we kill 1
    dfsCluster.startDataNodes(conf, 1, true, null, null);
    dfsCluster.waitActive();
    assertTrue(dfsCluster.getDataNodes().size() >=
               fs.getDefaultReplication() + 1);

    // Create the test table and open it
    String tableName = "testLogRollOnDatanodeDeath";
    HTableDescriptor desc = new HTableDescriptor(tableName);
    desc.addFamily(new HColumnDescriptor(HConstants.CATALOG_FAMILY));
    HBaseAdmin admin = new HBaseAdmin(conf);
    admin.createTable(desc);
    HTable table = new HTable(conf, tableName);
    table.setAutoFlush(true);

    long curTime = System.currentTimeMillis();
    long oldFilenum = log.getFilenum();
    assertTrue("Log should have a timestamp older than now",
             curTime > oldFilenum && oldFilenum != -1);

    // normal write
    writeData(table, 1);
    assertTrue("The log shouldn't have rolled yet",
              oldFilenum == log.getFilenum());

    // kill a datanode in the pipeline to force a log roll on the next sync()
    Object repl;
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
        getPipeline != null);
    repl = getPipeline.invoke(stm  /*NO_ARGS*/);
    DatanodeInfo[] pipeline = (DatanodeInfo[]) repl;
    assertEquals(fs.getDefaultReplication(), pipeline.length);
    DataNodeProperties dnprop = dfsCluster.stopDataNode(pipeline[0].getName());
    assertTrue(dnprop != null);

    // this write should succeed, but trigger a log roll
    writeData(table, 2);
    long newFilenum = log.getFilenum();
    assertTrue("Missing datanode should've triggered a log roll",
              newFilenum > oldFilenum && newFilenum > curTime);

    // write some more log data (this should use a new hdfs_out)
    writeData(table, 3);
    assertTrue("The log should not roll again.",
              log.getFilenum() == newFilenum);
    assertEquals(fs.getDefaultReplication(), log.getLogReplication());
  }
}
