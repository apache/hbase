/**
 * Copyright 2011 The Apache Software Foundation
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
package org.apache.hadoop.hbase.master;

import static org.apache.hadoop.hbase.zookeeper.ZKSplitLog.Counters.tot_mgr_wait_for_zk_delete;
import static org.apache.hadoop.hbase.zookeeper.ZKSplitLog.Counters.tot_wkr_final_transistion_failed;
import static org.apache.hadoop.hbase.zookeeper.ZKSplitLog.Counters.tot_wkr_task_acquired;
import static org.apache.hadoop.hbase.zookeeper.ZKSplitLog.Counters.tot_wkr_task_err;
import static org.apache.hadoop.hbase.zookeeper.ZKSplitLog.Counters.tot_wkr_task_resigned;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NavigableSet;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.master.SplitLogManager.TaskBatch;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.JVMClusterUtil.RegionServerThread;
import org.apache.hadoop.hbase.util.TagRunner;
import org.apache.hadoop.hbase.util.TestTag;
import org.apache.hadoop.hbase.zookeeper.ZKSplitLog;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(TagRunner.class)
public class TestDistributedLogSplitting {
  private static final Log LOG = LogFactory.getLog(TestDistributedLogSplitting.class);
  static {
    Logger.getLogger("org.apache.hadoop.hbase").setLevel(Level.DEBUG);
  }

  final int NUM_RS = 6;
  final int HLOG_CNT_PER_SERVER = 2;

  MiniHBaseCluster cluster;
  HMaster master;
  Configuration conf;
  HBaseTestingUtility TEST_UTIL;
  byte[] table = Bytes.toBytes("table");
  byte[] family = Bytes.toBytes("family");
  byte[] value = Bytes.toBytes("value");

  @Before
  public void before() throws Exception {

  }

  private void startCluster(int num_rs) throws Exception{
    ZKSplitLog.Counters.resetCounters();
    LOG.info("Starting cluster");
    conf = HBaseConfiguration.create();
    conf.setInt(HConstants.REGIONSERVER_INFO_PORT, -1);
    conf.setFloat("hbase.regions.slop", (float)100.0); // no load balancing
    conf.setBoolean(HConstants.DISTRIBUTED_LOG_SPLITTING_KEY, true);
    conf.setInt(HConstants.HLOG_CNT_PER_SERVER, HLOG_CNT_PER_SERVER);
    conf.setBoolean(HConstants.HLOG_FORMAT_BACKWARD_COMPATIBILITY, false);
    TEST_UTIL = new HBaseTestingUtility(conf);
    cluster = TEST_UTIL.startMiniCluster(num_rs);
    int live_rs;
    while ((live_rs = cluster.getLiveRegionServerThreads().size()) < num_rs) {
      LOG.info(live_rs + " out of " + num_rs + " started, waiting ...");
      Thread.sleep(500);
    }
    master = cluster.getMaster();
    while (!master.isActiveMaster() || master.isClosed() || !master.getIsSplitLogAfterStartupDone()) {
      LOG.info("waiting for master to be ready");
      Thread.sleep(500);
    }
  }

  @After
  public void after() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  // Marked as unstable and recorded in #4053598
  @TestTag({ "unstable" })
  @Test
  public void testThreeRSAbort() throws Exception {
    LOG.info("testThreeRSAbort");
    final int NUM_REGIONS_TO_CREATE = 40;
    final int NUM_ROWS_PER_REGION = 100;

    startCluster(NUM_RS);


    HTable ht = installTable(table, family, NUM_REGIONS_TO_CREATE);
    populateDataInTable(NUM_ROWS_PER_REGION, "family");


    List<RegionServerThread> rsts = cluster.getLiveRegionServerThreads();
    assertEquals(NUM_RS, rsts.size());
    rsts.get(0).getRegionServer().abort("testing");
    rsts.get(1).getRegionServer().abort("testing");
    rsts.get(2).getRegionServer().abort("testing");

    long start = EnvironmentEdgeManager.currentTimeMillis();
    while (cluster.getLiveRegionServerThreads().size() > (NUM_RS - 3)) {
      if (EnvironmentEdgeManager.currentTimeMillis() - start > 60000) {
        assertTrue(false);
      }
      Thread.sleep(200);
    }

    start = EnvironmentEdgeManager.currentTimeMillis();
    while (getAllOnlineRegions(cluster).size() < (NUM_REGIONS_TO_CREATE + 2)) {
      if (EnvironmentEdgeManager.currentTimeMillis() - start > 60000) {
        assertTrue(false);
      }
      Thread.sleep(200);
    }

    assertEquals(NUM_REGIONS_TO_CREATE * NUM_ROWS_PER_REGION,
        TEST_UTIL.countRows(ht));
  }

  @Test
  public void testRecoveredEdits() throws Exception {
    LOG.info("testRecoveredEdits");
    startCluster(NUM_RS);
    final int NUM_LOG_LINES = 1000;
    final int NUM_REGIONS = 40;
    final SplitLogManager slm = master.getSplitLogManager();
    FileSystem fs = master.getFileSystem();
    List<RegionServerThread> rsts = cluster.getLiveRegionServerThreads();

    HRegionServer hrs = rsts.get(0).getRegionServer();
    Path rootdir = FSUtils.getRootDir(conf);
    final Path logDir = new Path(rootdir,
        HLog.getHLogDirectoryName(hrs.getServerInfo().getServerName()));

    HTable htable = installTable(table, family, NUM_REGIONS);

    Collection<HRegion> regions = new LinkedList<HRegion>(hrs.getOnlineRegions());
    LOG.info("#regions = " + regions.size());
    Iterator<HRegion> it = regions.iterator();
    while (it.hasNext()) {
      HRegion region = it.next();
      HRegionInfo hri = region.getRegionInfo();
      if (hri.isMetaRegion() || hri.isRootRegion()) {
        it.remove();
      }
    }
    for (HRegion region : regions) {
      for (int i = 0; i < NUM_LOG_LINES; i++) {
        Put p = new Put(region.getStartKey());
        p.add(family, Bytes.toBytes("cf"+i), i, value);
        htable.put(p);
      }
    }

    slm.splitLogDistributed(logDir);

    for (HRegion rgn : regions) {
      int count = 0;
      Path tdir = HTableDescriptor.getTableDir(rootdir, table);
      Path editsdir =
        HLog.getRegionDirRecoveredEditsDir(HRegion.getRegionDir(tdir,
        rgn.getRegionInfo().getEncodedName()));
      LOG.debug("checking edits dir " + editsdir);
      FileStatus[] files = fs.listStatus(editsdir);
      assertEquals(1, files.length);
      int c = countHLog(files[0].getPath(), fs, conf);
      count += c;
      LOG.info(c + " edits in " + files[0].getPath());
      // one more hlog edit for recording the seqid transition
      assertEquals(NUM_LOG_LINES+1, count);
    }
  }

  @Test
  public void testWorkerAbort() throws Exception {
    LOG.info("testWorkerAbort");
    startCluster(1);
    final int NUM_LOG_LINES = 1000;
    final SplitLogManager slm = master.getSplitLogManager();
    FileSystem fs = master.getFileSystem();

    final List<RegionServerThread> rsts = cluster.getLiveRegionServerThreads();
    HRegionServer hrs = rsts.get(0).getRegionServer();
    Path rootdir = FSUtils.getRootDir(conf);
    final Path logDir = new Path(rootdir,
        HLog.getHLogDirectoryName(hrs.getServerInfo().getServerName()));

    HTable htable = installTable(table, family, 40);

    for (HRegion region : hrs.getOnlineRegions()) {
      for (int i = 0; i < NUM_LOG_LINES; i++) {
        Put p = new Put(region.getStartKey());
        p.add(family, Bytes.toBytes("cf"+i), i, value);
        htable.put(p);
      }
    }

    new Thread() {
      @Override
      public void run() {
        waitForCounter(tot_wkr_task_acquired, 0, 1, 1000);
        for (RegionServerThread rst : rsts) {
          rst.getRegionServer().abort("testing");
        }
      }
    }.start();
    // slm.splitLogDistributed(logDir);
    FileStatus[] logfiles = fs.listStatus(logDir);
    TaskBatch batch = new TaskBatch();
    slm.installTask(logfiles[0].getPath().toString(), batch);
    //waitForCounter but for one of the 2 counters
    long curt = System.currentTimeMillis();
    long endt = curt + 30000;
    while (curt < endt) {
      if ((tot_wkr_task_resigned.get() + tot_wkr_task_err.get() +
          tot_wkr_final_transistion_failed.get()) == 0) {
        Thread.yield();
        curt = System.currentTimeMillis();
      } else {
        assertEquals(1, (tot_wkr_task_resigned.get() + tot_wkr_task_err.get() +
            tot_wkr_final_transistion_failed.get()));
        return;
      }
    }
    assertEquals(1, batch.done);
    // fail("region server completed the split before aborting");
    return;
  }

  @Test
  public void testDelayedDeleteOnFailure() throws Exception {
    LOG.info("testDelayedDeleteOnFailure");
    startCluster(1);
    final SplitLogManager slm = master.splitLogManager;
    final FileSystem fs = master.getFileSystem();
    final Path logDir = new Path(FSUtils.getRootDir(conf), "x");
    fs.mkdirs(logDir);
    final Path corruptedLogFile = new Path(logDir, "x");
    FSDataOutputStream out;
    out = fs.create(corruptedLogFile);
    out.write(0);
    out.write(Bytes.toBytes("corrupted bytes"));
    out.close();
    slm.ignoreZKDeleteForTesting = true;
    Thread t = new Thread() {
      @Override
      public void run() {
        try {
          slm.splitLogDistributed(logDir);
        } catch (IOException ioe) {
          try {
            assertTrue(fs.exists(corruptedLogFile));
            slm.splitLogDistributed(logDir);
          } catch (IOException e) {
            assertTrue(Thread.currentThread().isInterrupted());
            return;
          }
          fail("did not get the expected IOException from the 2nd call");
        }
        fail("did not get the expected IOException from the 1st call");
      }
    };
    t.start();
    waitForCounter(tot_mgr_wait_for_zk_delete, 0, 1, 10000);
    t.interrupt();
    t.join();
  }

  HTable installTable(byte [] tname, byte [] fname, int nrs ) throws Exception {
    LOG.info("Creating table with " + nrs + " regions");
    HTable ht = TEST_UTIL.createTable(table, new byte[][]{family},
        3, Bytes.toBytes("aaaaa"), Bytes.toBytes("zzzzz"), nrs);
    NavigableSet<String> regions = getAllOnlineRegions(cluster);
    assertEquals(nrs + 2, regions.size());
    return ht;
  }

  void populateDataInTable(int nrows, String fname) throws Exception {
    byte [] family = Bytes.toBytes(fname);

    List<RegionServerThread> rsts = cluster.getLiveRegionServerThreads();
    assertEquals(NUM_RS, rsts.size());

    for (RegionServerThread rst : rsts) {
      HRegionServer hrs = rst.getRegionServer();
      Collection<HRegion> regions = hrs.getOnlineRegions();
      for (HRegion r : regions) {
        HRegionInfo hri = r.getRegionInfo();
        if (hri.isMetaRegion() || hri.isRootRegion()) {
          continue;
        }
        LOG.debug("adding data to rs = " + rst.getName() +
            " region = "+ r.getRegionNameAsString());
        HRegion region = hrs.getOnlineRegion(r.getRegionName());
        putData(region, hri.getStartKey(), nrows, Bytes.toBytes("q"), family);
      }
    }
  }

  private int countHLog(Path log, FileSystem fs, Configuration conf)
  throws IOException {
    int count = 0;
    HLog.Reader in = HLog.getReader(fs, log, conf);
    while (in.next() != null) {
      count++;
    }
    return count;
  }

  private void putData(HRegion region, byte[] startRow, int numRows, byte [] qf,
      byte [] ...families)
  throws IOException {
    for(int i = 0; i < numRows; i++) {
      Put put = new Put(Bytes.add(startRow, Bytes.toBytes(i)));
      for(byte [] family : families) {
        put.add(family, qf, null);
      }
      region.put(put);
    }
  }

  private NavigableSet<String> getAllOnlineRegions(MiniHBaseCluster cluster) {
    NavigableSet<String> online = new TreeSet<String>();
    for (RegionServerThread rst : cluster.getLiveRegionServerThreads()) {
      for (HRegion region : rst.getRegionServer().getOnlineRegions()) {
        online.add(region.getRegionNameAsString());
      }
    }
    return online;
  }

  private void waitForCounter(AtomicLong ctr, long oldval, long newval,
      long timems) {
    long curt = System.currentTimeMillis();
    long endt = curt + timems;
    while (curt < endt) {
      if (ctr.get() == oldval) {
        Thread.yield();
        curt = System.currentTimeMillis();
      } else {
        assertEquals(newval, ctr.get());
        return;
      }
    }
    assertTrue(false);
  }
}
