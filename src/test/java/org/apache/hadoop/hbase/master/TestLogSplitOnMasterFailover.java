/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hbase.master;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Semaphore;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWrapper;
import org.junit.Test;

/**
 * Tests that the master splits the logs of dead regionservers on startup and
 * does not attempt to split live regionservers' logs. Done by killing a
 * regionserver to create a need to split logs, and quickly killing a master to
 * cause master failover.
 */
public class TestLogSplitOnMasterFailover extends MultiMasterTest {

  private static final Log LOG =
      LogFactory.getLog(TestLogSplitOnMasterFailover.class);

  private static final int NUM_MASTERS = 2;
  private static final int NUM_RS = 2;
  private static final int NUM_ROWS = 8000;
  private static final int COLS_PER_ROW = 30;

  private static final byte[] TABLE_BYTES = Bytes.toBytes("myTable");
  private static final byte[] CF_BYTES = Bytes.toBytes("myCF");

  private static Compression.Algorithm COMPRESSION = Compression.Algorithm.GZ;

  /**
   * A worker that inserts data into HBase on a separate thread. This is the
   * reusable base class, which is subclassed for use in this particular test.
   */
  static class DataLoader implements Runnable {

    private volatile Throwable failureException;
    private volatile boolean shutdownRequested = false;

    private Map<String, List<String>> rowToQuals =
        new HashMap<String, List<String>>();
    private HTable t;

    private Semaphore halfRowsLoaded = new Semaphore(0);
    private Semaphore dataLoadVerifyFinished = new Semaphore(0);

    private final Configuration conf;
    private volatile Thread myThread;

    public DataLoader(Configuration conf) {
      this.conf = conf;
    }

    @Override
    public void run() {
      myThread = Thread.currentThread();
      myThread.setName(getClass().getSimpleName());
      try {
        HBaseTestingUtility.createPreSplitLoadTestTable(conf,
            TABLE_BYTES, CF_BYTES, COMPRESSION, DataBlockEncoding.NONE);
        t = new HTable(conf, TABLE_BYTES);

        loadData();
        verifyData();
      } catch (Throwable ex) {
        LOG.error("Data loader failure", ex);
        failureException = ex;
      } finally {
        dataLoadVerifyFinished.release();
        if (t != null) {
          try {
            t.close();
          } catch (IOException e) {
            LOG.error("Error closing HTable", e);
          }
        }
      }
    }

    private void loadData() throws IOException {
      Random rand = new Random(190879817L);
      int bytesInserted = 0;
      for (int i = 0; i < NUM_ROWS; ++i) {
        if (shutdownRequested) {
          break;
        }
        int rowsLoaded = i + 1;
        String rowStr = String.format("%04x", rand.nextInt(65536)) + "_" + i;
        byte[] rowBytes = Bytes.toBytes(rowStr);
        Put p = new Put(rowBytes);
        List<String> quals = new ArrayList<String>();
        rowToQuals.put(rowStr, quals);
        for (int j = 0; j < COLS_PER_ROW; ++j) {
          String qualStr = "" + rand.nextInt(10000) + "_" + j;
          quals.add(qualStr);
          String valueStr = createValue(rowStr, qualStr);
          byte[] qualBytes = Bytes.toBytes(qualStr);
          byte[] valueBytes = Bytes.toBytes(valueStr);
          p.add(CF_BYTES, qualBytes, valueBytes);
          bytesInserted += rowBytes.length + qualBytes.length +
              valueBytes.length;
        }
        t.put(p);
        if (rowsLoaded % (NUM_ROWS / 10) == 0) {
          LOG.info("Loaded " + rowsLoaded + " rows");
        }
        if (rowsLoaded == NUM_ROWS / 2) {
          LOG.info("Loaded half of the rows (" + rowsLoaded
              + "), waking up main thread");
          halfRowsLoaded.release();
        }
      }
      LOG.info("Approximate number of bytes inserted: " + bytesInserted);
    }

    private void verifyData() throws IOException {
      LOG.debug("Starting data verification");
      for (Map.Entry<String, List<String>> entry : rowToQuals.entrySet()) {
        if (shutdownRequested) {
          break;
        }
        String row = entry.getKey();
        List<String> quals = entry.getValue();
        Get g = new Get(Bytes.toBytes(row));
        Result r = t.get(g);
        Map<byte[], byte[]> familyMap = r.getFamilyMap(CF_BYTES);
        assertNotNull(familyMap);
        assertEquals(quals.size(), familyMap.size());
        for (String q : quals) {
          byte[] v = familyMap.get(Bytes.toBytes(q));
          assertNotNull(v);
          assertEquals(createValue(row, q), Bytes.toStringBinary(v));
        }
      }
      LOG.debug("Data verification completed");
    }

    private String createValue(String rowStr, String qualStr) {
      return "v" + rowStr + "_" + qualStr;
    }

    public void waitUntilFinishedOrFailed() throws InterruptedException {
      LOG.debug("Waiting until we finish loading/verifying the data");
      dataLoadVerifyFinished.acquire();
    }

    public void waitUntilHalfRowsLoaded() throws InterruptedException {
      LOG.debug("Waiting until half of the rows are loaded");
      halfRowsLoaded.acquire();
    }
    public void requestShutdown() {
      shutdownRequested = true;
    }

    public void assertSuccess() {
      if (failureException != null) {
        LOG.error("Data loader failure", failureException);
        AssertionError ae = new AssertionError("Data loader failure");
        ae.initCause(failureException);
        throw ae;
      }
    }

    public void join() throws InterruptedException {
      myThread.join();
    }
  }

  @Test(timeout=180000)
  public void testWithRegularLogSplitting() throws Exception {
    ZooKeeperWrapper.setNamespaceForTesting();
    conf.setBoolean(HConstants.DISTRIBUTED_LOG_SPLITTING_KEY, false);
    runTest();
  }

  @Test(timeout=180000)
  public void testWithDistributedLogSplitting() throws Exception {
    ZooKeeperWrapper.setNamespaceForTesting();
    conf.setBoolean(HConstants.DISTRIBUTED_LOG_SPLITTING_KEY, true);
    runTest();
  }

  private void runTest() throws Exception {
    startMiniCluster(NUM_MASTERS, NUM_RS);
    Thread.currentThread().setName(getClass().getSimpleName());
    ensureMastersAreUp(NUM_MASTERS);

    final int activeIndex = getActiveMasterIndex();

    List<HMaster> masters = miniCluster().getMasters();

    header("Starting data loader");
    DataLoader dataLoader =
        new DataLoader(conf);
    Thread inserterThread = new Thread(dataLoader);
    inserterThread.start();
    dataLoader.waitUntilHalfRowsLoaded();

    Path logsDir = new Path(FSUtils.getRootDir(conf),
        HConstants.HREGION_LOGDIR_NAME);

    header("Killing one region server so we have some logs to split");
    HRegionServer rsToKill = miniCluster().getRegionServer(0);
    String killedRsName = rsToKill.getServerInfo().getServerName();
    List<String> otherRsNames = new ArrayList<String>();
    for (int i = 1; i < NUM_RS; ++i) {
      otherRsNames.add(
          miniCluster().getRegionServer(i).getServerInfo().getServerName());
    }
    rsToKill.kill();
    // Wait until the regionserver actually goes down.
    while (miniCluster().getLiveRegionServerThreads().size() == NUM_RS) {
      Threads.sleep(HConstants.SOCKET_RETRY_WAIT_MS);
    }

    // Check that we have some logs.
    FileSystem fs = FileSystem.get(conf);
    assertTrue("Directory " + logsDir + " does not exist",
        fs.exists(logsDir));
    FileStatus[] logDirs = fs.listStatus(logsDir);
    assertTrue("No logs in the log directory " + logsDir, logDirs.length > 0);

    header("Killing the active master (#" + activeIndex + ")");

    miniCluster().killMaster(activeIndex);
    miniCluster().getHBaseCluster().waitOnMasterStop(activeIndex);

    masters = miniCluster().getMasters();
    assertEquals(1, masters.size());

    // wait for an active master to show up and be ready
    assertTrue(miniCluster().waitForActiveAndReadyMaster());

    header("Verifying backup master is now active");
    // should only have one master now
    assertEquals(1, masters.size());
    // and he should be active
    HMaster master = masters.get(0);
    assertTrue(master.isActiveMaster());

    dataLoader.waitUntilFinishedOrFailed();
    dataLoader.join();
    dataLoader.assertSuccess();

    // Check the master split the correct logs at startup;
    List<String> logDirsSplitAtStartup = master.getLogDirsSplitOnStartup();
    LOG.info("Log dirs split at startup: " + logDirsSplitAtStartup);

    Set<String> logsSplit = new HashSet<String>();
    logsSplit.addAll(logDirsSplitAtStartup);
    String logDirToBeSplit = killedRsName + "-splitting";
    assertTrue("Log directory " + logDirToBeSplit + " was not split " +
        "on startup. Logs split: " + logDirsSplitAtStartup,
        logsSplit.contains(logDirToBeSplit));
    for (String logNotToSplit : otherRsNames) {
      assertFalse("Log directory " + logNotToSplit
          + " should not have been split", logsSplit.contains(logNotToSplit));
    }
  }

}
