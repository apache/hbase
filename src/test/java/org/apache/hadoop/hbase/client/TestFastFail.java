/**
 * Copyright The Apache Software Foundation
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
package org.apache.hadoop.hbase.client;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.lang.mutable.MutableLong;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.client.TableServers.FailureInfo;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.DelayInducingInjectionHandler;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.InjectionEvent;
import org.apache.hadoop.hbase.util.InjectionHandler;
import org.apache.hadoop.hbase.util.JVMClusterUtil.RegionServerThread;
import org.apache.hadoop.hbase.util.LoadTestKVGenerator;
import org.apache.hadoop.hbase.util.Threads;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Run tests that use the HBase clients; {@link HTable} and {@link HTablePool}.
 * Sets up the HBase mini cluster once at start and runs through all client tests.
 * Each creates a table named for the method and does its stuff against that.
 */
public class TestFastFail {
  private static final Log LOG = LogFactory.getLog(TestFastFail.class);
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static byte [] FAMILY = Bytes.toBytes("testFamily");
  private static byte [] QUALIFIER = Bytes.toBytes("testQualifier");
  private static int SLAVES = 15;

  private static  byte [] TABLE = Bytes.toBytes("testFastFail");
  private static  int NUM_REGIONS = 100;
  private static  int NUM_ROWS = 5000;
  private static  int NUM_READER_THREADS = 10;
  private static  HTable ht;
  private static  MiniHBaseCluster cluster;
  private static ThreadLocal<MutableLong> lastErrorLogTime = new ThreadLocal<MutableLong>();
  private volatile static boolean doRead = true;

  private static long DELAY = 5000; // 5 sec

  private static final DelayInducingInjectionHandler delayer = new DelayInducingInjectionHandler();

  static CountDownLatch stopReading = new CountDownLatch(NUM_READER_THREADS);
  static CountDownLatch startReading = new CountDownLatch(1);

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniCluster(SLAVES);
    cluster = TEST_UTIL.getMiniHBaseCluster();

    // Set the injection Handler to sleep before processing dead servers
    delayer.setEventDelay(InjectionEvent.HMASTER_START_PROCESS_DEAD_SERVER, DELAY);

    // Set the fast fail timeout for the client to be really small. 1 sec.
    TEST_UTIL.getConfiguration().setInt("hbase.client.fastfail.threshold", 1000);
    ht = TEST_UTIL.createTable(TABLE, new byte[][]{FAMILY},
        3, Bytes.toBytes("aaaaa"), Bytes.toBytes("zzzzz"), NUM_REGIONS);

    writeData(ht, NUM_ROWS);

    // Start 10 threads that keep reading stuff.
    for (int i = 0; i < NUM_READER_THREADS; i++) {
      LOG.info("Launching client reader thread " + i);
      Thread readerThread = new Thread("client-reader-" + i) {
        @Override
        public void run() {
          LOG.info("Starting reads : " + getName());
          HTable table;
          try {
            table = new HTable(TEST_UTIL.getConfiguration(), TABLE);
          } catch (IOException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
            return;
          }

          while (true) {
            try {
              // ignore errors.
              if (doRead) {
                readData(table, NUM_ROWS, true);
              } else {
                LOG.info("Stopping reads.");
                stopReading.countDown();
                // wait till the RS gets killed by the mtin test
                LOG.info("Waiting to start reads again.");
                startReading.await();
                LOG.info("Starting reads again.");
              }
            } catch (IOException e) {
              // should never get here.
            } catch (InterruptedException e) {
              // TODO Auto-generated catch block
              e.printStackTrace();
            }
          }
        }
      };
      readerThread.setDaemon(true);
      readerThread.start();
    }
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    ht.close();
    TEST_UTIL.shutdownMiniCluster();
  }

  @Before
  public void setUp() throws Exception {
    stopReading = new CountDownLatch(NUM_READER_THREADS);
    startReading = new CountDownLatch(1);
    LOG.info("preliminary Writing before the test");
    writeData(ht, NUM_ROWS);
    LOG.info("preliminary Reading before the test");
    readData(ht, NUM_ROWS, false);
    InjectionHandler.set(delayer);
  }

  @After
  public void tearDown() throws Exception {
    LOG.info("Clearing out the Injection Handler");
    InjectionHandler.clear();
    LOG.info("Waiting for master to stabilize everything");
    Threads.sleep(2 * DELAY);
    LOG.info("Proceeding");
  }

  @Test(timeout = 180000)
  public void testFastFailNormalWithoutClearCache() throws Exception {
    testFastFailNormal(false);
  }

  @Test(timeout = 180000)
  public void testFastFailNormalWithClearCache() throws Exception {
    testFastFailNormal(true);
  }

  public void testFastFailNormal(boolean clearCache) throws Exception {
    LOG.info("Running test: testFastFailNormal. clearCache = " + clearCache);
    // read once. ensure that we can read without any errors.
    // kill a RS randomly
    Random rand = new Random(234343); // random fixed seed.

    int root_id = cluster.getServerWithRoot();
    int meta_id = cluster.getServerWithMeta();
    int killedCnt = 0;
    do {
      // ensure we don't end up with negative numbers.
      int rsId = (SLAVES + rand.nextInt() % SLAVES) % SLAVES;

      if (rsId == root_id || rsId == meta_id) continue;

      killedCnt++;
      killRSAndWaitUntilStabilize(rsId, clearCache);
    } while (killedCnt < 1);
  }

  @Test(timeout = 180000)
  public void testFastFailMetaWithoutClearCache() throws Exception {
    testFastFailMeta(false);
  }

  @Test(timeout = 180000)
  public void testFastFailMetaWithClearCache() throws Exception {
    testFastFailMeta(true);
  }

  public void testFastFailMeta(boolean clearCache) throws Exception {
    LOG.info("Running test: testFastFailMeta. clearCache = " + clearCache);
    do {
      int meta_id = cluster.getServerWithMeta();

      if (meta_id == -1) {
        LOG.info("META not yet assigned. Sleeping 500ms.");
        Threads.sleep(500);
        continue;
      }

      killRSAndWaitUntilStabilize(meta_id, clearCache);
      return;
    } while (true);
  }

  @Test(timeout = 180000)
  public void testFastFailRootWithoutClearMeta() throws Exception {
    testFastFailRoot(false);
  }

  @Test(timeout = 180000)
  public void testFastFailRootWithClearMeta() throws Exception {
    testFastFailRoot(true);
  }

  public void testFastFailRoot(boolean clearCache) throws Exception {
    LOG.info("Running test: testFastFailRoot. clearCache = " + clearCache);
    do {
      int root_id = cluster.getServerWithRoot();

      if (root_id == -1) {
        LOG.info("ROOT not yet assigned. Sleeping 500ms.");
        Threads.sleep(500);
        continue;
      }

      killRSAndWaitUntilStabilize(root_id, clearCache);
      return;
    } while (true);
  }


  private void killRSAndWaitUntilStabilize(int rsId, boolean populateFailureMap) throws Exception {
    doRead = false;
    stopReading.await();

    LOG.debug("Killing region server " + rsId);
    // kill a RS randomly
    cluster.abortRegionServer(rsId);

    if (populateFailureMap) {
      TableServers hcm = (TableServers)
          HConnectionManager.getConnection(TEST_UTIL.getConfiguration());

      long currentTime = EnvironmentEdgeManager.currentTimeMillis();
      Map<HServerAddress, FailureInfo> repeatedFailuresMap =
        hcm.getFailureMap();
      for (RegionServerThread th: cluster.getRegionServerThreads()) {
        HServerAddress serverAddr = th.getRegionServer().getServerInfo().getServerAddress();
        FailureInfo fInfo = hcm.new FailureInfo(currentTime - 100000); // 100 sec ago
        repeatedFailuresMap.put(serverAddr, fInfo);

        // clear out the cache
        hcm.metaCache.clearForServer(serverAddr.toString());
      }
    }

    Threads.sleep(1000);
    doRead = true;
    startReading.countDown();

    int counter = 0;
    while (true) {
      try {
        LOG.info("Trying to read data to see if things work fine. Trial # " + ++counter);
        readData(ht, NUM_ROWS, false);

        LOG.info("Reading succeded.");
        break;
      } catch(IOException e) {
        LOG.info("readData had errors " + e.toString()
            + " during trial #" + counter);
        e.printStackTrace();
        Threads.sleep(500); // 500 ms
      }
    }
  }

  /**
   * Write data to the htable. While randomly killing/shutting down regionservers.
   * @param table
   * @param numRows
   * @param action -- enum RegionServerAction which defines the type of action.
   * @return number of attempts to complete the batch.
   * @throws IOException
   * @throws InterruptedException
   */
  public static void writeData(HTable table, long numRows) throws IOException, InterruptedException {
    for (long i = 0; i < numRows; i++) {
      byte [] rowKey = longToByteArrayKey(i);
      Put put = new Put(rowKey);
      byte[] value = rowKey; // value is the same as the row key
      put.add(FAMILY, QUALIFIER, value);
      table.put(put);
    }
    LOG.info("Written all puts.");
  }

  public static void readData(HTable table, long numRows, boolean ignoreExceptions) throws IOException {
    for(long i = 0; i < numRows; i++) {
      byte [] rowKey = longToByteArrayKey(i);

      Get get = new Get(rowKey);
      get.addColumn(FAMILY, QUALIFIER);
      get.setMaxVersions(1);
      try {
        Result result = table.get(get);

        assertTrue(Arrays.equals(rowKey, result.getValue(FAMILY, QUALIFIER)));
      } catch (IOException e) {
        MutableLong lastErrorLoggedAt = lastErrorLogTime.get();
        if (lastErrorLoggedAt == null) {
          lastErrorLoggedAt = new MutableLong(0);
          lastErrorLogTime.set(lastErrorLoggedAt);
        }

        long now = EnvironmentEdgeManager.currentTimeMillis();
        if (now > lastErrorLoggedAt.longValue() + 1000) { // PFFE will flood this. rate limit to 1 msg/second/thread
          lastErrorLoggedAt.setValue(now);
          LOG.error("Caught Exception " + e.toString());
        }
        if (!ignoreExceptions) throw e;
      }
    }
  }

  private static byte[] longToByteArrayKey(long rowKey) {
    return LoadTestKVGenerator.md5PrefixedKey(rowKey).getBytes();
  }
}

