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
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.LoadTestKVGenerator;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Run tests that use the HBase clients; {@link HTable} and {@link HTablePool}.
 * Sets up the HBase mini cluster once at start and runs through all client tests.
 * Each creates a table named for the method and does its stuff against that.
 */
public class TestBatchedUpload {
  private static final Log LOG = LogFactory.getLog(TestBatchedUpload.class);
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static byte [] FAMILY = Bytes.toBytes("testFamily");
  private static byte [] QUALIFIER = Bytes.toBytes("testQualifier");
  private static int SLAVES = 5;

  private enum RegionServerAction {
    KILL_REGIONSERVER,
    MOVE_REGION
  }

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.getConfiguration().set(HBaseTestingUtility.FS_TYPE_KEY,
        HBaseTestingUtility.FS_TYPE_LFS);

    TEST_UTIL.startMiniCluster(SLAVES);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test(timeout = 200000)
  public void testBatchedUpload() throws Exception {
    byte [] TABLE = Bytes.toBytes("testBatchedUpload");
    int NUM_REGIONS = 10;
    HTable ht = TEST_UTIL.createTable(TABLE, new byte[][]{FAMILY},
        3, Bytes.toBytes("aaaaa"), Bytes.toBytes("zzzzz"), NUM_REGIONS);

    int NUM_ROWS = 1000;

    // start batch processing
    // do a bunch of puts
    // finish batch. Check for Exceptions.
    int attempts = writeData(ht, NUM_ROWS, RegionServerAction.KILL_REGIONSERVER);
    assert(attempts > 1);

    readData(ht, NUM_ROWS);

    ht.close();
  }

  @Test(timeout = 200000)
  /*
   * Test to make sure that if a region moves benignly, and both
   * the source and dest region servers are alive, then the batch
   * should succeed.
   */
  public void testBatchedUploadWithRegionMoves() throws Exception {
    byte [] TABLE = Bytes.toBytes("testBatchedUploadWithRegionMoves");
    int NUM_REGIONS = 10;
    HTable ht = TEST_UTIL.createTable(TABLE, new byte[][]{FAMILY},
        3, Bytes.toBytes("aaaaa"), Bytes.toBytes("zzzzz"), NUM_REGIONS);
    int NUM_ROWS = 1000;

    // start batch processing
    // do a bunch of puts
    // finish batch. Check for Exceptions.
    HMaster m = TEST_UTIL.getHBaseCluster().getMaster();

    // Disable the load balancer as the movement of the region might cause the
    // load balancer to kick in and move the regions causing the end of batched
    // upload to fail.
    m.disableLoadBalancer();
    int attempts = writeData(ht, NUM_ROWS, RegionServerAction.MOVE_REGION);
    m.enableLoadBalancer();
    assert(attempts == 1);

    readData(ht, NUM_ROWS);

    ht.close();
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
  public int writeData(HTable table, long numRows, RegionServerAction action) throws IOException, InterruptedException {
    int attempts = 0;
    int MAX = 10;
    MiniHBaseCluster cluster = TEST_UTIL.getMiniHBaseCluster();
    Random rand = new Random(5234234);
    double killProb = 2.0 / numRows;
    double prob;
    int kills = 0;

    while (attempts < MAX) {
      try {
        attempts++;

        // start batched session
        table.startBatchedLoad();

        // do batched puts
        // with WAL turned off
        for (long i = 0; i < numRows; i++) {
          byte [] rowKey = longToByteArrayKey(i);
          Put put = new Put(rowKey);
          byte[] value = rowKey; // value is the same as the row key
          put.add(FAMILY, QUALIFIER, value);
          put.setWriteToWAL(false);

          prob = rand.nextDouble();
          if (kills < 2 && prob < killProb) { // kill up to 2 rs
            kills++;
            // Find the region server for the next put
            HRegionLocation regLoc = table.getRegionLocation(put.row);
            int srcRSIdx = cluster.getServerWith(regLoc.getRegionInfo().getRegionName());

            LOG.debug("Try " + attempts + " written Puts : " + i);
            if (action == RegionServerAction.KILL_REGIONSERVER) {
              // abort the region server
              LOG.info("Killing region server " + srcRSIdx
                  + " before the next put. Got probability " +
                  prob + " < " + killProb);
              cluster.abortRegionServer(srcRSIdx);

            } else if (action == RegionServerAction.MOVE_REGION) {

              // move the region to some other Region Server
              HRegionServer dstRS = cluster.getRegionServer(
                  (srcRSIdx + 1) % cluster.getLiveRegionServerThreads().size());
              LOG.info("Moving region " + regLoc.getRegionInfo().getRegionNameAsString()
                 + " from " + cluster.getRegionServer(srcRSIdx) + " to "
                 + dstRS);
              moveRegionAndWait(cluster.getRegionServer(srcRSIdx).
                  getOnlineRegion(regLoc.getRegionInfo().getRegionName()), dstRS);
            }
            // keep decreasing the probability of killing the RS
            killProb = killProb / 2;
          }
          table.put(put);
        }

        LOG.info("Written all puts. Trying to end Batch");
        // complete batched puts
        table.endBatchedLoad();
        return attempts;

      } catch (IOException e) {
        e.printStackTrace();
        LOG.info("Failed try # " + attempts);
      }
    }

    throw new IOException("Failed to do batched puts after " + MAX + " retries.");
  }

  public void readData(HTable table, long numRows) throws IOException {
    for(long i = 0; i < numRows; i++) {
      byte [] rowKey = longToByteArrayKey(i);

      Get.Builder get = new Get.Builder(rowKey);
      get.addColumn(FAMILY, QUALIFIER);
      get.setMaxVersions(1);
      Result result = table.get(get.create());

      assertTrue(Arrays.equals(rowKey, result.getValue(FAMILY, QUALIFIER)));
    }
  }

  private void moveRegionAndWait(HRegion regionToMove, HRegionServer destServer)
      throws InterruptedException, MasterNotRunningException,
       IOException {
    TEST_UTIL.getHBaseAdmin().moveRegion(
        regionToMove.getRegionName(),
        destServer.getServerInfo().getHostnamePort());
    while (destServer.getOnlineRegion(regionToMove.getRegionName()) == null) {
      //Wait for this move to complete.
      Thread.sleep(10);
    }
  }

  private byte[] longToByteArrayKey(long rowKey) {
    return LoadTestKVGenerator.md5PrefixedKey(rowKey).getBytes();
  }
}
