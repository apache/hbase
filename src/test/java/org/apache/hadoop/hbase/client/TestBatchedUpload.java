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

import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.LoadTestKVGenerator;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.assertTrue;

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

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniCluster(SLAVES);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testBatchedUpload() throws Exception {
    byte [] TABLE = Bytes.toBytes("testBatchedUpload");
    int NUM_REGIONS = 10;
    HTable ht = TEST_UTIL.createTable(TABLE, new byte[][]{FAMILY},
        3, Bytes.toBytes("aaaaa"), Bytes.toBytes("zzzzz"), NUM_REGIONS);
    int NUM_ROWS = 1000;

    // start batch processing
    // do a bunch of puts
    // finish batch. Check for Exceptions.
    int attempts = writeData(ht, NUM_ROWS);
    assert(attempts > 1);

    readData(ht, NUM_ROWS);

    ht.close();
  }

  public int writeData(HTable table, long numRows) throws IOException {
    int attempts = 0;
    int MAX = 10;
    MiniHBaseCluster cluster = TEST_UTIL.getMiniHBaseCluster();
    int numRS;
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
          table.put(put);

          prob = rand.nextDouble();
          if (kills < 2 && prob < killProb) { // kill up to 2 rs
            kills++;
            // kill a random one
            numRS = cluster.getRegionServerThreads().size();
            int idxToKill = Math.abs(rand.nextInt()) % numRS;
            LOG.debug("Try " + attempts + " written Puts : " + i);
            LOG.info("Randomly killing region server " + idxToKill + ". Got probability " + prob
                + " < " + killProb);
            cluster.abortRegionServer(idxToKill);

            // keep decreasing the probability of killing the RS
            killProb = killProb / 2;
          }
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

      Get get = new Get(rowKey);
      get.addColumn(FAMILY, QUALIFIER);
      get.setMaxVersions(1);
      Result result = table.get(get);
      assertTrue(Arrays.equals(rowKey, result.getValue(FAMILY, QUALIFIER)));
    }
  }

  private byte[] longToByteArrayKey(long rowKey) {
    return LoadTestKVGenerator.md5PrefixedKey(rowKey).getBytes();
  }
}
