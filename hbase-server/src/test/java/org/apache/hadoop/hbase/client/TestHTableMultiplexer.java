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

import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.LargeTests;
import org.apache.hadoop.hbase.client.HTableMultiplexer.HTableMultiplexerStatus;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(LargeTests.class)
public class TestHTableMultiplexer {
  final Log LOG = LogFactory.getLog(getClass());
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static byte[] FAMILY = Bytes.toBytes("testFamily");
  private static byte[] QUALIFIER = Bytes.toBytes("testQualifier");
  private static byte[] VALUE1 = Bytes.toBytes("testValue1");
  private static byte[] VALUE2 = Bytes.toBytes("testValue2");
  private static int SLAVES = 3;
  private static int PER_REGIONSERVER_QUEUE_SIZE = 100000;

  /**
   * @throws java.lang.Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniCluster(SLAVES);
  }

  /**
   * @throws java.lang.Exception
   */
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testHTableMultiplexer() throws Exception {
    byte[] TABLE = Bytes.toBytes("testHTableMultiplexer");
    final int NUM_REGIONS = 10;
    final int VERSION = 3;
    List<Put> failedPuts = null;
    boolean success = false;
    
    HTableMultiplexer multiplexer = new HTableMultiplexer(TEST_UTIL.getConfiguration(), 
        PER_REGIONSERVER_QUEUE_SIZE);
    HTableMultiplexerStatus status = multiplexer.getHTableMultiplexerStatus();

    HTable ht = TEST_UTIL.createTable(TABLE, new byte[][] { FAMILY }, VERSION,
        Bytes.toBytes("aaaaa"), Bytes.toBytes("zzzzz"), NUM_REGIONS);
    TEST_UTIL.waitUntilAllRegionsAssigned(NUM_REGIONS);

    byte[][] startRows = ht.getStartKeys();
    byte[][] endRows = ht.getEndKeys();

    // SinglePut case
    for (int i = 0; i < NUM_REGIONS; i++) {
      Put put = new Put(startRows[i]);
      put.add(FAMILY, QUALIFIER, VALUE1);
      success = multiplexer.put(TABLE, put);
      Assert.assertTrue(success);

      // ensure the buffer has been flushed
      verifyAllBufferedPutsHaveFlushed(status);
      LOG.info("Put for " + Bytes.toString(startRows[i]) + " @ iteration " + (i+1));

      // verify that the Get returns the correct result
      Get get = new Get(startRows[i]);
      get.addColumn(FAMILY, QUALIFIER);
      Result r;
      do {
        r = ht.get(get);
      } while (r == null || r.getValue(FAMILY, QUALIFIER) == null);
      Assert.assertEquals(0, Bytes.compareTo(VALUE1, r.getValue(FAMILY, QUALIFIER)));
    }

    // MultiPut case
    List<Put> multiput = new ArrayList<Put>();
    for (int i = 0; i < NUM_REGIONS; i++) {
      Put put = new Put(endRows[i]);
      put.add(FAMILY, QUALIFIER, VALUE2);
      multiput.add(put);
    }
    failedPuts = multiplexer.put(TABLE, multiput);
    Assert.assertTrue(failedPuts == null);

    // ensure the buffer has been flushed
    verifyAllBufferedPutsHaveFlushed(status);

    // verify that the Get returns the correct result
    for (int i = 0; i < NUM_REGIONS; i++) {
      Get get = new Get(endRows[i]);
      get.addColumn(FAMILY, QUALIFIER);
      Result r;
      do {
        r = ht.get(get);
      } while (r == null || r.getValue(FAMILY, QUALIFIER) == null);
      Assert.assertEquals(0,
          Bytes.compareTo(VALUE2, r.getValue(FAMILY, QUALIFIER)));
    }
  }

  private void verifyAllBufferedPutsHaveFlushed(HTableMultiplexerStatus status) {
    int retries = 8;
    int tries = 0;
    do {
      try {
        Thread.sleep(2 * TEST_UTIL.getConfiguration().getLong(
          HTableMultiplexer.TABLE_MULTIPLEXER_FLUSH_FREQ_MS, 100));
        tries++;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    } while (status.getTotalBufferedCounter() != 0 && tries != retries);

    Assert.assertEquals("There are still some buffered puts left in the queue",
        0, status.getTotalBufferedCounter());
  }
}
