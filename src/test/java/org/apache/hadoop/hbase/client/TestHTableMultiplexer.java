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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.client.HTableMultiplexer.HTableMultiplexerStatus;
import org.apache.hadoop.hbase.ipc.HBaseRPCOptions;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.StringBytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MediumTests.class)
public class TestHTableMultiplexer {
  final Log LOG = LogFactory.getLog(getClass());
  private static final HBaseTestingUtility TEST_UTIL =
      new HBaseTestingUtility();
  private static final int SLAVES = 3;
  private static final StringBytes TABLE = new StringBytes(
      "TestHTableMultiplexer");
  private static final byte[] FAMILY = Bytes.toBytes("testFamily");

  private static final byte[] QUALIFIER = Bytes.toBytes("testQualifier");
  private static final byte[] VALUE1 = Bytes.toBytes("testValue1");
  private static final byte[] VALUE2 = Bytes.toBytes("testValue2");
  private static final int PER_REGIONSERVER_QUEUE_SIZE = 100000;
  private static final int NUM_REGIONS = 10;

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

  @Before
  public void setUp() throws Exception {
    if (TEST_UTIL.getHBaseAdmin().tableExists(TABLE.getBytes())) {
      TEST_UTIL.deleteTable(TABLE);
    }
    TEST_UTIL.createTable(TABLE, new byte[][] { FAMILY }, 3,
        Bytes.toBytes("aaaaa"), Bytes.toBytes("zzzzz"), NUM_REGIONS);
  }

  @After
  public void tearDown() throws Exception {
    TEST_UTIL.deleteTable(TABLE);
  }

  @Test(timeout = 300000)
  public void testHTableMultiplexer() throws Exception {
    List<Put> failedPuts = null;
    boolean success = false;

    HTableMultiplexer multiplexer = new HTableMultiplexer(
        TEST_UTIL.getConfiguration(), PER_REGIONSERVER_QUEUE_SIZE);
    HTableMultiplexerStatus status = multiplexer.getHTableMultiplexerStatus();

    HTable ht = TEST_UTIL.getHTable(TABLE);
    byte[][] startRows = ht.getStartKeys();
    byte[][] endRows = ht.getEndKeys();

    // SinglePut case
    for (int i = 0; i < NUM_REGIONS; i++) {
      Put put = new Put(startRows[i]);
      put.add(FAMILY, QUALIFIER, VALUE1);
      success = multiplexer.put(TABLE.getBytes(), put, HBaseRPCOptions.DEFAULT);
      Assert.assertTrue(success);

      // ensure the buffer has been flushed
      verifyAllBufferedPutsHasFlushed(status);

      // verify that the Get returns the correct result
      Get get =
          new Get.Builder(startRows[i]).addColumn(FAMILY, QUALIFIER).create();
      Result r = ht.get(get);
      Assert.assertEquals(0,
          Bytes.compareTo(VALUE1, r.getValue(FAMILY, QUALIFIER)));
    }

    // MultiPut case
    List<Put> multiput = new ArrayList<Put>();
    for (int i = 0; i < NUM_REGIONS; i++) {
      Put put = new Put(endRows[i]);
      put.add(FAMILY, QUALIFIER, VALUE2);
      multiput.add(put);
    }
    failedPuts =
        multiplexer.put(TABLE.getBytes(), multiput, HBaseRPCOptions.DEFAULT);
    Assert.assertTrue(failedPuts == null);

    // ensure the buffer has been flushed
    verifyAllBufferedPutsHasFlushed(status);

    // verify that the Get returns the correct result
    for (int i = 0; i < NUM_REGIONS; i++) {
      Get get =
          new Get.Builder(endRows[i]).addColumn(FAMILY, QUALIFIER).create();
      Result r = ht.get(get);
      Assert.assertEquals(0,
          Bytes.compareTo(VALUE2, r.getValue(FAMILY, QUALIFIER)));
    }
  }

  private void verifyAllBufferedPutsHasFlushed(HTableMultiplexerStatus status) {
    int retries = 100; // max retry time is 200 * 100 ms => 20 sec
    int tries = 0;
    do {
      try {
        Thread.sleep(2 * TEST_UTIL.getConfiguration().getLong(
            "hbase.htablemultiplexer.flush.frequency", 100));
        tries++;
      } catch (InterruptedException e) {
      } // ignore
      status.recalculateCounters();
    } while (status.getTotalBufferedCounter() != 0 && tries != retries);

    Assert.assertEquals("There are still some buffered puts left in the queue",
        0, status.getTotalBufferedCounter());
  }


  /**
   * This test is to verify that the buffered and succeeded counters work as
   * expected.
   *
   * @throws Exception
   */
  @Test(timeout = 150000)
  public void testCounters() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setLong("hbase.htablemultiplexer.flush.frequency.ms", 100);

    HTableMultiplexer multiplexer =
      new HTableMultiplexer(TEST_UTIL.getConfiguration(),
        100);
    HTableMultiplexerStatus status = multiplexer.getHTableMultiplexerStatus();

    int numPuts = 100;
    boolean bufferWasNonEmptyAtSomePoint = false;
    for (int i = 1; i <= numPuts; i++) {
      byte[] row = Bytes.toBytes("Row" + i);
      byte[] qualifier = Bytes.toBytes("Qualifier" + i);
      byte[] value = Bytes.toBytes("Value" + i);
      Put put = new Put(row);
      put.add(FAMILY, qualifier, value);

      multiplexer.put(TABLE.getBytes(), put, HBaseRPCOptions.DEFAULT);
      status.recalculateCounters();
      if (status.getTotalBufferedCounter() > 0) {
        bufferWasNonEmptyAtSomePoint = true;
      }

      try {
        Thread.sleep(5);
      } catch (Exception e) {}
    }

    // Verify that all the puts got flushed.
    verifyAllBufferedPutsHasFlushed(status);

    // Recalculate the counters, and ensure that:
    // 1. The number of buffered puts was non-zero at some point.
    // 2. The number of buffered puts is zero now.
    // 3. The total number of succeeded puts is equal to the number of puts.
    status.recalculateCounters();
    Assert.assertTrue(bufferWasNonEmptyAtSomePoint);
    Assert.assertEquals(0, status.getTotalBufferedCounter());
    Assert.assertEquals(numPuts, status.getTotalSucccededPutCounter());

    HTable ht = TEST_UTIL.getHTable(TABLE);

    // Check if all the puts went through correctly.
    for (int i = 1; i <= numPuts; i++) {
      byte[] row = Bytes.toBytes("Row" + i);
      byte[] qualifier = Bytes.toBytes("Qualifier" + i);
      byte[] value = Bytes.toBytes("Value" + i);

      // verify that the Get returns the correct result
      Get get = new Get.Builder(row).addColumn(FAMILY, qualifier).create();
      Result r = ht.get(get);
      Assert.assertEquals(0,
        Bytes.compareTo(value, r.getValue(FAMILY, qualifier)));
    }
  }
  /**
   * This test is to verify that different instances of byte-array with same
   * content as the table names will result in the same HTable instance.
   */
  @Test(timeout = 120000)
  public void testCachedOfHTable() throws Exception {
    HTableMultiplexer multiplexer =
        new HTableMultiplexer(TEST_UTIL.getConfiguration(), 100);

    HTableMultiplexerStatus status = multiplexer.getHTableMultiplexerStatus();
    Assert.assertEquals("storedHTableCount", 0, status.getStoredHTableCount());

    byte[] row = Bytes.toBytes("Row" + 1);
    byte[] qualifier = Bytes.toBytes("Qualifier" + 1);
    byte[] value = Bytes.toBytes("Value" + 1);
    Put put = new Put(row);
    put.add(FAMILY, qualifier, value);
    // first put
    multiplexer.put(TABLE.getBytes(), put, HBaseRPCOptions.DEFAULT);
    Assert.assertEquals("storedHTableCount", 1, status.getStoredHTableCount());
    // second put
    byte[] TABLE1 = Arrays.copyOf(TABLE.getBytes(), TABLE.getBytes().length);
    multiplexer.put(TABLE1, put, HBaseRPCOptions.DEFAULT);
    Assert.assertEquals("storedHTableCount", 1, status.getStoredHTableCount());
  }

  /**
   * Test when multiple client threads are using HTableMultiplexer. Spawn 10
   * threads that do 10k multiputs each, and check in the end that we got
   * expected number of results back when we do Gets.
   *
   * @throws Exception
   */
  @Test(timeout = 300000)
  public void testMultipleThreads() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setLong("hbase.htablemultiplexer.flush.frequency.ms", 10);
    HTable ht = TEST_UTIL.getHTable(TABLE);
    HTableMultiplexer multiplexer = new HTableMultiplexer(
        TEST_UTIL.getConfiguration(), 1000);
    ExecutorService executor = Executors.newFixedThreadPool(10);
    List<Future<?>> futures = new ArrayList<>(10);
    byte[] rowPrefix = Bytes.toBytes("row");
    for (int i = 0; i < 10; i++) {
      byte[] suffix = Bytes.toBytes(i);
      byte[] row = Bytes.add(rowPrefix, suffix);
      Runnable runnable = new Client(multiplexer, TABLE.getBytes(), row);
      Future<?> future = executor.submit(runnable);
      futures.add(future);
    }
    for (Future<?> f : futures) {
      f.get();
    }
    // Wait for multiplexer flush
    Thread.sleep(2000);
    for (int i = 0; i < 10; i++) {
      byte[] suffix = Bytes.toBytes(i);
      byte[] row = Bytes.add(rowPrefix, suffix);
      checkForGets(ht, row);
    }
    // check the latencies
    HTableMultiplexerStatus status = multiplexer.getHTableMultiplexerStatus();
    System.out.println("max latency: " + status.getMaxLatency());
  }

  /**
   * Utility method to check if we got all the data back after putting with
   * multiplexer
   */
  public void checkForGets(HTable ht, byte[] row) throws IOException {
    for (int i = 0; i < 10000; i++) {

      byte[] suffix = Bytes.toBytes(i);
      byte[] exactRow = Bytes.add(row, suffix);

      Get get = new Get(exactRow);
      Result r = ht.get(get);
      Assert.assertEquals(1, r.getKvs().size());
      Assert.assertEquals(Bytes.toString(exactRow),
          Bytes.toString(r.getKvs().get(0).getValue()));
    }
  }

  /**
   * A client which is doing 10k puts via multiplexer
   *
   */
  public static class Client implements Runnable {
    private HTableMultiplexer multiPlex;
    private byte[] ht;
    private byte[] row;
    private byte[] dummy = Bytes.toBytes("dummy");

    public Client(HTableMultiplexer multiPlex, byte[] ht, byte[] row) {
      this.multiPlex = multiPlex;
      this.ht = ht;
      this.row = row;
    }

    @Override
    public void run() {
      int maxTry = 0;
      for (int i = 0; i < 10000; i++) {
        try {
          // sleeping so that we don't put a whole bunch of data at once
          Thread.sleep(1);
        } catch (InterruptedException e1) {
          Thread.currentThread().interrupt();
        }
        byte[] suffix = Bytes.toBytes(i);
        byte[] exactRow = Bytes.add(row, suffix);
        Put put = new Put(exactRow);
        put.add(FAMILY, dummy, exactRow);
        try {
          boolean success = true;
          int numTry = 0;
          while (true) {
            success = multiPlex.put(ht, put, HBaseRPCOptions.DEFAULT);
            numTry++;
            if (success)
              break;
            else
              Thread.sleep(1000);
          }
          if (numTry > maxTry)
            maxTry = numTry;
        } catch (IOException e) {
          e.printStackTrace();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
      System.out.println("Max number of times this thread retried: " + maxTry);
    }
  }
}

