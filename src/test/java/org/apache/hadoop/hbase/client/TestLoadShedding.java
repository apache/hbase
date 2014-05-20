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

import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang.math.RandomUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category(MediumTests.class)
public class TestLoadShedding {

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final AtomicBoolean RUNNING = new AtomicBoolean(true);
  private static final AtomicLong NUM_SHED = new AtomicLong(0);
  private static final String TABLE_NAME = "testLoadShedding";
  private static final String FAMILY_NAME_STR = "d";
  private static final byte[] FAMILY_NAME = Bytes.toBytes(FAMILY_NAME_STR);
  public static final int MAX_WRITER_THREADS = 15;

  @BeforeClass
  public static void setUp() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setInt("hbase.client.max.outstanding.requests.per.server", 5);
    TEST_UTIL.startMiniCluster();
    TEST_UTIL.waitForTableConsistent();
    HBaseAdmin hba = TEST_UTIL.getHBaseAdmin();
    HTableDescriptor htd = new HTableDescriptor(TABLE_NAME);
    htd.addFamily(new HColumnDescriptor(FAMILY_NAME));
    hba.createTable(htd, Bytes.toBytes("aaaa"), Bytes.toBytes("zzzz"), 10);
    RUNNING.set(true);
  }

  @AfterClass
  public static void tearDown() throws IOException {
    RUNNING.set(false);
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testLoadSheddingWrites() {

    // Start lots of threads so that there will be more than 2 contending per
    // server
    List<Thread> threads = new ArrayList<>(MAX_WRITER_THREADS);
    for (int i = 0; i < MAX_WRITER_THREADS; i++) {
      Thread t = new WriterThread(i);
      t.setDaemon(true);
      t.start();
      threads.add(t);
    }

    try {
      Thread.sleep(5000);
    } catch (InterruptedException e) {
    }

    RUNNING.set(false);
    assertTrue(NUM_SHED.get() > 0);

    // Now wait for everything to stop
    for (Thread t:threads) {
      try {
        t.join();
        t.stop();
      } catch (InterruptedException e) {

      }
    }

    // Clean up
    NUM_SHED.set(0);
    RUNNING.set(true);

    // Make sure that just a few threads don't contend
    for (int x=0; x< 4; x++) {
      Thread t = new WriterThread(x + MAX_WRITER_THREADS);
      t.setDaemon(true);
      t.start();
    }

    try {
      Thread.sleep(5000);
    } catch (InterruptedException e) {
    }

    RUNNING.set(false);
    assertEquals(0, NUM_SHED.get());


  }

  protected static Put generateRandomPut() {
    Put p = new Put(Bytes.toBytes(RandomStringUtils.randomAlphabetic(30)));
    p.add(FAMILY_NAME, Bytes.toBytes(RandomUtils.nextInt()), Bytes.toBytes(
        RandomStringUtils.randomAlphanumeric(10)));
    return p;
  }

  private static class WriterThread extends Thread {
    public WriterThread(int i) {
      super("client-writer-" + i);
    }

    public void run() {
      try {
        HTable ht = new HTable(TEST_UTIL.getConfiguration(), TABLE_NAME);
        ht.setAutoFlush(false);

        while(RUNNING.get()) {
          ht.put(generateRandomPut());
          ht.put(generateRandomPut());
          ht.put(generateRandomPut());
          ht.put(generateRandomPut());
          ht.put(generateRandomPut());
          ht.put(generateRandomPut());
          ht.put(generateRandomPut());
          ht.flushCommits();
        }
      } catch (IOException e) {
        NUM_SHED.incrementAndGet();
      }
    }
  }
}
