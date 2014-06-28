/**
 * Copyright 2008 The Apache Software Foundation
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
package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertTrue;

@Category(MediumTests.class)
public class TestSizeAwareThriftHRegionServer {

  private static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private ExecutorService service = Executors.newFixedThreadPool(20);

  @BeforeClass
  public static void setUp() throws Exception {
    Configuration configuration = TEST_UTIL.getConfiguration();
    configuration.setLong("max.callqueue.memory.size", 1024 * 5);
    configuration.setInt("hbase.client.retries.number", 3);
    TEST_UTIL.startMiniCluster(1);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testSize() throws Exception {
    String tableName = "testSizeThrottle";
    final byte[] tableNameBytes = Bytes.toBytes(tableName);
    final byte[] cf = Bytes.toBytes("d");
    TEST_UTIL.createTable(tableNameBytes, cf);

    HTable ht = new HTable(TEST_UTIL.getConfiguration(), tableName);
    ht.setAutoFlush(false);
    Put smallPut = new Put(Bytes.toBytes("a"));
    smallPut.add(cf, Bytes.toBytes(0), Bytes.toBytes("1"));

    // This should work.
    ht.put(smallPut);
    ht.flushCommits();

    final AtomicBoolean threw = new AtomicBoolean(false);

    final ArrayList<Future<Void>> futures = new ArrayList<>();

    // Since there's so many threads some of these should fail.
    for (int i = 0; i < 20; i++) {
      futures.add(service.submit(new Callable<Void>() {
        @Override public Void call() throws Exception {

          HTable t = new HTable(TEST_UTIL.getConfiguration(), tableNameBytes);
          t.setAutoFlush(false);
          for (int y = 0; y < 100; y++) {
            Put p = new Put(Bytes.toBytes("test"));
            for (int x = 0; x < 1000; x++) {
              p.add(cf, Bytes.toBytes(x), Bytes.toBytes("b"));
            }
            try {
              t.put(p);
              t.flushCommits();
            } catch (Exception roe) {
              threw.set(true);
            }

          }

          t.close();

          return null;
        }
      }));
    }

    for (Future<Void> f : futures) {
      f.get();
    }

    assertTrue("Should have caught a region too busy exception", threw.get());
  }
}
