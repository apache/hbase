/**
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


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

@Category(MediumTests.class)
public class TestClientOperationInterrupt {
  private static final Log LOG = LogFactory.getLog(TestClientOperationInterrupt.class);

  private static HBaseTestingUtility util;
  private static final byte[] tableName = Bytes.toBytes("test");
  private static final byte[] dummy = Bytes.toBytes("dummy");
  private static final byte[] row1 = Bytes.toBytes("r1");
  private static final byte[] test = Bytes.toBytes("test");
  private static Configuration conf;

  public static class TestCoprocessor extends BaseRegionObserver {
    @Override
    public void preGetOp(final ObserverContext<RegionCoprocessorEnvironment> e,
                         final Get get, final List<Cell> results) throws IOException {
      Threads.sleep(2500);
    }
  }


  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    conf = HBaseConfiguration.create();
    conf.setStrings(CoprocessorHost.USER_REGION_COPROCESSOR_CONF_KEY,
        TestCoprocessor.class.getName());
    util = new HBaseTestingUtility(conf);
    util.startMiniCluster();

    HBaseAdmin admin = util.getHBaseAdmin();
    if (admin.tableExists(tableName)) {
      if (admin.isTableEnabled(tableName)) {
        admin.disableTable(tableName);
      }
      admin.deleteTable(tableName);
    }
    util.createTable(tableName, new byte[][]{dummy, test});

    HTable ht = new HTable(conf, tableName);
    Put p = new Put(row1);
    p.add(dummy, dummy, dummy);
    ht.put(p);
  }


  @Test
  public void testInterrupt50Percent() throws IOException, InterruptedException {
    final AtomicInteger noEx = new AtomicInteger(0);
    final AtomicInteger badEx = new AtomicInteger(0);
    final AtomicInteger noInt = new AtomicInteger(0);
    final AtomicInteger done = new AtomicInteger(0);
    List<Thread> threads = new ArrayList<Thread>();

    final int nbThread = 100;

    for (int i = 0; i < nbThread; i++) {
      Thread t = new Thread() {
        @Override
        public void run() {
          try {
            HTable ht = new HTable(conf, tableName);
            Result r = ht.get(new Get(row1));
            noEx.incrementAndGet();
          } catch (IOException e) {
            LOG.info("exception", e);
            if (!(e instanceof InterruptedIOException) || (e instanceof SocketTimeoutException)) {
              badEx.incrementAndGet();
            } else {
              if (Thread.currentThread().isInterrupted()) {
                noInt.incrementAndGet();
                LOG.info("The thread should NOT be with the 'interrupt' status.");
              }
            }
          } finally {
            done.incrementAndGet();
          }
        }
      };
      t.setName("TestClientOperationInterrupt #" + i);
      threads.add(t);
      t.start();
    }

    for (int i = 0; i < nbThread / 2; i++) {
      threads.get(i).interrupt();
    }


    boolean stillAlive = true;
    while (stillAlive) {
      stillAlive = false;
      for (Thread t : threads) {
        if (t.isAlive()) {
          stillAlive = true;
        }
      }
      Threads.sleep(10);
    }

    Assert.assertFalse(Thread.currentThread().isInterrupted());

    Assert.assertTrue(" noEx: " + noEx.get() + ", badEx=" + badEx.get() + ", noInt=" + noInt.get(),
        noEx.get() == nbThread / 2 && badEx.get() == 0);

    // The problem here is that we need the server to free its handlers to handle all operations
    while (done.get() != nbThread){
      Thread.sleep(1);
    }

    HTable ht = new HTable(conf, tableName);
    Result r = ht.get(new Get(row1));
    Assert.assertFalse(r.isEmpty());
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    util.shutdownMiniCluster();
  }
}
