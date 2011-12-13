/*
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
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.coprocessor;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperListener;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperNodeTracker;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.*;

/**
 * Tests unhandled exceptions thrown by coprocessors running on a regionserver..
 * Expected result is that the regionserver will abort with an informative
 * error message describing the set of its loaded coprocessors for crash
 * diagnosis. (HBASE-4014).
 */
@Category(MediumTests.class)
public class TestRegionServerCoprocessorExceptionWithAbort {
  static final Log LOG = LogFactory.getLog(TestRegionObserverInterface.class);

  private class zkwAbortable implements Abortable {
    @Override
    public void abort(String why, Throwable e) {
      throw new RuntimeException("Fatal ZK rs tracker error, why=", e);
    }
    @Override
    public boolean isAborted() {
      return false;
    }
  };

  private class RSTracker extends ZooKeeperNodeTracker {
    public boolean regionZKNodeWasDeleted = false;
    public String rsNode;
    private Thread mainThread;

    public RSTracker(ZooKeeperWatcher zkw, String rsNode, Thread mainThread) {
      super(zkw, rsNode, new zkwAbortable());
      this.rsNode = rsNode;
      this.mainThread = mainThread;
    }

    @Override
    public synchronized void nodeDeleted(String path) {
      if (path.equals(rsNode)) {
        regionZKNodeWasDeleted = true;
        mainThread.interrupt();
      }
    }
  }
  private static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  static final int timeout = 30000;

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    // set configure to indicate which cp should be loaded
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.set(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY,
        BuggyRegionObserver.class.getName());
    conf.set("hbase.coprocessor.abortonerror", "true");
    TEST_UTIL.startMiniCluster(2);
  }

  @AfterClass
  public static void teardownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testExceptionFromCoprocessorDuringPut()
      throws IOException {
    // When we try to write to TEST_TABLE, the buggy coprocessor will
    // cause a NullPointerException, which will cause the regionserver (which
    // hosts the region we attempted to write to) to abort.
    byte[] TEST_TABLE = Bytes.toBytes("observed_table");
    byte[] TEST_FAMILY = Bytes.toBytes("aaa");

    HTable table = TEST_UTIL.createTable(TEST_TABLE, TEST_FAMILY);
    TEST_UTIL.waitUntilAllRegionsAssigned(
        TEST_UTIL.createMultiRegions(table, TEST_FAMILY));

    // Note which regionServer will abort (after put is attempted).
    final HRegionServer regionServer =
        TEST_UTIL.getRSForFirstRegionInTable(TEST_TABLE);

    // add watch so we can know when this regionserver aborted.
    ZooKeeperWatcher zkw = new ZooKeeperWatcher(TEST_UTIL.getConfiguration(),
        "unittest", new zkwAbortable());

    RSTracker rsTracker = new RSTracker(zkw,
        "/hbase/rs/"+regionServer.getServerName(), Thread.currentThread());
    rsTracker.start();
    zkw.registerListener(rsTracker);

    boolean caughtInterruption = false;
    try {
      final byte[] ROW = Bytes.toBytes("aaa");
      Put put = new Put(ROW);
      put.add(TEST_FAMILY, ROW, ROW);
      table.put(put);
    } catch (IOException e) {
      // Depending on exact timing of the threads involved, zkw's interruption
      // might be caught here ...
      if (e.getCause().getClass().equals(InterruptedException.class)) {
	LOG.debug("caught interruption here (during put()).");
        caughtInterruption = true;
      } else {
        fail("put() failed: " + e);
      }
    }
    if (caughtInterruption == false) {
      try {
        Thread.sleep(timeout);
        fail("RegionServer did not abort within 30 seconds.");
      } catch (InterruptedException e) {
        // .. or it might be caught here.
	LOG.debug("caught interruption here (during sleep()).");
        caughtInterruption = true;
      }
    }
    assertTrue("Main thread caught interruption.",caughtInterruption);
    assertTrue("RegionServer aborted on coprocessor exception, as expected.",
        rsTracker.regionZKNodeWasDeleted);
    table.close();
  }

  public static class BuggyRegionObserver extends SimpleRegionObserver {
    @Override
    public void prePut(final ObserverContext<RegionCoprocessorEnvironment> c,
                       final Put put, final WALEdit edit,
                       final boolean writeToWAL) {
      String tableName =
          c.getEnvironment().getRegion().getRegionInfo().getTableNameAsString();
      if (tableName.equals("observed_table")) {
        Integer i = null;
        i = i + 1;
      }
    }
  }

  @org.junit.Rule
  public org.apache.hadoop.hbase.ResourceCheckerJUnitRule cu =
    new org.apache.hadoop.hbase.ResourceCheckerJUnitRule();
}

