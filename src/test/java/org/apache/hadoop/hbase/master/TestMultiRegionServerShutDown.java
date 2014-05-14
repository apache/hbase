/**
 * Copyright 2010 The Apache Software Foundation
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
package org.apache.hadoop.hbase.master;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HMsg;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HServerInfo;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.master.ProcessServerShutdown.LogSplitResult;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test issues when RegionServer is shut down
 */
@Category(MediumTests.class)
public class TestMultiRegionServerShutDown {
  private static final Log LOG = LogFactory
      .getLog(TestMultiRegionServerShutDown.class);

  private static final int NUM_REGIONSERVER = 5;
  private static final int NUM_DEADREGIONSERVER = 2;
  private static AtomicInteger closedRegionServerNum = new AtomicInteger(0);

  private static final HBaseTestingUtility TEST_UTIL =
    new HBaseTestingUtility();
  private static MiniHBaseCluster cluster;

  @BeforeClass
  public static void beforeAllTests() throws Exception {
    TEST_UTIL.startMiniCluster(NUM_REGIONSERVER);
    cluster = TEST_UTIL.getHBaseCluster();
  }

  @AfterClass
  public static void afterAllTests() throws IOException {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Before
  public void setup() throws IOException {
    TEST_UTIL.ensureSomeRegionServersAvailable(NUM_REGIONSERVER);
  }

  /**
   * Audit the region server shutdown processing
   */
  public static class ShutdownProcessingAuditListerner implements
      RegionServerOperationListener {
    private final HServerAddress serverAddress;
    private boolean firstShutDown = true;

    ShutdownProcessingAuditListerner(final HServerAddress serverAddress) {
      this.serverAddress = serverAddress;
    }

    @Override
    public boolean process(final RegionServerOperation op) {
      return true;
    }

    @Override
    public boolean process(HServerInfo serverInfo, HMsg incomingMsg) {
      return true;
    }

    @Override
    public void processed(RegionServerOperation op) {
      if (op instanceof ProcessServerShutdown) {
        ProcessServerShutdown pss = (ProcessServerShutdown) op;
        if (pss.getDeadServerAddress().equals(this.serverAddress)) {
          if (firstShutDown) {
            closedRegionServerNum.incrementAndGet();
            firstShutDown = false;
          }
          boolean expected = (pss.getLogSplitResult() ==
            LogSplitResult.SUCCESS);
          org.junit.Assert.assertTrue(expected);
        }
      }
    }
  }

  @Test(timeout = 300000)
  public void testShutDownMultipleRegionServer() throws Exception {
    LOG.info("Running testShutDownRegionServerWhileSplittingLog");
    final HMaster master = cluster.getMaster();

    ArrayList<HRegionServer> rsList = new ArrayList<HRegionServer>();
    ArrayList<ShutdownProcessingAuditListerner> listenerList =
      new ArrayList<ShutdownProcessingAuditListerner>();

    for (int i = 0; i < NUM_DEADREGIONSERVER; i++) {
      HRegionServer rs = cluster.getRegionServer(i);
      ShutdownProcessingAuditListerner listener =
        new ShutdownProcessingAuditListerner(
          rs.getHServerInfo().getServerAddress());

      master.getRegionServerOperationQueue()
          .registerRegionServerOperationListener(listener);

      rsList.add(rs);
      listenerList.add(listener);
    }

    try {
      for (int i = 0; i < NUM_DEADREGIONSERVER; i++) {
        cluster.abortRegionServer(i);
      }
      // Wait for processing of the shutdown all server.
      while (closedRegionServerNum.get() != NUM_DEADREGIONSERVER) {
        Thread.sleep(100);
      }
    } finally {
      // remove all the listeners
      for (int i = 0; i < NUM_DEADREGIONSERVER; i++) {
        master.getRegionServerOperationQueue()
            .unregisterRegionServerOperationListener(listenerList.get(i));
      }
    }
  }
}
