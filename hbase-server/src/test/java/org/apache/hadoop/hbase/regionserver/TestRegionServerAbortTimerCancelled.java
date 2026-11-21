/*
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

import static org.junit.jupiter.api.Assertions.assertFalse;

import java.util.TimerTask;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.StartTestingClusterOption;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.JVMClusterUtil.RegionServerThread;
import org.apache.hadoop.hbase.util.Threads;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Verify that the abort timer does not fire once an aborted region server has shut down, since it
 * would otherwise halt a JVM that outlives the region server.
 */
@Tag(RegionServerTests.TAG)
@Tag(MediumTests.TAG)
public class TestRegionServerAbortTimerCancelled {

  private static final Logger LOG =
    LoggerFactory.getLogger(TestRegionServerAbortTimerCancelled.class);

  private static final HBaseTestingUtil UTIL = new HBaseTestingUtil();

  private static final long ABORT_TIMEOUT = 10000;

  private static volatile boolean abortTimeoutTaskRan = false;

  @BeforeAll
  public static void setUp() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    conf.setLong(HRegionServer.ABORT_TIMEOUT, ABORT_TIMEOUT);
    conf.set(HRegionServer.ABORT_TIMEOUT_TASK, RecordingAbortTimeoutTask.class.getName());
    UTIL.startMiniCluster(StartTestingClusterOption.builder().numRegionServers(2).build());
  }

  @AfterAll
  public static void tearDown() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void testAbortTimerCancelledAfterShutdown() throws Exception {
    // Abort the region server that does not carry meta, so the cluster stays healthy
    int serverIndex = UTIL.getMiniHBaseCluster().getServerWithMeta() == 0 ? 1 : 0;
    RegionServerThread rsThread =
      UTIL.getMiniHBaseCluster().getRegionServerThreads().get(serverIndex);

    long abortTime = EnvironmentEdgeManager.currentTime();
    rsThread.getRegionServer().abort("Abort RS for test");
    rsThread.join(ABORT_TIMEOUT);
    // Only meaningful if the abort completed before the abort timer was due
    Assumptions.assumeFalse(rsThread.isAlive(), "Region server did not finish aborting in time");
    LOG.info("Region server shut down {} ms after abort",
      EnvironmentEdgeManager.currentTime() - abortTime);

    // Wait past the point where the abort timer would have fired
    long waitUntil = abortTime + ABORT_TIMEOUT + 5000;
    while (EnvironmentEdgeManager.currentTime() < waitUntil) {
      Threads.sleep(500);
    }
    assertFalse(abortTimeoutTaskRan,
      "Abort timeout task ran after the region server had already shut down");
  }

  static class RecordingAbortTimeoutTask extends TimerTask {

    public RecordingAbortTimeoutTask() {
    }

    @Override
    public void run() {
      LOG.info("RecordingAbortTimeoutTask was run");
      abortTimeoutTaskRan = true;
    }
  }
}
