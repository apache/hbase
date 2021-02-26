/**
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

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter.ExplainingPredicate;
import org.apache.hadoop.hbase.YouAreDeadException;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil.RegionServerThread;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.apache.zookeeper.KeeperException.SessionExpiredException;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * See HBASE-19929 for more details.
 */
@RunWith(Parameterized.class)
@Category({ RegionServerTests.class, LargeTests.class })
public class TestShutdownWhileWALBroken {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestShutdownWhileWALBroken.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestShutdownWhileWALBroken.class);

  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  private static TableName TABLE_NAME = TableName.valueOf("TestShutdownWhileWALBroken");

  private static byte[] CF = Bytes.toBytes("CF");

  @Parameter
  public String walType;

  @Parameters(name = "{index}: WAL={0}")
  public static List<Object[]> params() {
    return Arrays.asList(new Object[] { "asyncfs" }, new Object[] { "filesystem" });
  }

  public static final class MyRegionServer extends HRegionServer {

    private final CountDownLatch latch = new CountDownLatch(1);

    public MyRegionServer(Configuration conf) throws IOException {
      super(conf);
    }

    @Override
    protected void tryRegionServerReport(long reportStartTime, long reportEndTime)
        throws IOException {
      try {
        super.tryRegionServerReport(reportStartTime, reportEndTime);
      } catch (YouAreDeadException e) {
        LOG.info("Caught YouAreDeadException, ignore", e);
      }
    }

    @Override
    public void abort(String reason, Throwable cause) {
      if (cause instanceof SessionExpiredException) {
        // called from ZKWatcher, let's wait a bit to make sure that we call stop before calling
        // abort.
        try {
          latch.await();
        } catch (InterruptedException e) {
        }
      } else {
        // abort from other classes, usually LogRoller, now we can make progress on abort.
        latch.countDown();
      }
      super.abort(reason, cause);
    }
  }

  @Before
  public void setUp() throws Exception {
    UTIL.getConfiguration().setClass(HConstants.REGION_SERVER_IMPL, MyRegionServer.class,
      HRegionServer.class);
    UTIL.getConfiguration().set(WALFactory.WAL_PROVIDER, walType);
    UTIL.startMiniCluster(2);
  }

  @After
  public void tearDown() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void test() throws Exception {
    UTIL.createMultiRegionTable(TABLE_NAME, CF);
    try (Table table = UTIL.getConnection().getTable(TABLE_NAME)) {
      UTIL.loadTable(table, CF);
    }
    int numRegions = UTIL.getMiniHBaseCluster().getRegions(TABLE_NAME).size();
    RegionServerThread rst0 = UTIL.getMiniHBaseCluster().getRegionServerThreads().get(0);
    RegionServerThread rst1 = UTIL.getMiniHBaseCluster().getRegionServerThreads().get(1);
    HRegionServer liveRS;
    RegionServerThread toKillRSThread;
    if (rst1.getRegionServer().getRegions(TableName.META_TABLE_NAME).isEmpty()) {
      liveRS = rst0.getRegionServer();
      toKillRSThread = rst1;
    } else {
      liveRS = rst1.getRegionServer();
      toKillRSThread = rst0;
    }
    assertTrue(liveRS.getRegions(TABLE_NAME).size() < numRegions);
    UTIL.expireSession(toKillRSThread.getRegionServer().getZooKeeper(), false);
    UTIL.waitFor(30000, new ExplainingPredicate<Exception>() {

      @Override
      public boolean evaluate() throws Exception {
        return liveRS.getRegions(TABLE_NAME).size() == numRegions;
      }

      @Override
      public String explainFailure() throws Exception {
        return "Failover is not finished yet";
      }
    });
    toKillRSThread.getRegionServer().stop("Stop for test");
    // make sure that we can successfully quit
    toKillRSThread.join();
  }
}
