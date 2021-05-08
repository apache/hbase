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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CoordinatedStateManager;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter.ExplainingPredicate;
import org.apache.hadoop.hbase.YouAreDeadException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil.RegionServerThread;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * This testcase is used to ensure that the compaction marker will fail a compaction if the RS is
 * already dead. It can not eliminate FNFE when scanning but it does reduce the possibility a lot.
 */
@Category({ RegionServerTests.class, LargeTests.class })
public class TestCompactionInDeadRegionServer {
  private static final Log LOG = LogFactory.getLog(TestCompactionInDeadRegionServer.class);

  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  private static final TableName TABLE_NAME = TableName.valueOf("test");

  private static final byte[] CF = Bytes.toBytes("cf");

  private static final byte[] CQ = Bytes.toBytes("cq");

  public static final class IgnoreYouAreDeadRS extends HRegionServer {

    public IgnoreYouAreDeadRS(Configuration conf) throws IOException, InterruptedException {
      super(conf);
    }

    public IgnoreYouAreDeadRS(Configuration conf, CoordinatedStateManager csm)
        throws IOException, InterruptedException {
      super(conf, csm);
    }

    @Override
    protected void tryRegionServerReport(long reportStartTime, long reportEndTime)
        throws IOException {
      try {
        super.tryRegionServerReport(reportStartTime, reportEndTime);
      } catch (YouAreDeadException e) {
        // ignore, do not abort
      }
    }
  }

  @Before
  public void setUp() throws Exception {
    UTIL.getConfiguration().setInt(HConstants.ZK_SESSION_TIMEOUT, 2000);
    UTIL.getConfiguration().setClass(HConstants.REGION_SERVER_IMPL, IgnoreYouAreDeadRS.class,
      HRegionServer.class);
    UTIL.startMiniCluster(2);
    Table table = UTIL.createTable(TABLE_NAME, CF);
    for (int i = 0; i < 10; i++) {
      table.put(new Put(Bytes.toBytes(i)).addColumn(CF, CQ, Bytes.toBytes(i)));
    }
    UTIL.getHBaseAdmin().flush(TABLE_NAME);
    for (int i = 10; i < 20; i++) {
      table.put(new Put(Bytes.toBytes(i)).addColumn(CF, CQ, Bytes.toBytes(i)));
    }
    UTIL.getHBaseAdmin().flush(TABLE_NAME);
  }

  @After
  public void tearDown() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void test() throws Exception {
    HRegionServer regionSvr = UTIL.getRSForFirstRegionInTable(TABLE_NAME);
    Region region = regionSvr.getOnlineRegions(TABLE_NAME).get(0);
    String regName = region.getRegionInfo().getEncodedName();
    List<Region> metaRegs = regionSvr.getOnlineRegions(TableName.META_TABLE_NAME);
    if (metaRegs != null && !metaRegs.isEmpty()) {
      LOG.info("meta is on the same server: " + regionSvr);
      // when region is on same server as hbase:meta, reassigning meta would abort the server
      // since WAL is broken.
      // so the region is moved to a different server
      HRegionServer otherRs = UTIL.getOtherRegionServer(regionSvr);
      UTIL.moveRegionAndWait(region.getRegionInfo(), otherRs.getServerName());
      LOG.info("Moved region: " + regName + " to " + otherRs.getServerName());
    }
    final HRegionServer rsToSuspend = UTIL.getRSForFirstRegionInTable(TABLE_NAME);
    region = (HRegion) rsToSuspend.getOnlineRegions(TABLE_NAME).get(0);
    ZooKeeperWatcher watcher = UTIL.getZooKeeperWatcher();
    watcher.getRecoverableZooKeeper()
        .delete(ZKUtil.joinZNode(watcher.rsZNode, rsToSuspend.getServerName().toString()), -1);
    UTIL.waitFor(60000, 1000, new ExplainingPredicate<Exception>() {

      @Override
      public boolean evaluate() throws Exception {
        for (RegionServerThread thread : UTIL.getHBaseCluster().getRegionServerThreads()) {
          HRegionServer rs = thread.getRegionServer();
          if (rs != rsToSuspend) {
            return !rs.getOnlineRegions(TABLE_NAME).isEmpty();
          }
        }
        return false;
      }

      @Override
      public String explainFailure() throws Exception {
        return "The region for " + TABLE_NAME + " is still on " + rsToSuspend.getServerName();
      }
    });
    try {
      ((HRegion)region).compact(true);
      fail("Should fail as our wal file has already been closed, " +
          "and walDir has also been renamed");
    } catch (Exception e) {
      LOG.debug("expected exception: ", e);
    }
    Table table = UTIL.getConnection().getTable(TABLE_NAME);
    // should not hit FNFE
    for (int i = 0; i < 20; i++) {
      assertEquals(i, Bytes.toInt(table.get(new Get(Bytes.toBytes(i))).getValue(CF, CQ)));
    }
  }
}
