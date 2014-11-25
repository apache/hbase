/*
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.RegionMergeTransaction;
import org.apache.hadoop.hbase.regionserver.RegionServerCoprocessorHost;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Tests invocation of the {@link org.apache.hadoop.hbase.coprocessor.RegionServerObserver}
 * interface hooks at all appropriate times during normal HMaster operations.
 */
@Category(MediumTests.class)
public class TestRegionServerObserver {
  private static final Log LOG = LogFactory.getLog(TestRegionServerObserver.class);

  /**
   * Test verifies the hooks in regions merge.
   * @throws Exception
   */
  @Test
  public void testCoprocessorHooksInRegionsMerge() throws Exception {
    final int NUM_MASTERS = 1;
    final int NUM_RS = 1;
    final String TABLENAME = "testRegionServerObserver";
    final String TABLENAME2 = "testRegionServerObserver_2";
    final byte[] FAM = Bytes.toBytes("fam");

    // Create config to use for this cluster
    Configuration conf = HBaseConfiguration.create();
    conf.setClass("hbase.coprocessor.regionserver.classes", CPRegionServerObserver.class,
      RegionServerObserver.class);

    // Start the cluster
    HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility(conf);
    TEST_UTIL.startMiniCluster(NUM_MASTERS, NUM_RS);
    Admin admin = new HBaseAdmin(conf);
    try {
      MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
      HRegionServer regionServer = cluster.getRegionServer(0);
      RegionServerCoprocessorHost cpHost = regionServer.getRegionServerCoprocessorHost();
      Coprocessor coprocessor = cpHost.findCoprocessor(CPRegionServerObserver.class.getName());
      CPRegionServerObserver regionServerObserver = (CPRegionServerObserver) coprocessor;
      HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(TABLENAME));
      desc.addFamily(new HColumnDescriptor(FAM));
      admin.createTable(desc, new byte[][] { Bytes.toBytes("row") });
      desc = new HTableDescriptor(TableName.valueOf(TABLENAME2));
      desc.addFamily(new HColumnDescriptor(FAM));
      admin.createTable(desc, new byte[][] { Bytes.toBytes("row") });
      assertFalse(regionServerObserver.wasRegionMergeCalled());
      List<HRegion> regions = regionServer.getOnlineRegions(TableName.valueOf(TABLENAME));
      admin.mergeRegions(regions.get(0).getRegionInfo().getEncodedNameAsBytes(), regions.get(1)
          .getRegionInfo().getEncodedNameAsBytes(), true);
      int regionsCount = regionServer.getOnlineRegions(TableName.valueOf(TABLENAME)).size();
      while (regionsCount != 1) {
        regionsCount = regionServer.getOnlineRegions(TableName.valueOf(TABLENAME)).size();
        Thread.sleep(1000);
      }
      assertTrue(regionServerObserver.wasRegionMergeCalled());
      assertTrue(regionServerObserver.wasPreMergeCommit());
      assertTrue(regionServerObserver.wasPostMergeCommit());
      assertEquals(regionsCount, 1);
      assertEquals(regionServer.getOnlineRegions(TableName.valueOf(TABLENAME2)).size(), 1);
    } finally {
      if (admin != null) admin.close();
      TEST_UTIL.shutdownMiniCluster();
    }
  }

  public static class CPRegionServerObserver extends BaseRegionServerObserver {
    private RegionMergeTransaction rmt = null;
    private HRegion mergedRegion = null;

    private boolean preMergeCalled;
    private boolean preMergeBeforePONRCalled;
    private boolean preMergeAfterPONRCalled;
    private boolean preRollBackMergeCalled;
    private boolean postRollBackMergeCalled;
    private boolean postMergeCalled;

    public void resetStates() {
      preMergeCalled = false;
      preMergeBeforePONRCalled = false;
      preMergeAfterPONRCalled = false;
      preRollBackMergeCalled = false;
      postRollBackMergeCalled = false;
      postMergeCalled = false;
    }

    @Override
    public void preMerge(ObserverContext<RegionServerCoprocessorEnvironment> ctx, HRegion regionA,
        HRegion regionB) throws IOException {
      preMergeCalled = true;
    }

    @Override
    public void preMergeCommit(ObserverContext<RegionServerCoprocessorEnvironment> ctx,
        HRegion regionA, HRegion regionB, List<Mutation> metaEntries) throws IOException {
      preMergeBeforePONRCalled = true;
      RegionServerCoprocessorEnvironment environment = ctx.getEnvironment();
      HRegionServer rs = (HRegionServer) environment.getRegionServerServices();
      List<HRegion> onlineRegions =
          rs.getOnlineRegions(TableName.valueOf("testRegionServerObserver_2"));
      rmt = new RegionMergeTransaction(onlineRegions.get(0), onlineRegions.get(1), true);
      if (!rmt.prepare(rs)) {
        LOG.error("Prepare for the region merge of table "
            + onlineRegions.get(0).getTableDesc().getNameAsString()
            + " failed. So returning null. ");
        ctx.bypass();
        return;
      }
      mergedRegion = rmt.stepsBeforePONR(rs, rs, false);
      rmt.prepareMutationsForMerge(mergedRegion.getRegionInfo(), regionA.getRegionInfo(),
        regionB.getRegionInfo(), rs.getServerName(), metaEntries);
      MetaTableAccessor.mutateMetaTable(rs.getConnection(), metaEntries);
    }

    @Override
    public void postMergeCommit(ObserverContext<RegionServerCoprocessorEnvironment> ctx,
        HRegion regionA, HRegion regionB, HRegion mr) throws IOException {
      preMergeAfterPONRCalled = true;
      RegionServerCoprocessorEnvironment environment = ctx.getEnvironment();
      HRegionServer rs = (HRegionServer) environment.getRegionServerServices();
      rmt.stepsAfterPONR(rs, rs, this.mergedRegion);
    }

    @Override
    public void preRollBackMerge(ObserverContext<RegionServerCoprocessorEnvironment> ctx,
        HRegion regionA, HRegion regionB) throws IOException {
      preRollBackMergeCalled = true;
    }

    @Override
    public void postRollBackMerge(ObserverContext<RegionServerCoprocessorEnvironment> ctx,
        HRegion regionA, HRegion regionB) throws IOException {
      postRollBackMergeCalled = true;
    }

    @Override
    public void postMerge(ObserverContext<RegionServerCoprocessorEnvironment> c, HRegion regionA,
        HRegion regionB, HRegion mergedRegion) throws IOException {
      postMergeCalled = true;
    }

    public boolean wasPreMergeCalled() {
      return this.preMergeCalled;
    }

    public boolean wasPostMergeCalled() {
      return this.postMergeCalled;
    }

    public boolean wasPreMergeCommit() {
      return this.preMergeBeforePONRCalled;
    }

    public boolean wasPostMergeCommit() {
      return this.preMergeAfterPONRCalled;
    }

    public boolean wasPreRollBackMerge() {
      return this.preRollBackMergeCalled;
    }

    public boolean wasPostRollBackMerge() {
      return this.postRollBackMergeCalled;
    }

    public boolean wasRegionMergeCalled() {
      return this.preMergeCalled && this.postMergeCalled;
    }

  }

}
