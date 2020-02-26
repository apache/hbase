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
package org.apache.hadoop.hbase.master.assignment;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessor;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.MasterObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({MasterTests.class, MediumTests.class})
public class TestMasterAbortWhileMergingTable {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestMasterAbortWhileMergingTable.class);

  private static final Logger LOG = LoggerFactory
      .getLogger(TestMasterAbortWhileMergingTable.class);

  protected static final HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private static TableName TABLE_NAME = TableName.valueOf("test");
  private static Admin admin;
  private static byte[] CF = Bytes.toBytes("cf");
  private static byte[] SPLITKEY = Bytes.toBytes("bbbbbbb");
  private static CountDownLatch mergeCommitArrive = new CountDownLatch(1);



  @BeforeClass
  public static void setupCluster() throws Exception {
    UTIL.getConfiguration().set(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY,
        MergeRegionObserver.class.getName());
    UTIL.startMiniCluster(3);
    admin = UTIL.getHBaseAdmin();
    byte[][] splitKeys = new byte[1][];
    splitKeys[0] = SPLITKEY;
    UTIL.createTable(TABLE_NAME, CF, splitKeys);
    UTIL.waitTableAvailable(TABLE_NAME);
  }

  @AfterClass
  public static void cleanupTest() throws Exception {
    try {
      UTIL.shutdownMiniCluster();
    } catch (Exception e) {
      LOG.warn("failure shutting down cluster", e);
    }
  }

  @Test
  public void test() throws Exception {
    List<RegionInfo> regionInfos = admin.getRegions(TABLE_NAME);
    MergeTableRegionsProcedure mergeTableRegionsProcedure = new MergeTableRegionsProcedure(
        UTIL.getMiniHBaseCluster().getMaster().getMasterProcedureExecutor()
            .getEnvironment(), new RegionInfo [] {regionInfos.get(0), regionInfos.get(1)}, false);
    long procID = UTIL.getMiniHBaseCluster().getMaster().getMasterProcedureExecutor()
        .submitProcedure(mergeTableRegionsProcedure);
    mergeCommitArrive.await();
    UTIL.getMiniHBaseCluster().stopMaster(0);
    UTIL.getMiniHBaseCluster().startMaster();
    //wait until master initialized
    UTIL.waitFor(30000,
      () -> UTIL.getMiniHBaseCluster().getMaster() != null && UTIL
        .getMiniHBaseCluster().getMaster().isInitialized());
    UTIL.waitFor(30000, () -> UTIL.getMiniHBaseCluster().getMaster()
      .getMasterProcedureExecutor().isFinished(procID));
    Assert.assertTrue("Found region RIT, that's impossible! " +
      UTIL.getMiniHBaseCluster().getMaster().getAssignmentManager().getRegionsInTransition(),
      UTIL.getMiniHBaseCluster().getMaster().getAssignmentManager()
        .getRegionsInTransition().size() == 0);
  }

  public static class MergeRegionObserver implements MasterCoprocessor,
      MasterObserver {

    @Override
    public Optional<MasterObserver> getMasterObserver() {
      return Optional.of(this);
    }

    @Override
    public void preMergeRegionsCommitAction(
        ObserverContext<MasterCoprocessorEnvironment> ctx,
        RegionInfo[] regionsToMerge, List<Mutation> metaEntries) {
      mergeCommitArrive.countDown();
      LOG.error("mergeCommitArrive countdown");
    }
  }

}
