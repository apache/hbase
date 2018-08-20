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
package org.apache.hadoop.hbase.master.assignment;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ MasterTests.class, MediumTests.class })
public class TestAssignmentManagerUtil {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestAssignmentManagerUtil.class);

  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  private static TableName TABLE_NAME = TableName.valueOf("AM");

  private static MasterProcedureEnv ENV;

  private static AssignmentManager AM;

  private static int REGION_REPLICATION = 3;

  @BeforeClass
  public static void setUp() throws Exception {
    UTIL.startMiniCluster(1);
    UTIL.getAdmin().balancerSwitch(false, true);
    UTIL.createTable(TableDescriptorBuilder.newBuilder(TABLE_NAME)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of("cf"))
      .setRegionReplication(REGION_REPLICATION).build(), new byte[][] { Bytes.toBytes(0) });
    UTIL.waitTableAvailable(TABLE_NAME);
    HMaster master = UTIL.getMiniHBaseCluster().getMaster();
    ENV = master.getMasterProcedureExecutor().getEnvironment();
    AM = master.getAssignmentManager();
  }

  @After
  public void tearDownAfterTest() throws IOException {
    for (RegionInfo region : UTIL.getAdmin().getRegions(TABLE_NAME)) {
      RegionStateNode regionNode = AM.getRegionStates().getRegionStateNode(region);
      // confirm that we have released the lock
      assertFalse(((ReentrantLock) regionNode.lock).isLocked());
      TransitRegionStateProcedure proc = regionNode.getProcedure();
      if (proc != null) {
        regionNode.unsetProcedure(proc);
      }
    }
  }

  @AfterClass
  public static void tearDown() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  private List<RegionInfo> getPrimaryRegions() throws IOException {
    return UTIL.getAdmin().getRegions(TABLE_NAME).stream()
      .filter(r -> RegionReplicaUtil.isDefaultReplica(r)).collect(Collectors.toList());
  }

  @Test
  public void testCreateUnassignProcedureForSplitFail() throws IOException {
    RegionInfo region = getPrimaryRegions().get(0);
    AM.getRegionStates().getRegionStateNode(region)
      .setProcedure(TransitRegionStateProcedure.unassign(ENV, region));
    try {
      AssignmentManagerUtil.createUnassignProceduresForSplitOrMerge(ENV, Stream.of(region),
        REGION_REPLICATION);
      fail("Should fail as the region is in transition");
    } catch (HBaseIOException e) {
      // expected
    }
  }

  @Test
  public void testCreateUnassignProceduresForMergeFail() throws IOException {
    List<RegionInfo> regions = getPrimaryRegions();
    RegionInfo regionA = regions.get(0);
    RegionInfo regionB = regions.get(1);
    AM.getRegionStates().getRegionStateNode(regionB)
      .setProcedure(TransitRegionStateProcedure.unassign(ENV, regionB));
    try {
      AssignmentManagerUtil.createUnassignProceduresForSplitOrMerge(ENV,
        Stream.of(regionA, regionB), REGION_REPLICATION);
      fail("Should fail as the region is in transition");
    } catch (HBaseIOException e) {
      // expected
    }
    IntStream.range(0, REGION_REPLICATION)
      .mapToObj(i -> RegionReplicaUtil.getRegionInfoForReplica(regionA, i))
      .map(AM.getRegionStates()::getRegionStateNode).forEachOrdered(
        rn -> assertFalse("Should have unset the proc for " + rn, rn.isInTransition()));
  }
}
