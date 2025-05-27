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
package org.apache.hadoop.hbase.client;

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.StartMiniClusterOption;
import org.apache.hadoop.hbase.TableDescriptors;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNameTestRule;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.regionserver.StorefileRefresherChore;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ MiscTests.class, MediumTests.class })
public class TestMetaReplicaAssignment {
  protected static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  protected static final int REGIONSERVERS_COUNT = 3;

  @Rule
  public TableNameTestRule name = new TableNameTestRule();

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestMetaReplicaAssignment.class);

  @AfterClass
  public static void tearDown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @BeforeClass
  public static void setUp() throws Exception {
    TEST_UTIL.getConfiguration().setInt("zookeeper.session.timeout", 30000);
    TEST_UTIL.getConfiguration()
      .setInt(StorefileRefresherChore.REGIONSERVER_STOREFILE_REFRESH_PERIOD, 1000);
    StartMiniClusterOption option = StartMiniClusterOption.builder().numAlwaysStandByMasters(1)
      .numMasters(1).numRegionServers(REGIONSERVERS_COUNT).build();
    TEST_UTIL.startMiniCluster(option);
    Admin admin = TEST_UTIL.getAdmin();
    TEST_UTIL.waitFor(30000, () -> TEST_UTIL.getMiniHBaseCluster().getMaster() != null);
    HBaseTestingUtility.setReplicas(admin, TableName.META_TABLE_NAME, 1);
  }

  @Test
  public void testUpgradeSameReplicaCount() throws Exception {
    HMaster oldMaster = TEST_UTIL.getMiniHBaseCluster().getMaster();
    TableDescriptors oldTds = oldMaster.getTableDescriptors();
    TableDescriptor oldMetaTd = oldTds.get(TableName.META_TABLE_NAME);

    assertEquals(1, oldMaster.getAssignmentManager().getRegionStates()
      .getRegionsOfTable(TableName.META_TABLE_NAME).size());
    assertEquals(1, oldMetaTd.getRegionReplication());
    // force update the replica count to 3 and then kill the master, to simulate that
    // HBase storage location is reused, resulting a sane-looking meta and only the
    // RegionInfoBuilder.FIRST_META_REGIONINFO gets assigned in InitMetaProcedure.
    // Meta region replicas are not assigned, but we have replication in meta table descriptor.

    oldTds.update(TableDescriptorBuilder.newBuilder(oldMetaTd).setRegionReplication(3).build());
    oldMaster.stop("Restarting");
    TEST_UTIL.waitFor(30000, () -> oldMaster.isStopped());

    // increase replica count to 3 through Configuration
    TEST_UTIL.getMiniHBaseCluster().getConfiguration().setInt(HConstants.META_REPLICAS_NUM, 3);
    TEST_UTIL.getMiniHBaseCluster().startMaster();
    TEST_UTIL.waitFor(30000,
      () -> TEST_UTIL.getZooKeeperWatcher().getMetaReplicaNodes().size() == 3);
    HMaster newMaster = TEST_UTIL.getMiniHBaseCluster().getMaster();
    assertEquals(3, newMaster.getAssignmentManager().getRegionStates()
      .getRegionsOfTable(TableName.META_TABLE_NAME).size());
    assertEquals(3,
      newMaster.getTableDescriptors().get(TableName.META_TABLE_NAME).getRegionReplication());
  }
}
