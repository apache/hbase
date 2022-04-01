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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.StartTestingClusterOption;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNameTestRule;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.executor.ExecutorType;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.ServerRegionReplicaUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ RegionServerTests.class, MediumTests.class })
public class TestRegionReplicaWaitForPrimaryFlushConf {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestRegionReplicaWaitForPrimaryFlushConf.class);

  private static final byte[] FAMILY = Bytes.toBytes("family_test");

  private TableName tableName;

  @Rule
  public final TableNameTestRule name = new TableNameTestRule();
  private static final HBaseTestingUtil HTU = new HBaseTestingUtil();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    Configuration conf = HTU.getConfiguration();
    conf.setBoolean(ServerRegionReplicaUtil.REGION_REPLICA_REPLICATION_CONF_KEY, true);
    conf.setBoolean(ServerRegionReplicaUtil.REGION_REPLICA_WAIT_FOR_PRIMARY_FLUSH_CONF_KEY, false);
    HTU.startMiniCluster(StartTestingClusterOption.builder().numRegionServers(2).build());

  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    HTU.shutdownMiniCluster();
  }

  /**
   * This test is for HBASE-26811,before HBASE-26811,when
   * {@link ServerRegionReplicaUtil#REGION_REPLICA_WAIT_FOR_PRIMARY_FLUSH_CONF_KEY} is false and set
   * {@link TableDescriptorBuilder#setRegionMemStoreReplication} to true explicitly,the secondary
   * replica would be disabled for read after open,after HBASE-26811,the secondary replica would be
   * enabled for read after open.
   */
  @Test
  public void testSecondaryReplicaReadEnabled() throws Exception {
    tableName = name.getTableName();
    TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(tableName)
        .setRegionReplication(2).setColumnFamily(ColumnFamilyDescriptorBuilder.of(FAMILY))
        .setRegionMemStoreReplication(true).build();
    HTU.getAdmin().createTable(tableDescriptor);

    final ArrayList<Pair<HRegion, HRegionServer>> regionAndRegionServers =
        new ArrayList<Pair<HRegion, HRegionServer>>(Arrays.asList(null, null));

    for (int i = 0; i < 2; i++) {
      HRegionServer rs = HTU.getMiniHBaseCluster().getRegionServer(i);
      List<HRegion> onlineRegions = rs.getRegions(tableName);
      for (HRegion region : onlineRegions) {
        int replicaId = region.getRegionInfo().getReplicaId();
        assertNull(regionAndRegionServers.get(replicaId));
        regionAndRegionServers.set(replicaId, new Pair<HRegion, HRegionServer>(region, rs));
      }
    }
    for (Pair<HRegion, HRegionServer> pair : regionAndRegionServers) {
      assertNotNull(pair);
    }

    HRegionServer secondaryRs = regionAndRegionServers.get(1).getSecond();

    try {
      secondaryRs.getExecutorService()
          .getExecutorThreadPool(ExecutorType.RS_REGION_REPLICA_FLUSH_OPS);
      fail();
    } catch (NullPointerException e) {
      assertTrue(e != null);
    }
    HRegion secondaryRegion = regionAndRegionServers.get(1).getFirst();
    assertFalse(
      ServerRegionReplicaUtil.isRegionReplicaWaitForPrimaryFlushEnabled(secondaryRegion.conf));
    assertTrue(secondaryRegion.isReadsEnabled());
  }

}
