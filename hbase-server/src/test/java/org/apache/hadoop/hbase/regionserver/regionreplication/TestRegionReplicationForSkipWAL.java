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
package org.apache.hadoop.hbase.regionserver.regionreplication;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.StartTestingClusterOption;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Consistency;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ServerRegionReplicaUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ RegionServerTests.class, LargeTests.class })
public class TestRegionReplicationForSkipWAL {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRegionReplicationForSkipWAL.class);

  private static final byte[] FAM1 = Bytes.toBytes("family_test1");

  private static final byte[] QUAL1 = Bytes.toBytes("qualifier_test1");

  private static final byte[] FAM2 = Bytes.toBytes("family_test2");

  private static final byte[] QUAL2 = Bytes.toBytes("qualifier_test2");

  private static final byte[] FAM3 = Bytes.toBytes("family_test3");

  private static final byte[] QUAL3 = Bytes.toBytes("qualifier_test3");

  private static final byte[] FAM4 = Bytes.toBytes("family_test4");

  private static final byte[] QUAL4 = Bytes.toBytes("qualifier_test4");

  private static final byte[] FAM5 = Bytes.toBytes("family_test5");

  private static final byte[] QUAL5 = Bytes.toBytes("qualifier_test5");

  private static final byte[] FAM6 = Bytes.toBytes("family_test6");

  private static final byte[] QUAL6 = Bytes.toBytes("qualifier_test6");

  private static final HBaseTestingUtil HTU = new HBaseTestingUtil();
  private static final int NB_SERVERS = 2;

  private static final String strTableName = "TestRegionReplicationForSkipWAL";

  @BeforeClass
  public static void setUp() throws Exception {
    Configuration conf = HTU.getConfiguration();
    conf.setBoolean(ServerRegionReplicaUtil.REGION_REPLICA_REPLICATION_CONF_KEY, true);
    conf.setBoolean(RegionReplicaUtil.REGION_REPLICA_WAIT_FOR_PRIMARY_FLUSH_CONF_KEY, false);
    HTU.startMiniCluster(StartTestingClusterOption.builder().numRegionServers(NB_SERVERS).build());
  }

  @AfterClass
  public static void tearDown() throws Exception {
    HTU.shutdownMiniCluster();
  }

  /**
   * This test is for HBASE-26933,make the new region replication framework introduced by
   * HBASE-26233 work for table which DURABILITY is Durability.SKIP_WAL.
   */
  @Test
  public void testReplicateToReplicaWhenSkipWAL() throws Exception {
    final HRegion[] skipWALRegions = this.createTable(true);
    byte[] rowKey1 = Bytes.toBytes(1);
    byte[] value1 = Bytes.toBytes(2);

    byte[] rowKey2 = Bytes.toBytes(2);
    byte[] value2 = Bytes.toBytes(4);

    // Test the table is skipWAL
    skipWALRegions[0].batchMutate(new Mutation[] { new Put(rowKey1).addColumn(FAM1, QUAL1, value1),
      new Put(rowKey2).addColumn(FAM2, QUAL2, value2) });

    try (Table skipWALTable = HTU.getConnection().getTable(getTableName(true))) {
      HTU.waitFor(30000, () -> checkReplica(skipWALTable, FAM1, QUAL1, rowKey1, value1)
        && checkReplica(skipWALTable, FAM2, QUAL2, rowKey2, value2));
    }

    byte[] rowKey3 = Bytes.toBytes(3);
    byte[] value3 = Bytes.toBytes(6);
    byte[] rowKey4 = Bytes.toBytes(4);
    byte[] value4 = Bytes.toBytes(8);
    byte[] rowKey5 = Bytes.toBytes(5);
    byte[] value5 = Bytes.toBytes(10);
    byte[] rowKey6 = Bytes.toBytes(6);
    byte[] value6 = Bytes.toBytes(12);

    // Test the table is normal,but the Put is skipWAL
    final HRegion[] normalRegions = this.createTable(false);
    normalRegions[0].batchMutate(new Mutation[] { new Put(rowKey3).addColumn(FAM3, QUAL3, value3),
      new Put(rowKey4).addColumn(FAM4, QUAL4, value4).setDurability(Durability.SKIP_WAL),
      new Put(rowKey5).addColumn(FAM5, QUAL5, value5).setDurability(Durability.SKIP_WAL),
      new Put(rowKey6).addColumn(FAM6, QUAL6, value6) });

    try (Table normalTable = HTU.getConnection().getTable(getTableName(false))) {
      HTU.waitFor(30000,
        () -> checkReplica(normalTable, FAM3, QUAL3, rowKey3, value3)
          && checkReplica(normalTable, FAM4, QUAL4, rowKey4, value4)
          && checkReplica(normalTable, FAM5, QUAL5, rowKey5, value5)
          && checkReplica(normalTable, FAM6, QUAL6, rowKey6, value6));
    }
  }

  private static boolean checkReplica(Table table, byte[] fam, byte[] qual, byte[] rowKey,
    byte[] expectValue) throws IOException {
    Get get = new Get(rowKey).setConsistency(Consistency.TIMELINE).setReplicaId(1);
    Result result = table.get(get);
    byte[] value = result.getValue(fam, qual);
    return value != null && value.length > 0 && Arrays.equals(expectValue, value);
  }

  private TableName getTableName(boolean skipWAL) {
    return TableName.valueOf(strTableName + (skipWAL ? "_skipWAL" : ""));
  }

  private HRegion[] createTable(boolean skipWAL) throws Exception {
    TableName tableName = getTableName(skipWAL);
    TableDescriptorBuilder builder =
      TableDescriptorBuilder.newBuilder(tableName).setRegionReplication(NB_SERVERS)
        .setColumnFamilies(Arrays.asList(ColumnFamilyDescriptorBuilder.of(FAM1),
          ColumnFamilyDescriptorBuilder.of(FAM2), ColumnFamilyDescriptorBuilder.of(FAM3),
          ColumnFamilyDescriptorBuilder.of(FAM4), ColumnFamilyDescriptorBuilder.of(FAM5),
          ColumnFamilyDescriptorBuilder.of(FAM6)));
    if (skipWAL) {
      builder.setDurability(Durability.SKIP_WAL);

    }
    TableDescriptor tableDescriptor = builder.build();

    HTU.getAdmin().createTable(tableDescriptor);
    final HRegion[] regions = new HRegion[NB_SERVERS];
    for (int i = 0; i < NB_SERVERS; i++) {
      HRegionServer rs = HTU.getMiniHBaseCluster().getRegionServer(i);
      List<HRegion> onlineRegions = rs.getRegions(tableName);
      for (HRegion region : onlineRegions) {
        int replicaId = region.getRegionInfo().getReplicaId();
        assertTrue(regions[replicaId] == null);
        regions[region.getRegionInfo().getReplicaId()] = region;
      }
    }
    for (Region region : regions) {
      assertNotNull(region);
    }
    return regions;
  }
}
