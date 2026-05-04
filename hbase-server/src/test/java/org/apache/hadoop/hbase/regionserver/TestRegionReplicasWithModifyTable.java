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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.RegionSplitter;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.Parameter;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@Tag(RegionServerTests.TAG)
@Tag(MediumTests.TAG)
@ParameterizedClass(name = "{index}: disableBeforeModifying={0}")
@MethodSource("parameters")
public class TestRegionReplicasWithModifyTable {
  private static final int NB_SERVERS = 3;

  private static final HBaseTestingUtil HTU = new HBaseTestingUtil();
  private static final byte[] f = HConstants.CATALOG_FAMILY;

  @Parameter
  private boolean disableBeforeModifying;

  private TableName tableName;

  public static Stream<Arguments> parameters() {
    return Stream.of(Arguments.of(true), Arguments.of(false));
  }

  @BeforeAll
  public static void before() throws Exception {
    HTU.startMiniCluster(NB_SERVERS);
  }

  @BeforeEach
  public void setUp(TestInfo testInfo) {
    tableName = TableName.valueOf(testInfo.getTestMethod().get().getName());
  }

  private void enableReplicationByModification(boolean withReplica, int initialReplicaCount,
    int enableReplicaCount, int splitCount) throws IOException, InterruptedException {
    TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(tableName);
    if (withReplica) {
      builder.setRegionReplication(initialReplicaCount);
    }
    TableDescriptor htd = builder.build();
    if (splitCount > 0) {
      byte[][] splits = getSplits(splitCount);
      HTU.createTable(htd, new byte[][] { f }, splits, new Configuration(HTU.getConfiguration()));
    } else {
      HTU.createTable(htd, new byte[][] { f }, (byte[][]) null,
        new Configuration(HTU.getConfiguration()));
    }
    if (disableBeforeModifying) {
      HTU.getAdmin().disableTable(tableName);
    }
    HBaseTestingUtil.setReplicas(HTU.getAdmin(), tableName, enableReplicaCount);
    if (disableBeforeModifying) {
      HTU.getAdmin().enableTable(tableName);
    }
    int expectedRegionCount;
    if (splitCount > 0) {
      expectedRegionCount = enableReplicaCount * splitCount;
    } else {
      expectedRegionCount = enableReplicaCount;
    }
    assertTotalRegions(expectedRegionCount);
  }

  private static byte[][] getSplits(int numRegions) {
    RegionSplitter.UniformSplit split = new RegionSplitter.UniformSplit();
    split.setFirstRow(Bytes.toBytes(0L));
    split.setLastRow(Bytes.toBytes(Long.MAX_VALUE));
    return split.split(numRegions);
  }

  @AfterAll
  public static void afterClass() throws Exception {
    HTU.shutdownMiniCluster();
  }

  @AfterEach
  public void tearDown() throws IOException {
    HTU.getAdmin().disableTable(tableName);
    HTU.getAdmin().deleteTable(tableName);
  }

  private void assertTotalRegions(int expected) {
    int actual = HTU.getHBaseCluster().getRegions(tableName).size();
    assertEquals(expected, actual);
  }

  @Test
  public void testRegionReplicasUsingEnableTable() throws Exception {
    enableReplicationByModification(false, 0, 3, 0);
  }

  @Test
  public void testRegionReplicasUsingEnableTableForMultipleRegions() throws Exception {
    enableReplicationByModification(false, 0, 3, 10);
  }

  @Test
  public void testRegionReplicasByEnableTableWhenReplicaCountIsIncreased() throws Exception {
    enableReplicationByModification(true, 2, 3, 0);
  }

  @Test
  public void testRegionReplicasByEnableTableWhenReplicaCountIsDecreased() throws Exception {
    enableReplicationByModification(true, 3, 2, 0);
  }

  @Test
  public void testRegionReplicasByEnableTableWhenReplicaCountIsDecreasedWithMultipleRegions()
    throws Exception {
    enableReplicationByModification(true, 3, 2, 20);
  }

  @Test
  public void testRegionReplicasByEnableTableWhenReplicaCountIsIncreasedWithMultipleRegions()
    throws Exception {
    enableReplicationByModification(true, 2, 3, 15);
  }
}
