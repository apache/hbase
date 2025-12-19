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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ RegionServerTests.class, SmallTests.class })
public class TestMetricsRegionWrapperTableDescriptorHash {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestMetricsRegionWrapperTableDescriptorHash.class);

  private HBaseTestingUtility testUtil;
  private Configuration conf;

  @Before
  public void setUp() throws Exception {
    conf = HBaseConfiguration.create();
    testUtil = new HBaseTestingUtility(conf);
  }

  @After
  public void tearDown() throws Exception {
    if (testUtil != null) {
      testUtil.cleanupTestDir();
    }
  }

  @Test
  public void testTableDescriptorHashGeneration() throws Exception {
    TableName tableName = TableName.valueOf("testTable");
    TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(tableName)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of("cf")).build();

    RegionInfo regionInfo = RegionInfoBuilder.newBuilder(tableName).setStartKey(Bytes.toBytes("a"))
      .setEndKey(Bytes.toBytes("z")).build();

    Path testDir = testUtil.getDataTestDir("testTableDescriptorHashGeneration");
    HRegion region =
      HBaseTestingUtility.createRegionAndWAL(regionInfo, testDir, conf, tableDescriptor);

    try (MetricsRegionWrapperImpl wrapper = new MetricsRegionWrapperImpl(region)) {
      String hash = wrapper.getTableDescriptorHash();
      assertNotNull(hash);
      assertNotEquals("unknown", hash);
      assertEquals(8, hash.length());
    } finally {
      HBaseTestingUtility.closeRegionAndWAL(region);
    }
  }

  @Test
  public void testHashConsistency() throws Exception {
    TableName tableName = TableName.valueOf("testTable2");
    TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(tableName)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of("cf")).build();

    RegionInfo regionInfo1 = RegionInfoBuilder.newBuilder(tableName).setStartKey(Bytes.toBytes("a"))
      .setEndKey(Bytes.toBytes("m")).build();
    RegionInfo regionInfo2 = RegionInfoBuilder.newBuilder(tableName).setStartKey(Bytes.toBytes("m"))
      .setEndKey(Bytes.toBytes("z")).build();

    Path testDir1 = testUtil.getDataTestDir("testHashConsistency1");
    HRegion region1 =
      HBaseTestingUtility.createRegionAndWAL(regionInfo1, testDir1, conf, tableDescriptor);

    Path testDir2 = testUtil.getDataTestDir("testHashConsistency2");
    HRegion region2 =
      HBaseTestingUtility.createRegionAndWAL(regionInfo2, testDir2, conf, tableDescriptor);
    try (MetricsRegionWrapperImpl wrapper1 = new MetricsRegionWrapperImpl(region1);
      MetricsRegionWrapperImpl wrapper2 = new MetricsRegionWrapperImpl(region2)) {

      String hash1 = wrapper1.getTableDescriptorHash();
      String hash2 = wrapper2.getTableDescriptorHash();

      assertEquals(hash1, hash2);
    } finally {
      HBaseTestingUtility.closeRegionAndWAL(region1);
      HBaseTestingUtility.closeRegionAndWAL(region2);
    }
  }

  @Test
  public void testHashChangeOnDescriptorChange() throws Exception {
    TableName tableName = TableName.valueOf("testTable3");
    TableDescriptor tableDescriptor1 = TableDescriptorBuilder.newBuilder(tableName)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of("cf")).build();
    TableDescriptor tableDescriptor2 = TableDescriptorBuilder.newBuilder(tableName)
      .setColumnFamily(
        ColumnFamilyDescriptorBuilder.newBuilder("cf".getBytes()).setTimeToLive(86400).build())
      .build();

    RegionInfo regionInfo1 = RegionInfoBuilder.newBuilder(tableName).setStartKey(Bytes.toBytes("a"))
      .setEndKey(Bytes.toBytes("m")).build();
    RegionInfo regionInfo2 = RegionInfoBuilder.newBuilder(tableName).setStartKey(Bytes.toBytes("m"))
      .setEndKey(Bytes.toBytes("z")).build();

    Path testDir1 = testUtil.getDataTestDir("testHashChangeOnDescriptorChange1");
    HRegion region1 =
      HBaseTestingUtility.createRegionAndWAL(regionInfo1, testDir1, conf, tableDescriptor1);

    Path testDir2 = testUtil.getDataTestDir("testHashChangeOnDescriptorChange2");
    HRegion region2 =
      HBaseTestingUtility.createRegionAndWAL(regionInfo2, testDir2, conf, tableDescriptor2);

    try (MetricsRegionWrapperImpl wrapper1 = new MetricsRegionWrapperImpl(region1);
      MetricsRegionWrapperImpl wrapper2 = new MetricsRegionWrapperImpl(region2)) {
      String hash1 = wrapper1.getTableDescriptorHash();
      String hash2 = wrapper2.getTableDescriptorHash();

      assertNotEquals(hash1, hash2);
    } finally {
      HBaseTestingUtility.closeRegionAndWAL(region1);
      HBaseTestingUtility.closeRegionAndWAL(region2);
    }
  }
}
