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
package org.apache.hadoop.hbase.master.balancer;

import static org.junit.Assert.assertTrue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HDFSBlocksDistribution;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

@Category({ MasterTests.class, SmallTests.class })
public class TestRegionHDFSBlockLocationFinderCacheBound {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRegionHDFSBlockLocationFinderCacheBound.class);
  private long maxSize;

  private static final Random RNG = new Random(); // This test depends on Random#setSeed
  private static TableDescriptor TD;
  private static List<RegionInfo> REGIONS;

  private RegionHDFSBlockLocationFinder finder;

  static HDFSBlocksDistribution generate(RegionInfo region) {
    HDFSBlocksDistribution distribution = new HDFSBlocksDistribution();
    int seed = region.hashCode();
    RNG.setSeed(seed);
    int size = 1 + RNG.nextInt(10);
    for (int i = 0; i < size; i++) {
      distribution.addHostsAndBlockWeight(new String[] { "host-" + i }, 1 + RNG.nextInt(100));
    }
    return distribution;
  }

  @BeforeClass
  public static void setUpBeforeClass() {
    TD = TableDescriptorBuilder.newBuilder(TableName.valueOf("RegionLocationFinder")).build();
    int numRegions = 100;
    REGIONS = new ArrayList<>(numRegions);
    for (int i = 1; i <= numRegions; i++) {
      byte[] startKey = i == 0 ? HConstants.EMPTY_START_ROW : Bytes.toBytes(i);
      byte[] endKey = i == numRegions ? HConstants.EMPTY_BYTE_ARRAY : Bytes.toBytes(i + 1);
      RegionInfo region = RegionInfoBuilder.newBuilder(TD.getTableName()).setStartKey(startKey)
        .setEndKey(endKey).build();
      REGIONS.add(region);
    }
  }

  @Before
  public void setUp() {
    DummyClusterInfoProvider provider = new DummyClusterInfoProvider(null) {

      @Override
      public TableDescriptor getTableDescriptor(TableName tableName) throws IOException {
        return TD;
      }

      @Override
      public List<RegionInfo> getAssignedRegions() {
        return REGIONS;
      }

      @Override
      public HDFSBlocksDistribution computeHDFSBlocksDistribution(Configuration conf,
        TableDescriptor tableDescriptor, RegionInfo regionInfo) throws IOException {
        return generate(regionInfo);
      }
    };

    Configuration conf = HBaseConfiguration.create();

    for (RegionInfo regionInfo : REGIONS) {
      maxSize += (regionInfo.heapSize() +
        generate(regionInfo).heapSize());
    }

    conf.setLong("hbase.master.balancer.regionBlockLocation.cache.max.size", maxSize);
    finder = new RegionHDFSBlockLocationFinder(conf);
    finder.setClusterInfoProvider(provider);
  }

  @Test
  public void testCacheMaxBound() {
    finder.getCache().invalidateAll();

    for (RegionInfo regionInfo : REGIONS) {
      finder.getCache().put(regionInfo, generate(regionInfo));
    }

    long actualSize = 0;
    for (Map.Entry<RegionInfo, HDFSBlocksDistribution> entry: finder.getCache().asMap().entrySet()) {
      actualSize += entry.getKey().heapSize() + entry.getValue().heapSize();
    }
    assertTrue(actualSize < maxSize);
  }
}
