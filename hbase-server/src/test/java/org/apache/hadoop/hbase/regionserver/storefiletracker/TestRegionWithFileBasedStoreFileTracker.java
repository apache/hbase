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
package org.apache.hadoop.hbase.regionserver.storefiletracker;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

@Category({ RegionServerTests.class, MediumTests.class })
public class TestRegionWithFileBasedStoreFileTracker {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRegionWithFileBasedStoreFileTracker.class);

  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  private static final byte[] CF = Bytes.toBytes("cf");

  private static final byte[] CQ = Bytes.toBytes("cq");

  private static final TableDescriptor TD =
    TableDescriptorBuilder.newBuilder(TableName.valueOf("file_based_tracker"))
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(CF)).build();

  private static final RegionInfo RI = RegionInfoBuilder.newBuilder(TD.getTableName()).build();

  @Rule
  public TestName name = new TestName();

  private HRegion region;

  @Before
  public void setUp() throws IOException {
    Configuration conf = new Configuration(UTIL.getConfiguration());
    conf.set(StoreFileTrackerFactory.TRACKER_IMPL, StoreFileTrackerFactory.Trackers.FILE.name());
    region = HBaseTestingUtility.createRegionAndWAL(RI, UTIL.getDataTestDir(name.getMethodName()),
      conf, TD);
  }

  @After
  public void tearDown() throws IOException {
    if (region != null) {
      HBaseTestingUtility.closeRegionAndWAL(region);
    }
    UTIL.cleanupTestDir();
  }

  @Test
  public void testFlushAndCompaction() throws IOException {
    for (int i = 0; i < 10; i++) {
      for (int j = 0; j < 10; j++) {
        int v = i * 10 + j;
        region.put(new Put(Bytes.toBytes(v)).addColumn(CF, CQ, Bytes.toBytes(v)));
      }
      region.flush(true);
      if (i % 3 == 2) {
        region.compact(true);
      }
    }
    // reopen the region, make sure the store file tracker works, i.e, we can get all the records
    // back
    region.close();
    region = HRegion.openHRegion(region, null);
    for (int i = 0; i < 100; i++) {
      Result result = region.get(new Get(Bytes.toBytes(i)));
      assertEquals(i, Bytes.toInt(result.getValue(CF, CQ)));
    }
  }
}
