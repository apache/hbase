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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.quotas.RegionSize;
import org.apache.hadoop.hbase.quotas.RegionSizeStore;
import org.apache.hadoop.hbase.quotas.RegionSizeStoreFactory;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.RegionSpaceUse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.RegionSpaceUseReportRequest;

/**
 * Test class for isolated (non-cluster) tests surrounding the report
 * of Region space use to the Master by RegionServers.
 */
@Category(SmallTests.class)
public class TestRegionServerRegionSpaceUseReport {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestRegionServerRegionSpaceUseReport.class);

  @Test
  public void testConversion() {
    TableName tn = TableName.valueOf("table1");

    RegionInfo hri1 = RegionInfoBuilder.newBuilder(tn)
        .setStartKey(Bytes.toBytes("a"))
        .setEndKey(Bytes.toBytes("b"))
        .build();
    RegionInfo hri2 = RegionInfoBuilder.newBuilder(tn)
        .setStartKey(Bytes.toBytes("b"))
        .setEndKey(Bytes.toBytes("c"))
        .build();
    RegionInfo hri3 = RegionInfoBuilder.newBuilder(tn)
        .setStartKey(Bytes.toBytes("c"))
        .setEndKey(Bytes.toBytes("d"))
        .build();
    RegionSizeStore store = RegionSizeStoreFactory.getInstance().createStore();
    store.put(hri1, 1024L * 1024L);
    store.put(hri2, 1024L * 1024L * 8L);
    store.put(hri3, 1024L * 1024L * 32L);

    // Call the real method to convert the map into a protobuf
    HRegionServer rs = mock(HRegionServer.class);
    doCallRealMethod().when(rs).buildRegionSpaceUseReportRequest(any(RegionSizeStore.class));
    doCallRealMethod().when(rs).convertRegionSize(any(), anyLong());

    RegionSpaceUseReportRequest requests = rs.buildRegionSpaceUseReportRequest(store);
    assertEquals(store.size(), requests.getSpaceUseCount());
    for (RegionSpaceUse spaceUse : requests.getSpaceUseList()) {
      RegionInfo hri = ProtobufUtil.toRegionInfo(spaceUse.getRegionInfo());
      RegionSize expectedSize = store.remove(hri);
      assertNotNull("Could not find size for HRI: " + hri, expectedSize);
      assertEquals(expectedSize.getSize(), spaceUse.getRegionSize());
    }
    assertTrue("Should not have any space use entries left: " + store, store.isEmpty());
  }
}
