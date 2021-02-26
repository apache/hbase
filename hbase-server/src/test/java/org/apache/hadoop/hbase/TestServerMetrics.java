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
package org.apache.hadoop.hbase;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hbase.thirdparty.com.google.protobuf.ByteString;

import org.apache.hadoop.hbase.shaded.protobuf.generated.ClusterStatusProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;

@Category({ MiscTests.class, SmallTests.class })
public class TestServerMetrics {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestServerMetrics.class);

  @Test
  public void testRegionLoadAggregation() {
    ServerMetrics metrics = ServerMetricsBuilder.toServerMetrics(
        ServerName.valueOf("localhost,1,1"), createServerLoadProto());
    assertEquals(13,
        metrics.getRegionMetrics().values().stream().mapToInt(v -> v.getStoreCount()).sum());
    assertEquals(114,
        metrics.getRegionMetrics().values().stream().mapToInt(v -> v.getStoreFileCount()).sum());
    assertEquals(129, metrics.getRegionMetrics().values().stream()
        .mapToDouble(v -> v.getUncompressedStoreFileSize().get(Size.Unit.MEGABYTE)).sum(), 0);
    assertEquals(504, metrics.getRegionMetrics().values().stream()
        .mapToDouble(v -> v.getStoreFileRootLevelIndexSize().get(Size.Unit.KILOBYTE)).sum(), 0);
    assertEquals(820, metrics.getRegionMetrics().values().stream()
        .mapToDouble(v -> v.getStoreFileSize().get(Size.Unit.MEGABYTE)).sum(), 0);
    assertEquals(82, metrics.getRegionMetrics().values().stream()
        .mapToDouble(v -> v.getStoreFileIndexSize().get(Size.Unit.KILOBYTE)).sum(), 0);
    assertEquals(((long) Integer.MAX_VALUE) * 2,
        metrics.getRegionMetrics().values().stream().mapToLong(v -> v.getReadRequestCount()).sum());
    assertEquals(300,
        metrics.getRegionMetrics().values().stream().mapToLong(v -> v.getFilteredReadRequestCount())
            .sum());
  }

  @Test
  public void testToString() {
    ServerMetrics metrics = ServerMetricsBuilder.toServerMetrics(
        ServerName.valueOf("localhost,1,1"), createServerLoadProto());
    String slToString = metrics.toString();
    assertTrue(slToString.contains("numberOfStores=13"));
    assertTrue(slToString.contains("numberOfStorefiles=114"));
    assertTrue(slToString.contains("storefileUncompressedSizeMB=129"));
    assertTrue(slToString.contains("storefileSizeMB=820"));
    assertTrue(slToString.contains("rootIndexSizeKB=504"));
    assertTrue(slToString.contains("coprocessors=[]"));
    assertTrue(slToString.contains("filteredReadRequestsCount=300"));
  }

  @Test
  public void testRegionLoadWrapAroundAggregation() {
    ServerMetrics metrics = ServerMetricsBuilder.toServerMetrics(
        ServerName.valueOf("localhost,1,1"), createServerLoadProto());
    long totalCount = ((long) Integer.MAX_VALUE) * 2;
    assertEquals(totalCount,
        metrics.getRegionMetrics().values().stream().mapToLong(v -> v.getReadRequestCount()).sum());
    assertEquals(totalCount,
        metrics.getRegionMetrics().values().stream().mapToLong(v -> v.getWriteRequestCount())
            .sum());
  }

  private ClusterStatusProtos.ServerLoad createServerLoadProto() {
    HBaseProtos.RegionSpecifier rSpecOne = HBaseProtos.RegionSpecifier.newBuilder()
        .setType(HBaseProtos.RegionSpecifier.RegionSpecifierType.ENCODED_REGION_NAME)
        .setValue(ByteString.copyFromUtf8("ASDFGQWERT")).build();
    HBaseProtos.RegionSpecifier rSpecTwo = HBaseProtos.RegionSpecifier.newBuilder()
        .setType(HBaseProtos.RegionSpecifier.RegionSpecifierType.ENCODED_REGION_NAME)
        .setValue(ByteString.copyFromUtf8("QWERTYUIOP")).build();

    ClusterStatusProtos.RegionLoad rlOne =
        ClusterStatusProtos.RegionLoad.newBuilder().setRegionSpecifier(rSpecOne).setStores(10)
            .setStorefiles(101).setStoreUncompressedSizeMB(106).setStorefileSizeMB(520)
            .setFilteredReadRequestsCount(100).setStorefileIndexSizeKB(42).setRootIndexSizeKB(201)
            .setReadRequestsCount(Integer.MAX_VALUE).setWriteRequestsCount(Integer.MAX_VALUE)
            .build();
    ClusterStatusProtos.RegionLoad rlTwo =
        ClusterStatusProtos.RegionLoad.newBuilder().setRegionSpecifier(rSpecTwo).setStores(3)
            .setStorefiles(13).setStoreUncompressedSizeMB(23).setStorefileSizeMB(300)
            .setFilteredReadRequestsCount(200).setStorefileIndexSizeKB(40).setRootIndexSizeKB(303)
            .setReadRequestsCount(Integer.MAX_VALUE).setWriteRequestsCount(Integer.MAX_VALUE)
            .build();

    ClusterStatusProtos.ServerLoad sl =
        ClusterStatusProtos.ServerLoad.newBuilder().addRegionLoads(rlOne).
            addRegionLoads(rlTwo).build();
    return sl;
  }

}
