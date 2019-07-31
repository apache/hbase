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
import static org.junit.Assert.assertNotNull;
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
public class TestServerLoad {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestServerLoad.class);

  @Test
  public void testRegionLoadAggregation() {
    ServerLoad sl = new ServerLoad(ServerName.valueOf("localhost,1,1"), createServerLoadProto());
    assertEquals(13, sl.getStores());
    assertEquals(114, sl.getStorefiles());
    assertEquals(129, sl.getStoreUncompressedSizeMB());
    assertEquals(504, sl.getRootIndexSizeKB());
    assertEquals(820, sl.getStorefileSizeMB());
    assertEquals(82, sl.getStorefileIndexSizeKB());
    assertEquals(((long) Integer.MAX_VALUE) * 2, sl.getReadRequestsCount());
    assertEquals(300, sl.getFilteredReadRequestsCount());
  }

  @Test
  public void testToString() {
    ServerLoad sl = new ServerLoad(ServerName.valueOf("localhost,1,1"), createServerLoadProto());
    String slToString = sl.toString();
    assertNotNull(sl.obtainServerLoadPB());
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
    ServerLoad sl = new ServerLoad(ServerName.valueOf("localhost,1,1"), createServerLoadProto());
    assertNotNull(sl.obtainServerLoadPB());
    long totalCount = ((long) Integer.MAX_VALUE) * 2;
    assertEquals(totalCount, sl.getReadRequestsCount());
    assertEquals(totalCount, sl.getWriteRequestsCount());
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
        .setReadRequestsCount(Integer.MAX_VALUE).setWriteRequestsCount(Integer.MAX_VALUE).build();
    ClusterStatusProtos.RegionLoad rlTwo =
      ClusterStatusProtos.RegionLoad.newBuilder().setRegionSpecifier(rSpecTwo).setStores(3)
        .setStorefiles(13).setStoreUncompressedSizeMB(23).setStorefileSizeMB(300)
        .setFilteredReadRequestsCount(200).setStorefileIndexSizeKB(40).setRootIndexSizeKB(303)
        .setReadRequestsCount(Integer.MAX_VALUE).setWriteRequestsCount(Integer.MAX_VALUE).build();

    ClusterStatusProtos.ServerLoad sl =
      ClusterStatusProtos.ServerLoad.newBuilder().addRegionLoads(rlOne).
        addRegionLoads(rlTwo).build();
    return sl;
  }

}
