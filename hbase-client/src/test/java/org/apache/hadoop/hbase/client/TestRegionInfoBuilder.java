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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.MD5Hash;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import org.apache.hbase.thirdparty.com.google.protobuf.UnsafeByteOperations;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;

@Tag(ClientTests.TAG)
@Tag(SmallTests.TAG)
public class TestRegionInfoBuilder {

  private TableName name;

  @BeforeEach
  public void setup(TestInfo testInfo) {
    this.name = TableName.valueOf(testInfo.getTestMethod().get().getName());
  }

  @Test
  public void testBuilder() {
    TableName tn = TableName.valueOf("test");
    RegionInfoBuilder builder = RegionInfoBuilder.newBuilder(tn);
    byte[] startKey = Bytes.toBytes("a");
    builder.setStartKey(startKey);
    byte[] endKey = Bytes.toBytes("z");
    builder.setEndKey(endKey);
    int regionId = 1;
    builder.setRegionId(1);
    int replicaId = 2;
    builder.setReplicaId(replicaId);
    boolean offline = true;
    builder.setOffline(offline);
    boolean isSplit = true;
    builder.setSplit(isSplit);
    RegionInfo ri = builder.build();

    assertEquals(tn, ri.getTable());
    assertArrayEquals(startKey, ri.getStartKey());
    assertArrayEquals(endKey, ri.getEndKey());
    assertEquals(regionId, ri.getRegionId());
    assertEquals(replicaId, ri.getReplicaId());
    assertEquals(offline, ri.isOffline());
    assertEquals(isSplit, ri.isSplit());
  }

  @Test
  public void testPb() throws DeserializationException {
    RegionInfo ri = RegionInfoBuilder.FIRST_META_REGIONINFO;
    byte[] bytes = RegionInfo.toByteArray(ri);
    RegionInfo pbri = RegionInfo.parseFrom(bytes);
    assertTrue(RegionInfo.COMPARATOR.compare(ri, pbri) == 0);
  }

  @Test
  public void testCreateRegionInfoName() throws Exception {
    final TableName tn = name;
    String startKey = "startkey";
    final byte[] sk = Bytes.toBytes(startKey);
    String id = "id";

    // old format region name
    byte[] name = RegionInfo.createRegionName(tn, sk, id, false);
    String nameStr = Bytes.toString(name);
    assertEquals(tn + "," + startKey + "," + id, nameStr);

    // new format region name.
    String md5HashInHex = MD5Hash.getMD5AsHex(name);
    assertEquals(RegionInfo.MD5_HEX_LENGTH, md5HashInHex.length());
    name = RegionInfo.createRegionName(tn, sk, id, true);
    nameStr = Bytes.toString(name);
    assertEquals(tn + "," + startKey + "," + id + "." + md5HashInHex + ".", nameStr);
  }

  @Test
  public void testContainsRange() {
    TableDescriptor tableDesc = TableDescriptorBuilder.newBuilder(name).build();
    RegionInfo ri = RegionInfoBuilder.newBuilder(tableDesc.getTableName())
      .setStartKey(Bytes.toBytes("a")).setEndKey(Bytes.toBytes("g")).build();
    // Single row range at start of region
    assertTrue(ri.containsRange(Bytes.toBytes("a"), Bytes.toBytes("a")));
    // Fully contained range
    assertTrue(ri.containsRange(Bytes.toBytes("b"), Bytes.toBytes("c")));
    // Range overlapping start of region
    assertTrue(ri.containsRange(Bytes.toBytes("a"), Bytes.toBytes("c")));
    // Fully contained single-row range
    assertTrue(ri.containsRange(Bytes.toBytes("c"), Bytes.toBytes("c")));
    // Range that overlaps end key and hence doesn't fit
    assertFalse(ri.containsRange(Bytes.toBytes("a"), Bytes.toBytes("g")));
    // Single row range on end key
    assertFalse(ri.containsRange(Bytes.toBytes("g"), Bytes.toBytes("g")));
    // Single row range entirely outside
    assertFalse(ri.containsRange(Bytes.toBytes("z"), Bytes.toBytes("z")));

    // Degenerate range
    try {
      ri.containsRange(Bytes.toBytes("z"), Bytes.toBytes("a"));
      fail("Invalid range did not throw IAE");
    } catch (IllegalArgumentException iae) {
    }
  }

  @Test
  public void testContainsRangeForMetaTable() {
    TableDescriptor tableDesc =
      TableDescriptorBuilder.newBuilder(TableName.META_TABLE_NAME).build();
    RegionInfo hri = RegionInfoBuilder.newBuilder(tableDesc.getTableName()).build();
    byte[] startRow = HConstants.EMPTY_START_ROW;
    byte[] row1 = Bytes.toBytes("a,a,0");
    byte[] row2 = Bytes.toBytes("aaaaa,,1");
    byte[] row3 = Bytes.toBytes("aaaaa,\u0000\u0000,2");
    byte[] row4 = Bytes.toBytes("aaaaa,\u0001,3");
    byte[] row5 = Bytes.toBytes("aaaaa,a,4");
    byte[] row6 = Bytes.toBytes("aaaaa,\u1000,5");

    // Single row range at start of region
    assertTrue(hri.containsRange(startRow, startRow));
    // Fully contained range
    assertTrue(hri.containsRange(row1, row2));
    assertTrue(hri.containsRange(row2, row3));
    assertTrue(hri.containsRange(row3, row4));
    assertTrue(hri.containsRange(row4, row5));
    assertTrue(hri.containsRange(row5, row6));
    // Range overlapping start of region
    assertTrue(hri.containsRange(startRow, row2));
    // Fully contained single-row range
    assertTrue(hri.containsRange(row1, row1));
    // Degenerate range
    try {
      hri.containsRange(row3, row2);
      fail("Invalid range did not throw IAE");
    } catch (IllegalArgumentException iae) {
    }
  }

  @Test
  public void testLastRegionCompare() {
    TableDescriptor tableDesc = TableDescriptorBuilder.newBuilder(name).build();
    RegionInfo rip = RegionInfoBuilder.newBuilder(tableDesc.getTableName())
      .setStartKey(Bytes.toBytes("a")).setEndKey(new byte[0]).build();
    RegionInfo ric = RegionInfoBuilder.newBuilder(tableDesc.getTableName())
      .setStartKey(Bytes.toBytes("a")).setEndKey(Bytes.toBytes("b")).build();
    assertTrue(RegionInfo.COMPARATOR.compare(rip, ric) > 0);
  }

  @Test
  public void testMetaTables() {
    assertTrue(RegionInfoBuilder.FIRST_META_REGIONINFO.isMetaRegion());
  }

  @Test
  public void testComparator() {
    final TableName tableName = name;
    byte[] empty = new byte[0];
    RegionInfo older = RegionInfoBuilder.newBuilder(tableName).setStartKey(empty).setEndKey(empty)
      .setSplit(false).setRegionId(0L).build();
    RegionInfo newer = RegionInfoBuilder.newBuilder(tableName).setStartKey(empty).setEndKey(empty)
      .setSplit(false).setRegionId(1L).build();
    assertTrue(RegionInfo.COMPARATOR.compare(older, newer) < 0);
    assertTrue(RegionInfo.COMPARATOR.compare(newer, older) > 0);
    assertTrue(RegionInfo.COMPARATOR.compare(older, older) == 0);
    assertTrue(RegionInfo.COMPARATOR.compare(newer, newer) == 0);
  }

  @Test
  public void testRegionNameForRegionReplicas() throws Exception {
    final TableName tn = name;
    String startKey = "startkey";
    final byte[] sk = Bytes.toBytes(startKey);
    String id = "id";

    // assert with only the region name without encoding

    // primary, replicaId = 0
    byte[] name = RegionInfo.createRegionName(tn, sk, Bytes.toBytes(id), 0, false);
    String nameStr = Bytes.toString(name);
    assertEquals(tn + "," + startKey + "," + id, nameStr);

    // replicaId = 1
    name = RegionInfo.createRegionName(tn, sk, Bytes.toBytes(id), 1, false);
    nameStr = Bytes.toString(name);
    assertEquals(
      tn + "," + startKey + "," + id + "_" + String.format(RegionInfo.REPLICA_ID_FORMAT, 1),
      nameStr);

    // replicaId = max
    name = RegionInfo.createRegionName(tn, sk, Bytes.toBytes(id), 0xFFFF, false);
    nameStr = Bytes.toString(name);
    assertEquals(
      tn + "," + startKey + "," + id + "_" + String.format(RegionInfo.REPLICA_ID_FORMAT, 0xFFFF),
      nameStr);
  }

  @Test
  public void testParseName() throws IOException {
    final TableName tableName = name;
    byte[] startKey = Bytes.toBytes("startKey");
    long regionId = EnvironmentEdgeManager.currentTime();
    int replicaId = 42;

    // test without replicaId
    byte[] regionName = RegionInfo.createRegionName(tableName, startKey, regionId, false);

    byte[][] fields = RegionInfo.parseRegionName(regionName);
    assertArrayEquals(tableName.getName(), fields[0], Bytes.toString(fields[0]));
    assertArrayEquals(startKey, fields[1], Bytes.toString(fields[1]));
    assertArrayEquals(Bytes.toBytes(Long.toString(regionId)), fields[2], Bytes.toString(fields[2]));
    assertEquals(3, fields.length);

    // test with replicaId
    regionName = RegionInfo.createRegionName(tableName, startKey, regionId, replicaId, false);

    fields = RegionInfo.parseRegionName(regionName);
    assertArrayEquals(tableName.getName(), fields[0], Bytes.toString(fields[0]));
    assertArrayEquals(startKey, fields[1], Bytes.toString(fields[1]));
    assertArrayEquals(Bytes.toBytes(Long.toString(regionId)), fields[2], Bytes.toString(fields[2]));
    assertArrayEquals(Bytes.toBytes(String.format(RegionInfo.REPLICA_ID_FORMAT, replicaId)),
      fields[3], Bytes.toString(fields[3]));
  }

  @Test
  public void testConvert() {
    final TableName tableName = TableName.valueOf("ns1:" + name.getQualifierAsString());
    byte[] startKey = Bytes.toBytes("startKey");
    byte[] endKey = Bytes.toBytes("endKey");
    boolean split = false;
    long regionId = EnvironmentEdgeManager.currentTime();
    int replicaId = 42;

    RegionInfo ri = RegionInfoBuilder.newBuilder(tableName).setStartKey(startKey).setEndKey(endKey)
      .setSplit(split).setRegionId(regionId).setReplicaId(replicaId).build();

    // convert two times, compare
    RegionInfo convertedRi = ProtobufUtil.toRegionInfo(ProtobufUtil.toRegionInfo(ri));

    assertEquals(ri, convertedRi);

    // test convert RegionInfo without replicaId
    HBaseProtos.RegionInfo info = HBaseProtos.RegionInfo.newBuilder()
      .setTableName(HBaseProtos.TableName.newBuilder()
        .setQualifier(UnsafeByteOperations.unsafeWrap(tableName.getQualifier()))
        .setNamespace(UnsafeByteOperations.unsafeWrap(tableName.getNamespace())).build())
      .setStartKey(UnsafeByteOperations.unsafeWrap(startKey))
      .setEndKey(UnsafeByteOperations.unsafeWrap(endKey)).setSplit(split).setRegionId(regionId)
      .build();

    convertedRi = ProtobufUtil.toRegionInfo(info);
    RegionInfo expectedRi = RegionInfoBuilder.newBuilder(tableName).setStartKey(startKey)
      .setEndKey(endKey).setSplit(split).setRegionId(regionId).setReplicaId(0).build();

    assertEquals(expectedRi, convertedRi);
  }

  private void assertRegionNameNotEquals(RegionInfo expected, RegionInfo actual) {
    assertNotEquals(expected.getRegionNameAsString(), actual.getRegionNameAsString());
    assertNotEquals(expected.getEncodedName(), actual.getEncodedName());
  }

  private void assertRegionNameEquals(RegionInfo expected, RegionInfo actual) {
    assertEquals(expected.getRegionNameAsString(), actual.getRegionNameAsString());
    assertEquals(expected.getEncodedName(), actual.getEncodedName());
  }

  @Test
  public void testNewBuilderWithRegionInfo() {
    RegionInfo ri = RegionInfoBuilder.newBuilder(name).build();
    assertEquals(ri, RegionInfoBuilder.newBuilder(ri).build());

    // make sure that the region name and encoded name are changed, see HBASE-24500 for more
    // details.
    assertRegionNameNotEquals(ri,
      RegionInfoBuilder.newBuilder(ri).setStartKey(new byte[1]).build());
    assertRegionNameNotEquals(ri,
      RegionInfoBuilder.newBuilder(ri).setRegionId(ri.getRegionId() + 1).build());
    assertRegionNameNotEquals(ri, RegionInfoBuilder.newBuilder(ri).setReplicaId(1).build());

    // these fields are not in region name
    assertRegionNameEquals(ri, RegionInfoBuilder.newBuilder(ri).setEndKey(new byte[1]).build());
    assertRegionNameEquals(ri, RegionInfoBuilder.newBuilder(ri).setSplit(true).build());
    assertRegionNameEquals(ri, RegionInfoBuilder.newBuilder(ri).setOffline(true).build());
  }
}
