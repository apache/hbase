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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSTableDescriptors;
import org.apache.hadoop.hbase.util.MD5Hash;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import org.apache.hbase.thirdparty.com.google.protobuf.UnsafeByteOperations;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;

@Category({RegionServerTests.class, SmallTests.class})
public class TestRegionInfoBuilder {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestRegionInfoBuilder.class);

  @Rule
  public TestName name = new TestName();

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
    byte [] bytes = RegionInfo.toByteArray(ri);
    RegionInfo pbri = RegionInfo.parseFrom(bytes);
    assertTrue(RegionInfo.COMPARATOR.compare(ri, pbri) == 0);
  }

  @Test
  public void testReadAndWriteRegionInfoFile() throws IOException, InterruptedException {
    HBaseTestingUtility htu = new HBaseTestingUtility();
    RegionInfo ri = RegionInfoBuilder.FIRST_META_REGIONINFO;
    Path basedir = htu.getDataTestDir();
    // Create a region.  That'll write the .regioninfo file.
    FSTableDescriptors fsTableDescriptors = new FSTableDescriptors(htu.getConfiguration());
    HRegion r = HBaseTestingUtility.createRegionAndWAL(convert(ri), basedir, htu.getConfiguration(),
        fsTableDescriptors.get(TableName.META_TABLE_NAME));
    // Get modtime on the file.
    long modtime = getModTime(r);
    HBaseTestingUtility.closeRegionAndWAL(r);
    Thread.sleep(1001);
    r = HRegion.openHRegion(basedir, convert(ri), fsTableDescriptors.get(TableName.META_TABLE_NAME),
        null, htu.getConfiguration());
    // Ensure the file is not written for a second time.
    long modtime2 = getModTime(r);
    assertEquals(modtime, modtime2);
    // Now load the file.
    RegionInfo deserializedRi = HRegionFileSystem.loadRegionInfoFileContent(
        r.getRegionFileSystem().getFileSystem(), r.getRegionFileSystem().getRegionDir());
    HBaseTestingUtility.closeRegionAndWAL(r);
  }

  long getModTime(final HRegion r) throws IOException {
    FileStatus[] statuses = r.getRegionFileSystem().getFileSystem().listStatus(
      new Path(r.getRegionFileSystem().getRegionDir(), HRegionFileSystem.REGION_INFO_FILE));
    assertTrue(statuses != null && statuses.length == 1);
    return statuses[0].getModificationTime();
  }

  @Test
  public void testCreateRegionInfoName() throws Exception {
    final String tableName = name.getMethodName();
    final TableName tn = TableName.valueOf(tableName);
    String startKey = "startkey";
    final byte[] sk = Bytes.toBytes(startKey);
    String id = "id";

    // old format region name
    byte [] name = RegionInfo.createRegionName(tn, sk, id, false);
    String nameStr = Bytes.toString(name);
    assertEquals(tableName + "," + startKey + "," + id, nameStr);


    // new format region name.
    String md5HashInHex = MD5Hash.getMD5AsHex(name);
    assertEquals(RegionInfo.MD5_HEX_LENGTH, md5HashInHex.length());
    name = RegionInfo.createRegionName(tn, sk, id, true);
    nameStr = Bytes.toString(name);
    assertEquals(tableName + "," + startKey + ","
                 + id + "." + md5HashInHex + ".",
                 nameStr);
  }

  @Test
  public void testContainsRange() {
    TableDescriptor tableDesc = TableDescriptorBuilder.newBuilder(
            TableName.valueOf(name.getMethodName())).build();
    RegionInfo ri = RegionInfoBuilder.newBuilder(tableDesc.getTableName())
            .setStartKey(Bytes.toBytes("a"))
            .setEndKey(Bytes.toBytes("g")).build();
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
  public void testLastRegionCompare() {
    TableDescriptor tableDesc = TableDescriptorBuilder
            .newBuilder(TableName.valueOf(name.getMethodName())).build();
    RegionInfo rip = RegionInfoBuilder.newBuilder(tableDesc.getTableName())
            .setStartKey(Bytes.toBytes("a"))
            .setEndKey(new byte[0]).build();
    RegionInfo ric = RegionInfoBuilder.newBuilder(tableDesc.getTableName())
            .setStartKey(Bytes.toBytes("a"))
            .setEndKey(Bytes.toBytes("b")).build();
    assertTrue(RegionInfo.COMPARATOR.compare(rip, ric) > 0);
  }

  @Test
  public void testMetaTables() {
    assertTrue(RegionInfoBuilder.FIRST_META_REGIONINFO.isMetaRegion());
  }

  @Test
  public void testComparator() {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    byte[] empty = new byte[0];
    RegionInfo older = RegionInfoBuilder.newBuilder(tableName)
            .setStartKey(empty)
            .setEndKey(empty)
            .setSplit(false)
            .setRegionId(0L).build();
    RegionInfo newer = RegionInfoBuilder.newBuilder(tableName)
            .setStartKey(empty)
            .setEndKey(empty)
            .setSplit(false)
            .setRegionId(1L).build();
    assertTrue(RegionInfo.COMPARATOR.compare(older, newer) < 0);
    assertTrue(RegionInfo.COMPARATOR.compare(newer, older) > 0);
    assertTrue(RegionInfo.COMPARATOR.compare(older, older) == 0);
    assertTrue(RegionInfo.COMPARATOR.compare(newer, newer) == 0);
  }

  @Test
  public void testRegionNameForRegionReplicas() throws Exception {
    String tableName = name.getMethodName();
    final TableName tn = TableName.valueOf(tableName);
    String startKey = "startkey";
    final byte[] sk = Bytes.toBytes(startKey);
    String id = "id";

    // assert with only the region name without encoding

    // primary, replicaId = 0
    byte [] name = RegionInfo.createRegionName(tn, sk, Bytes.toBytes(id), 0, false);
    String nameStr = Bytes.toString(name);
    assertEquals(tableName + "," + startKey + "," + id, nameStr);

    // replicaId = 1
    name = RegionInfo.createRegionName(tn, sk, Bytes.toBytes(id), 1, false);
    nameStr = Bytes.toString(name);
    assertEquals(tableName + "," + startKey + "," + id + "_" +
      String.format(RegionInfo.REPLICA_ID_FORMAT, 1), nameStr);

    // replicaId = max
    name = RegionInfo.createRegionName(tn, sk, Bytes.toBytes(id), 0xFFFF, false);
    nameStr = Bytes.toString(name);
    assertEquals(tableName + "," + startKey + "," + id + "_" +
        String.format(RegionInfo.REPLICA_ID_FORMAT, 0xFFFF), nameStr);
  }

  @Test
  public void testParseName() throws IOException {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    byte[] startKey = Bytes.toBytes("startKey");
    long regionId = System.currentTimeMillis();
    int replicaId = 42;

    // test without replicaId
    byte[] regionName = RegionInfo.createRegionName(tableName, startKey, regionId, false);

    byte[][] fields = RegionInfo.parseRegionName(regionName);
    assertArrayEquals(Bytes.toString(fields[0]),tableName.getName(), fields[0]);
    assertArrayEquals(Bytes.toString(fields[1]),startKey, fields[1]);
    assertArrayEquals(Bytes.toString(fields[2]), Bytes.toBytes(Long.toString(regionId)),fields[2]);
    assertEquals(3, fields.length);

    // test with replicaId
    regionName = RegionInfo.createRegionName(tableName, startKey, regionId,
      replicaId, false);

    fields = RegionInfo.parseRegionName(regionName);
    assertArrayEquals(Bytes.toString(fields[0]),tableName.getName(), fields[0]);
    assertArrayEquals(Bytes.toString(fields[1]),startKey, fields[1]);
    assertArrayEquals(Bytes.toString(fields[2]), Bytes.toBytes(Long.toString(regionId)),fields[2]);
    assertArrayEquals(Bytes.toString(fields[3]), Bytes.toBytes(
      String.format(RegionInfo.REPLICA_ID_FORMAT, replicaId)), fields[3]);
  }

  @Test
  public void testConvert() {
    final TableName tableName = TableName.valueOf("ns1:" + name.getMethodName());
    byte[] startKey = Bytes.toBytes("startKey");
    byte[] endKey = Bytes.toBytes("endKey");
    boolean split = false;
    long regionId = System.currentTimeMillis();
    int replicaId = 42;


    RegionInfo ri = RegionInfoBuilder.newBuilder(tableName)
            .setStartKey(startKey)
            .setEndKey(endKey)
            .setSplit(split)
            .setRegionId(regionId)
            .setReplicaId(replicaId).build();

    // convert two times, compare
    RegionInfo convertedRi = ProtobufUtil.toRegionInfo(ProtobufUtil.toRegionInfo(ri));

    assertEquals(ri, convertedRi);

    // test convert RegionInfo without replicaId
    HBaseProtos.RegionInfo info = HBaseProtos.RegionInfo.newBuilder()
      .setTableName(HBaseProtos.TableName.newBuilder()
        .setQualifier(UnsafeByteOperations.unsafeWrap(tableName.getQualifier()))
        .setNamespace(UnsafeByteOperations.unsafeWrap(tableName.getNamespace()))
        .build())
      .setStartKey(UnsafeByteOperations.unsafeWrap(startKey))
      .setEndKey(UnsafeByteOperations.unsafeWrap(endKey))
      .setSplit(split)
      .setRegionId(regionId)
      .build();

    convertedRi = ProtobufUtil.toRegionInfo(info);
    RegionInfo expectedRi = RegionInfoBuilder.newBuilder(tableName)
            .setStartKey(startKey)
            .setEndKey(endKey)
            .setSplit(split)
            .setRegionId(regionId)
            .setReplicaId(0).build();

    assertEquals(expectedRi, convertedRi);
  }

  // Duplicated method in TestRegionInfoDisplay too.
  private HRegionInfo convert(RegionInfo ri) {
    HRegionInfo hri = new HRegionInfo(
    ri.getTable(), ri.getStartKey(), ri.getEndKey(), ri.isSplit(), ri.getRegionId());
    hri.setOffline(ri.isOffline());
    return hri;
  }
}
