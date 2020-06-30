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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.RegionInfoDisplay;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSTableDescriptors;
import org.apache.hadoop.hbase.util.MD5Hash;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import org.apache.hbase.thirdparty.com.google.protobuf.UnsafeByteOperations;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;

@Category({ RegionServerTests.class, SmallTests.class })
public class TestRegionInfo {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRegionInfo.class);

  @Rule
  public TestName name = new TestName();

  @Test
  public void testIsStart() {
    assertTrue(RegionInfoBuilder.newBuilder(TableName.META_TABLE_NAME).build().isFirst());
    RegionInfo ri = RegionInfoBuilder.newBuilder(TableName.META_TABLE_NAME)
      .setStartKey(Bytes.toBytes("not_start")).build();
    assertFalse(ri.isFirst());
  }

  @Test
  public void testIsEnd() {
    assertTrue(RegionInfoBuilder.newBuilder(TableName.META_TABLE_NAME).build().isLast());
    RegionInfo ri = RegionInfoBuilder.newBuilder(TableName.META_TABLE_NAME)
      .setEndKey(Bytes.toBytes("not_end")).build();
    assertFalse(ri.isLast());
  }

  @Test
  public void testIsNext() {
    byte[] bytes = Bytes.toBytes("row");
    RegionInfo ri =
      RegionInfoBuilder.newBuilder(TableName.META_TABLE_NAME).setEndKey(bytes).build();
    RegionInfo ri2 =
      RegionInfoBuilder.newBuilder(TableName.META_TABLE_NAME).setStartKey(bytes).build();
    assertFalse(ri.isNext(RegionInfoBuilder.newBuilder(TableName.META_TABLE_NAME).build()));
    assertTrue(ri.isNext(ri2));
  }

  @Test
  public void testIsOverlap() {
    byte[] a = Bytes.toBytes("a");
    byte[] b = Bytes.toBytes("b");
    byte[] c = Bytes.toBytes("c");
    byte[] d = Bytes.toBytes("d");
    RegionInfo all = RegionInfoBuilder.newBuilder(TableName.META_TABLE_NAME).build();
    RegionInfo ari = RegionInfoBuilder.newBuilder(TableName.META_TABLE_NAME).setEndKey(a).build();
    RegionInfo abri =
      RegionInfoBuilder.newBuilder(TableName.META_TABLE_NAME).setStartKey(a).setEndKey(b).build();
    RegionInfo adri =
      RegionInfoBuilder.newBuilder(TableName.META_TABLE_NAME).setStartKey(a).setEndKey(d).build();
    RegionInfo cdri =
      RegionInfoBuilder.newBuilder(TableName.META_TABLE_NAME).setStartKey(c).setEndKey(d).build();
    RegionInfo dri = RegionInfoBuilder.newBuilder(TableName.META_TABLE_NAME).setStartKey(d).build();
    assertTrue(all.isOverlap(all));
    assertTrue(all.isOverlap(abri));
    assertFalse(abri.isOverlap(cdri));
    assertTrue(all.isOverlap(ari));
    assertFalse(ari.isOverlap(abri));
    assertFalse(ari.isOverlap(abri));
    assertTrue(ari.isOverlap(all));
    assertTrue(dri.isOverlap(all));
    assertTrue(abri.isOverlap(adri));
    assertFalse(dri.isOverlap(ari));
    assertTrue(abri.isOverlap(adri));
    assertTrue(adri.isOverlap(abri));
  }

  /**
   * Tests {@link RegionInfo#isOverlap(RegionInfo)}
   */
  @Test
  public void testIsOverlaps() {
    byte[] a = Bytes.toBytes("a");
    byte[] b = Bytes.toBytes("b");
    byte[] c = Bytes.toBytes("c");
    byte[] d = Bytes.toBytes("d");
    byte[] e = Bytes.toBytes("e");
    byte[] f = Bytes.toBytes("f");
    RegionInfo ari = RegionInfoBuilder.newBuilder(TableName.META_TABLE_NAME).setEndKey(a).build();
    RegionInfo abri =
      RegionInfoBuilder.newBuilder(TableName.META_TABLE_NAME).setStartKey(a).setEndKey(b).build();
    RegionInfo eri = RegionInfoBuilder.newBuilder(TableName.META_TABLE_NAME).setEndKey(e).build();
    RegionInfo cdri =
      RegionInfoBuilder.newBuilder(TableName.META_TABLE_NAME).setStartKey(c).setEndKey(d).build();
    RegionInfo efri =
      RegionInfoBuilder.newBuilder(TableName.META_TABLE_NAME).setStartKey(e).setEndKey(f).build();
    assertFalse(ari.isOverlap(abri));
    assertTrue(abri.isOverlap(eri));
    assertFalse(cdri.isOverlap(efri));
    assertTrue(eri.isOverlap(ari));
  }

  @Test
  public void testPb() throws DeserializationException {
    RegionInfo hri = RegionInfoBuilder.newBuilder(TableName.valueOf("test"))
      .setStartKey(Bytes.toBytes("start")).build();
    byte[] bytes = RegionInfo.toByteArray(hri);
    RegionInfo pbhri = RegionInfo.parseFrom(bytes);
    assertTrue(hri.equals(pbhri));
  }

  @Test
  public void testReadAndWriteHRegionInfoFile() throws IOException, InterruptedException {
    HBaseTestingUtility htu = new HBaseTestingUtility();
    RegionInfo hri = RegionInfoBuilder.newBuilder(TableName.META_TABLE_NAME).build();
    Path basedir = htu.getDataTestDir();
    // Create a region. That'll write the .regioninfo file.
    FSTableDescriptors fsTableDescriptors = new FSTableDescriptors(htu.getConfiguration());
    FSTableDescriptors.tryUpdateMetaTableDescriptor(htu.getConfiguration());
    HRegion r = HBaseTestingUtility.createRegionAndWAL(hri, basedir, htu.getConfiguration(),
      fsTableDescriptors.get(TableName.META_TABLE_NAME));
    // Get modtime on the file.
    long modtime = getModTime(r);
    HBaseTestingUtility.closeRegionAndWAL(r);
    Thread.sleep(1001);
    r = HRegion.openHRegion(basedir, hri, fsTableDescriptors.get(TableName.META_TABLE_NAME), null,
      htu.getConfiguration());
    // Ensure the file is not written for a second time.
    long modtime2 = getModTime(r);
    assertEquals(modtime, modtime2);
    // Now load the file.
    RegionInfo deserializedHri = HRegionFileSystem.loadRegionInfoFileContent(
      r.getRegionFileSystem().getFileSystem(), r.getRegionFileSystem().getRegionDir());
    assertEquals(0, RegionInfo.COMPARATOR.compare(hri, deserializedHri));
    HBaseTestingUtility.closeRegionAndWAL(r);
  }

  long getModTime(final HRegion r) throws IOException {
    FileStatus[] statuses = r.getRegionFileSystem().getFileSystem().listStatus(
      new Path(r.getRegionFileSystem().getRegionDir(), HRegionFileSystem.REGION_INFO_FILE));
    assertTrue(statuses != null && statuses.length == 1);
    return statuses[0].getModificationTime();
  }

  @Test
  public void testCreateHRegionInfoName() throws Exception {
    final String tableName = name.getMethodName();
    final TableName tn = TableName.valueOf(tableName);
    String startKey = "startkey";
    final byte[] sk = Bytes.toBytes(startKey);
    String id = "id";

    // old format region name
    byte[] name = RegionInfo.createRegionName(tn, sk, id, false);
    String nameStr = Bytes.toString(name);
    assertEquals(tableName + "," + startKey + "," + id, nameStr);

    // new format region name.
    String md5HashInHex = MD5Hash.getMD5AsHex(name);
    assertEquals(RegionInfo.MD5_HEX_LENGTH, md5HashInHex.length());
    name = RegionInfo.createRegionName(tn, sk, id, true);
    nameStr = Bytes.toString(name);
    assertEquals(tableName + "," + startKey + "," + id + "." + md5HashInHex + ".", nameStr);
  }

  @Test
  public void testContainsRange() {
    TableDescriptor tableDesc =
      TableDescriptorBuilder.newBuilder(TableName.valueOf(name.getMethodName())).build();
    RegionInfo hri = RegionInfoBuilder.newBuilder(tableDesc.getTableName())
      .setStartKey(Bytes.toBytes("a")).setEndKey(Bytes.toBytes("g")).build();
    // Single row range at start of region
    assertTrue(hri.containsRange(Bytes.toBytes("a"), Bytes.toBytes("a")));
    // Fully contained range
    assertTrue(hri.containsRange(Bytes.toBytes("b"), Bytes.toBytes("c")));
    // Range overlapping start of region
    assertTrue(hri.containsRange(Bytes.toBytes("a"), Bytes.toBytes("c")));
    // Fully contained single-row range
    assertTrue(hri.containsRange(Bytes.toBytes("c"), Bytes.toBytes("c")));
    // Range that overlaps end key and hence doesn't fit
    assertFalse(hri.containsRange(Bytes.toBytes("a"), Bytes.toBytes("g")));
    // Single row range on end key
    assertFalse(hri.containsRange(Bytes.toBytes("g"), Bytes.toBytes("g")));
    // Single row range entirely outside
    assertFalse(hri.containsRange(Bytes.toBytes("z"), Bytes.toBytes("z")));

    // Degenerate range
    try {
      hri.containsRange(Bytes.toBytes("z"), Bytes.toBytes("a"));
      fail("Invalid range did not throw IAE");
    } catch (IllegalArgumentException iae) {
    }
  }

  @Test
  public void testLastRegionCompare() {
    TableDescriptor tableDesc =
      TableDescriptorBuilder.newBuilder(TableName.valueOf(name.getMethodName())).build();
    RegionInfo hrip = RegionInfoBuilder.newBuilder(tableDesc.getTableName())
      .setStartKey(Bytes.toBytes("a")).build();
    RegionInfo hric = RegionInfoBuilder.newBuilder(tableDesc.getTableName())
      .setStartKey(Bytes.toBytes("a")).setEndKey(Bytes.toBytes("b")).build();
    assertTrue(hrip.compareTo(hric) > 0);
  }

  @Test
  public void testMetaTables() {
    assertTrue(RegionInfoBuilder.newBuilder(TableName.META_TABLE_NAME).build().isMetaRegion());
    assertFalse(RegionInfoBuilder.newBuilder(TableName.valueOf("test")).build().isMetaRegion());
  }

  @SuppressWarnings("SelfComparison")
  @Test
  public void testComparator() {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    RegionInfo older = RegionInfoBuilder.newBuilder(tableName).setRegionId(0).build();
    RegionInfo newer = RegionInfoBuilder.newBuilder(tableName).setRegionId(1).build();
    assertTrue(older.compareTo(newer) < 0);
    assertTrue(newer.compareTo(older) > 0);
    assertEquals(0, older.compareTo(older));
    assertEquals(0, newer.compareTo(newer));

    RegionInfo a = RegionInfoBuilder.newBuilder(TableName.valueOf("a")).build();
    RegionInfo b = RegionInfoBuilder.newBuilder(TableName.valueOf("b")).build();
    assertNotEquals(0, a.compareTo(b));
    TableName t = TableName.valueOf("t");
    byte[] midway = Bytes.toBytes("midway");
    a = RegionInfoBuilder.newBuilder(t).setEndKey(midway).build();
    b = RegionInfoBuilder.newBuilder(t).setStartKey(midway).build();
    assertTrue(a.compareTo(b) < 0);
    assertTrue(b.compareTo(a) > 0);
    assertEquals(a, a);
    assertEquals(0, a.compareTo(a));
    a = RegionInfoBuilder.newBuilder(t).setStartKey(Bytes.toBytes("a"))
      .setEndKey(Bytes.toBytes("d")).build();
    b = RegionInfoBuilder.newBuilder(t).setStartKey(Bytes.toBytes("e"))
      .setEndKey(Bytes.toBytes("g")).build();
    assertTrue(a.compareTo(b) < 0);
    a = RegionInfoBuilder.newBuilder(t).setStartKey(Bytes.toBytes("aaaa"))
      .setEndKey(Bytes.toBytes("dddd")).build();
    b = RegionInfoBuilder.newBuilder(t).setStartKey(Bytes.toBytes("e"))
      .setEndKey(Bytes.toBytes("g")).build();
    assertTrue(a.compareTo(b) < 0);
    a = RegionInfoBuilder.newBuilder(t).setStartKey(Bytes.toBytes("aaaa"))
      .setEndKey(Bytes.toBytes("dddd")).build();
    b = RegionInfoBuilder.newBuilder(t).setStartKey(Bytes.toBytes("aaaa"))
      .setEndKey(Bytes.toBytes("eeee")).build();
    assertTrue(a.compareTo(b) < 0);

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
    byte[] name = RegionInfo.createRegionName(tn, sk, Bytes.toBytes(id), 0, false);
    String nameStr = Bytes.toString(name);
    assertEquals(tableName + "," + startKey + "," + id, nameStr);

    // replicaId = 1
    name = RegionInfo.createRegionName(tn, sk, Bytes.toBytes(id), 1, false);
    nameStr = Bytes.toString(name);
    assertEquals(
      tableName + "," + startKey + "," + id + "_" + String.format(RegionInfo.REPLICA_ID_FORMAT, 1),
      nameStr);

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
    assertArrayEquals(Bytes.toString(fields[0]), tableName.getName(), fields[0]);
    assertArrayEquals(Bytes.toString(fields[1]), startKey, fields[1]);
    assertArrayEquals(Bytes.toString(fields[2]), Bytes.toBytes(Long.toString(regionId)), fields[2]);
    assertEquals(3, fields.length);

    // test with replicaId
    regionName = RegionInfo.createRegionName(tableName, startKey, regionId, replicaId, false);

    fields = RegionInfo.parseRegionName(regionName);
    assertArrayEquals(Bytes.toString(fields[0]), tableName.getName(), fields[0]);
    assertArrayEquals(Bytes.toString(fields[1]), startKey, fields[1]);
    assertArrayEquals(Bytes.toString(fields[2]), Bytes.toBytes(Long.toString(regionId)), fields[2]);
    assertArrayEquals(Bytes.toString(fields[3]),
      Bytes.toBytes(String.format(RegionInfo.REPLICA_ID_FORMAT, replicaId)), fields[3]);
  }

  @Test
  public void testConvert() {
    final TableName tableName = TableName.valueOf("ns1:" + name.getMethodName());
    byte[] startKey = Bytes.toBytes("startKey");
    byte[] endKey = Bytes.toBytes("endKey");
    boolean split = false;
    long regionId = System.currentTimeMillis();
    int replicaId = 42;

    RegionInfo hri = RegionInfoBuilder.newBuilder(tableName).setStartKey(startKey).setEndKey(endKey)
      .setSplit(split).setRegionId(regionId).setReplicaId(replicaId).build();

    // convert two times, compare
    RegionInfo convertedHri = ProtobufUtil.toRegionInfo(ProtobufUtil.toRegionInfo(hri));

    assertEquals(hri, convertedHri);

    // test convert RegionInfo without replicaId
    HBaseProtos.RegionInfo info = HBaseProtos.RegionInfo.newBuilder()
      .setTableName(HBaseProtos.TableName.newBuilder()
        .setQualifier(UnsafeByteOperations.unsafeWrap(tableName.getQualifier()))
        .setNamespace(UnsafeByteOperations.unsafeWrap(tableName.getNamespace())).build())
      .setStartKey(UnsafeByteOperations.unsafeWrap(startKey))
      .setEndKey(UnsafeByteOperations.unsafeWrap(endKey)).setSplit(split).setRegionId(regionId)
      .build();

    convertedHri = ProtobufUtil.toRegionInfo(info);
    // expecting default replicaId
    RegionInfo expectedHri = RegionInfoBuilder.newBuilder(tableName).setStartKey(startKey)
      .setEndKey(endKey).setSplit(split).setRegionId(regionId).setReplicaId(0).build();

    assertEquals(expectedHri, convertedHri);
  }

  @Test
  public void testRegionDetailsForDisplay() throws IOException {
    byte[] startKey = new byte[] { 0x01, 0x01, 0x02, 0x03 };
    byte[] endKey = new byte[] { 0x01, 0x01, 0x02, 0x04 };
    Configuration conf = new Configuration();
    conf.setBoolean("hbase.display.keys", false);
    RegionInfo h = RegionInfoBuilder.newBuilder(TableName.valueOf(name.getMethodName()))
      .setStartKey(startKey).setEndKey(endKey).build();
    checkEquality(h, conf);
    // check HRIs with non-default replicaId
    h = RegionInfoBuilder.newBuilder(TableName.valueOf(name.getMethodName())).setStartKey(startKey)
      .setEndKey(endKey).setRegionId(System.currentTimeMillis()).setReplicaId(1).build();
    checkEquality(h, conf);
    assertArrayEquals(RegionInfoDisplay.HIDDEN_END_KEY,
      RegionInfoDisplay.getEndKeyForDisplay(h, conf));
    assertArrayEquals(RegionInfoDisplay.HIDDEN_START_KEY,
      RegionInfoDisplay.getStartKeyForDisplay(h, conf));

    RegionState state = RegionState.createForTesting(h, RegionState.State.OPEN);
    String descriptiveNameForDisplay =
      RegionInfoDisplay.getDescriptiveNameFromRegionStateForDisplay(state, conf);
    checkDescriptiveNameEquality(descriptiveNameForDisplay, state.toDescriptiveString(), startKey);

    conf.setBoolean("hbase.display.keys", true);
    assertArrayEquals(endKey, RegionInfoDisplay.getEndKeyForDisplay(h, conf));
    assertArrayEquals(startKey, RegionInfoDisplay.getStartKeyForDisplay(h, conf));
    assertEquals(state.toDescriptiveString(),
      RegionInfoDisplay.getDescriptiveNameFromRegionStateForDisplay(state, conf));
  }

  private void checkDescriptiveNameEquality(String descriptiveNameForDisplay, String origDesc,
    byte[] startKey) {
    // except for the "hidden-start-key" substring everything else should exactly match
    String firstPart = descriptiveNameForDisplay.substring(0,
      descriptiveNameForDisplay.indexOf(new String(RegionInfoDisplay.HIDDEN_START_KEY)));
    String secondPart = descriptiveNameForDisplay
      .substring(descriptiveNameForDisplay.indexOf(new String(RegionInfoDisplay.HIDDEN_START_KEY)) +
        RegionInfoDisplay.HIDDEN_START_KEY.length);
    String firstPartOrig = origDesc.substring(0, origDesc.indexOf(Bytes.toStringBinary(startKey)));
    String secondPartOrig = origDesc.substring(
      origDesc.indexOf(Bytes.toStringBinary(startKey)) + Bytes.toStringBinary(startKey).length());
    assert (firstPart.equals(firstPartOrig));
    assert (secondPart.equals(secondPartOrig));
  }

  private void checkEquality(RegionInfo h, Configuration conf) throws IOException {
    byte[] modifiedRegionName = RegionInfoDisplay.getRegionNameForDisplay(h, conf);
    byte[][] modifiedRegionNameParts = RegionInfo.parseRegionName(modifiedRegionName);
    byte[][] regionNameParts = RegionInfo.parseRegionName(h.getRegionName());

    // same number of parts
    assert (modifiedRegionNameParts.length == regionNameParts.length);

    for (int i = 0; i < regionNameParts.length; i++) {
      // all parts should match except for [1] where in the modified one,
      // we should have "hidden_start_key"
      if (i != 1) {
        Assert.assertArrayEquals(regionNameParts[i], modifiedRegionNameParts[i]);
      } else {
        assertNotEquals(regionNameParts[i][0], modifiedRegionNameParts[i][0]);
        Assert.assertArrayEquals(modifiedRegionNameParts[1],
          RegionInfoDisplay.getStartKeyForDisplay(h, conf));
      }
    }
  }
}
