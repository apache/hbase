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
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
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
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;

@Category({RegionServerTests.class, SmallTests.class})
public class TestHRegionInfo {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestHRegionInfo.class);

  @Rule
  public TestName name = new TestName();

  @Test
  public void testIsStart() {
    assertTrue(RegionInfoBuilder.FIRST_META_REGIONINFO.isFirst());
    org.apache.hadoop.hbase.client.RegionInfo ri =
        org.apache.hadoop.hbase.client.RegionInfoBuilder.newBuilder(TableName.META_TABLE_NAME).
            setStartKey(Bytes.toBytes("not_start")).build();
    assertFalse(ri.isFirst());
  }

  @Test
  public void testIsEnd() {
    assertTrue(RegionInfoBuilder.FIRST_META_REGIONINFO.isFirst());
    org.apache.hadoop.hbase.client.RegionInfo ri =
        org.apache.hadoop.hbase.client.RegionInfoBuilder.newBuilder(TableName.META_TABLE_NAME).
            setEndKey(Bytes.toBytes("not_end")).build();
    assertFalse(ri.isLast());
  }

  @Test
  public void testIsNext() {
    byte [] bytes = Bytes.toBytes("row");
    org.apache.hadoop.hbase.client.RegionInfo ri =
        org.apache.hadoop.hbase.client.RegionInfoBuilder.newBuilder(TableName.META_TABLE_NAME).
            setEndKey(bytes).build();
    org.apache.hadoop.hbase.client.RegionInfo ri2 =
        org.apache.hadoop.hbase.client.RegionInfoBuilder.newBuilder(TableName.META_TABLE_NAME).
            setStartKey(bytes).build();
    assertFalse(ri.isNext(RegionInfoBuilder.FIRST_META_REGIONINFO));
    assertTrue(ri.isNext(ri2));
  }

  @Test
  public void testIsOverlap() {
    byte [] a = Bytes.toBytes("a");
    byte [] b = Bytes.toBytes("b");
    byte [] c = Bytes.toBytes("c");
    byte [] d = Bytes.toBytes("d");
    org.apache.hadoop.hbase.client.RegionInfo all =
        RegionInfoBuilder.FIRST_META_REGIONINFO;
    org.apache.hadoop.hbase.client.RegionInfo ari =
        org.apache.hadoop.hbase.client.RegionInfoBuilder.newBuilder(TableName.META_TABLE_NAME).
            setEndKey(a).build();
    org.apache.hadoop.hbase.client.RegionInfo abri =
        org.apache.hadoop.hbase.client.RegionInfoBuilder.newBuilder(TableName.META_TABLE_NAME).
            setStartKey(a).setEndKey(b).build();
    org.apache.hadoop.hbase.client.RegionInfo adri =
        org.apache.hadoop.hbase.client.RegionInfoBuilder.newBuilder(TableName.META_TABLE_NAME).
            setStartKey(a).setEndKey(d).build();
    org.apache.hadoop.hbase.client.RegionInfo cdri =
        org.apache.hadoop.hbase.client.RegionInfoBuilder.newBuilder(TableName.META_TABLE_NAME).
            setStartKey(c).setEndKey(d).build();
    org.apache.hadoop.hbase.client.RegionInfo dri =
        org.apache.hadoop.hbase.client.RegionInfoBuilder.newBuilder(TableName.META_TABLE_NAME).
            setStartKey(d).build();
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
   * Tests {@link RegionInfo#isOverlap(RegionInfo[])}
   */
  @Test
  public void testIsOverlaps() {
    byte[] a = Bytes.toBytes("a");
    byte[] b = Bytes.toBytes("b");
    byte[] c = Bytes.toBytes("c");
    byte[] d = Bytes.toBytes("d");
    byte[] e = Bytes.toBytes("e");
    byte[] f = Bytes.toBytes("f");
    org.apache.hadoop.hbase.client.RegionInfo ari =
      org.apache.hadoop.hbase.client.RegionInfoBuilder.newBuilder(TableName.META_TABLE_NAME).
        setEndKey(a).build();
    org.apache.hadoop.hbase.client.RegionInfo abri =
      org.apache.hadoop.hbase.client.RegionInfoBuilder.newBuilder(TableName.META_TABLE_NAME).
        setStartKey(a).setEndKey(b).build();
    org.apache.hadoop.hbase.client.RegionInfo eri =
      org.apache.hadoop.hbase.client.RegionInfoBuilder.newBuilder(TableName.META_TABLE_NAME).
        setEndKey(e).build();
    org.apache.hadoop.hbase.client.RegionInfo cdri =
      org.apache.hadoop.hbase.client.RegionInfoBuilder.newBuilder(TableName.META_TABLE_NAME).
        setStartKey(c).setEndKey(d).build();
    org.apache.hadoop.hbase.client.RegionInfo efri =
      org.apache.hadoop.hbase.client.RegionInfoBuilder.newBuilder(TableName.META_TABLE_NAME).
        setStartKey(e).setEndKey(f).build();
  }

  @Test
  public void testPb() throws DeserializationException {
    HRegionInfo hri = HRegionInfo.FIRST_META_REGIONINFO;
    byte [] bytes = hri.toByteArray();
    HRegionInfo pbhri = HRegionInfo.parseFrom(bytes);
    assertTrue(hri.equals(pbhri));
  }

  @Test
  public void testReadAndWriteHRegionInfoFile() throws IOException, InterruptedException {
    HBaseTestingUtility htu = new HBaseTestingUtility();
    HRegionInfo hri = HRegionInfo.FIRST_META_REGIONINFO;
    Path basedir = htu.getDataTestDir();
    // Create a region.  That'll write the .regioninfo file.
    FSTableDescriptors fsTableDescriptors = new FSTableDescriptors(htu.getConfiguration());
    FSTableDescriptors.tryUpdateMetaTableDescriptor(htu.getConfiguration());
    HRegion r = HBaseTestingUtility.createRegionAndWAL(hri, basedir, htu.getConfiguration(),
        fsTableDescriptors.get(TableName.META_TABLE_NAME));
    // Get modtime on the file.
    long modtime = getModTime(r);
    HBaseTestingUtility.closeRegionAndWAL(r);
    Thread.sleep(1001);
    r = HRegion.openHRegion(basedir, hri, fsTableDescriptors.get(TableName.META_TABLE_NAME),
        null, htu.getConfiguration());
    // Ensure the file is not written for a second time.
    long modtime2 = getModTime(r);
    assertEquals(modtime, modtime2);
    // Now load the file.
    org.apache.hadoop.hbase.client.RegionInfo deserializedHri =
      HRegionFileSystem.loadRegionInfoFileContent(
        r.getRegionFileSystem().getFileSystem(), r.getRegionFileSystem().getRegionDir());
    assertEquals(0,
      org.apache.hadoop.hbase.client.RegionInfo.COMPARATOR.compare(hri, deserializedHri));
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
    byte [] name = HRegionInfo.createRegionName(tn, sk, id, false);
    String nameStr = Bytes.toString(name);
    assertEquals(tableName + "," + startKey + "," + id, nameStr);


    // new format region name.
    String md5HashInHex = MD5Hash.getMD5AsHex(name);
    assertEquals(HRegionInfo.MD5_HEX_LENGTH, md5HashInHex.length());
    name = HRegionInfo.createRegionName(tn, sk, id, true);
    nameStr = Bytes.toString(name);
    assertEquals(tableName + "," + startKey + ","
                 + id + "." + md5HashInHex + ".",
                 nameStr);
  }

  @Test
  public void testContainsRange() {
    HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(name.getMethodName()));
    HRegionInfo hri = new HRegionInfo(
        tableDesc.getTableName(), Bytes.toBytes("a"), Bytes.toBytes("g"));
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
  public void testContainsRangeForMetaTable() {
    HTableDescriptor tableDesc = new HTableDescriptor(TableName.META_TABLE_NAME);
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
    HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(name.getMethodName()));
    HRegionInfo hrip = new HRegionInfo(
        tableDesc.getTableName(), Bytes.toBytes("a"), new byte[0]);
    HRegionInfo hric = new HRegionInfo(
        tableDesc.getTableName(), Bytes.toBytes("a"), Bytes.toBytes("b"));
    assertTrue(hrip.compareTo(hric) > 0);
  }

  @Test
  public void testMetaTables() {
    assertTrue(HRegionInfo.FIRST_META_REGIONINFO.isMetaRegion());
  }

  @SuppressWarnings("SelfComparison")
  @Test
  public void testComparator() {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    byte[] empty = new byte[0];
    HRegionInfo older = new HRegionInfo(tableName, empty, empty, false, 0L);
    HRegionInfo newer = new HRegionInfo(tableName, empty, empty, false, 1L);
    assertTrue(older.compareTo(newer) < 0);
    assertTrue(newer.compareTo(older) > 0);
    assertEquals(0, older.compareTo(older));
    assertEquals(0, newer.compareTo(newer));

    HRegionInfo a = new HRegionInfo(TableName.valueOf("a"), null, null);
    HRegionInfo b = new HRegionInfo(TableName.valueOf("b"), null, null);
    assertNotEquals(0, a.compareTo(b));
    HTableDescriptor t = new HTableDescriptor(TableName.valueOf("t"));
    byte [] midway = Bytes.toBytes("midway");
    a = new HRegionInfo(t.getTableName(), null, midway);
    b = new HRegionInfo(t.getTableName(), midway, null);
    assertTrue(a.compareTo(b) < 0);
    assertTrue(b.compareTo(a) > 0);
    assertEquals(a, a);
    assertEquals(0, a.compareTo(a));
    a = new HRegionInfo(t.getTableName(), Bytes.toBytes("a"), Bytes.toBytes("d"));
    b = new HRegionInfo(t.getTableName(), Bytes.toBytes("e"), Bytes.toBytes("g"));
    assertTrue(a.compareTo(b) < 0);
    a = new HRegionInfo(t.getTableName(), Bytes.toBytes("aaaa"), Bytes.toBytes("dddd"));
    b = new HRegionInfo(t.getTableName(), Bytes.toBytes("e"), Bytes.toBytes("g"));
    assertTrue(a.compareTo(b) < 0);
    a = new HRegionInfo(t.getTableName(), Bytes.toBytes("aaaa"), Bytes.toBytes("dddd"));
    b = new HRegionInfo(t.getTableName(), Bytes.toBytes("aaaa"), Bytes.toBytes("eeee"));
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
    byte [] name = HRegionInfo.createRegionName(tn, sk, Bytes.toBytes(id), 0, false);
    String nameStr = Bytes.toString(name);
    assertEquals(tableName + "," + startKey + "," + id, nameStr);

    // replicaId = 1
    name = HRegionInfo.createRegionName(tn, sk, Bytes.toBytes(id), 1, false);
    nameStr = Bytes.toString(name);
    assertEquals(tableName + "," + startKey + "," + id + "_" +
      String.format(HRegionInfo.REPLICA_ID_FORMAT, 1), nameStr);

    // replicaId = max
    name = HRegionInfo.createRegionName(tn, sk, Bytes.toBytes(id), 0xFFFF, false);
    nameStr = Bytes.toString(name);
    assertEquals(tableName + "," + startKey + "," + id + "_" +
        String.format(HRegionInfo.REPLICA_ID_FORMAT, 0xFFFF), nameStr);
  }

  @Test
  public void testParseName() throws IOException {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    byte[] startKey = Bytes.toBytes("startKey");
    long regionId = System.currentTimeMillis();
    int replicaId = 42;

    // test without replicaId
    byte[] regionName = HRegionInfo.createRegionName(tableName, startKey, regionId, false);

    byte[][] fields = HRegionInfo.parseRegionName(regionName);
    assertArrayEquals(Bytes.toString(fields[0]),tableName.getName(), fields[0]);
    assertArrayEquals(Bytes.toString(fields[1]),startKey, fields[1]);
    assertArrayEquals(Bytes.toString(fields[2]), Bytes.toBytes(Long.toString(regionId)),fields[2]);
    assertEquals(3, fields.length);

    // test with replicaId
    regionName = HRegionInfo.createRegionName(tableName, startKey, regionId,
      replicaId, false);

    fields = HRegionInfo.parseRegionName(regionName);
    assertArrayEquals(Bytes.toString(fields[0]),tableName.getName(), fields[0]);
    assertArrayEquals(Bytes.toString(fields[1]),startKey, fields[1]);
    assertArrayEquals(Bytes.toString(fields[2]), Bytes.toBytes(Long.toString(regionId)),fields[2]);
    assertArrayEquals(Bytes.toString(fields[3]), Bytes.toBytes(
      String.format(HRegionInfo.REPLICA_ID_FORMAT, replicaId)), fields[3]);
  }

  @Test
  public void testConvert() {
    final TableName tableName = TableName.valueOf("ns1:" + name.getMethodName());
    byte[] startKey = Bytes.toBytes("startKey");
    byte[] endKey = Bytes.toBytes("endKey");
    boolean split = false;
    long regionId = System.currentTimeMillis();
    int replicaId = 42;


    HRegionInfo hri = new HRegionInfo(tableName, startKey, endKey, split,
      regionId, replicaId);

    // convert two times, compare
    HRegionInfo convertedHri = HRegionInfo.convert(HRegionInfo.convert(hri));

    assertEquals(hri, convertedHri);

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

    convertedHri = HRegionInfo.convert(info);
    HRegionInfo expectedHri = new HRegionInfo(tableName, startKey, endKey, split,
      regionId, 0); // expecting default replicaId

    assertEquals(expectedHri, convertedHri);
  }
  @Test
  public void testRegionDetailsForDisplay() throws IOException {
    byte[] startKey = new byte[] {0x01, 0x01, 0x02, 0x03};
    byte[] endKey = new byte[] {0x01, 0x01, 0x02, 0x04};
    Configuration conf = new Configuration();
    conf.setBoolean("hbase.display.keys", false);
    HRegionInfo h = new HRegionInfo(TableName.valueOf(name.getMethodName()), startKey, endKey);
    checkEquality(h, conf);
    // check HRIs with non-default replicaId
    h = new HRegionInfo(TableName.valueOf(name.getMethodName()), startKey, endKey, false,
        System.currentTimeMillis(), 1);
    checkEquality(h, conf);
    Assert.assertArrayEquals(HRegionInfo.HIDDEN_END_KEY,
        HRegionInfo.getEndKeyForDisplay(h, conf));
    Assert.assertArrayEquals(HRegionInfo.HIDDEN_START_KEY,
        HRegionInfo.getStartKeyForDisplay(h, conf));

    RegionState state = RegionState.createForTesting(h, RegionState.State.OPEN);
    String descriptiveNameForDisplay =
        HRegionInfo.getDescriptiveNameFromRegionStateForDisplay(state, conf);
    checkDescriptiveNameEquality(descriptiveNameForDisplay,state.toDescriptiveString(), startKey);

    conf.setBoolean("hbase.display.keys", true);
    Assert.assertArrayEquals(endKey, HRegionInfo.getEndKeyForDisplay(h, conf));
    Assert.assertArrayEquals(startKey, HRegionInfo.getStartKeyForDisplay(h, conf));
    Assert.assertEquals(state.toDescriptiveString(),
        HRegionInfo.getDescriptiveNameFromRegionStateForDisplay(state, conf));
  }

  private void checkDescriptiveNameEquality(String descriptiveNameForDisplay, String origDesc,
      byte[] startKey) {
    // except for the "hidden-start-key" substring everything else should exactly match
    String firstPart = descriptiveNameForDisplay.substring(0,
        descriptiveNameForDisplay.indexOf(new String(HRegionInfo.HIDDEN_START_KEY)));
    String secondPart = descriptiveNameForDisplay.substring(
        descriptiveNameForDisplay.indexOf(new String(HRegionInfo.HIDDEN_START_KEY)) +
        HRegionInfo.HIDDEN_START_KEY.length);
    String firstPartOrig = origDesc.substring(0,
        origDesc.indexOf(Bytes.toStringBinary(startKey)));
    String secondPartOrig = origDesc.substring(
        origDesc.indexOf(Bytes.toStringBinary(startKey)) +
        Bytes.toStringBinary(startKey).length());
    assert(firstPart.equals(firstPartOrig));
    assert(secondPart.equals(secondPartOrig));
  }

  private void checkEquality(HRegionInfo h, Configuration conf) throws IOException {
    byte[] modifiedRegionName = HRegionInfo.getRegionNameForDisplay(h, conf);
    byte[][] modifiedRegionNameParts = HRegionInfo.parseRegionName(modifiedRegionName);
    byte[][] regionNameParts = HRegionInfo.parseRegionName(h.getRegionName());

    //same number of parts
    assert(modifiedRegionNameParts.length == regionNameParts.length);

    for (int i = 0; i < regionNameParts.length; i++) {
      // all parts should match except for [1] where in the modified one,
      // we should have "hidden_start_key"
      if (i != 1) {
        Assert.assertArrayEquals(regionNameParts[i], modifiedRegionNameParts[i]);
      } else {
        assertNotEquals(regionNameParts[i][0], modifiedRegionNameParts[i][0]);
        Assert.assertArrayEquals(modifiedRegionNameParts[1],
            HRegionInfo.getStartKeyForDisplay(h, conf));
      }
    }
  }
}

