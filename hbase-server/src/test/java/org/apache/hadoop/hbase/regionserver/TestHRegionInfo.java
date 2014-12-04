/**
 *
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
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.SmallTests;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.RegionInfo;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSTableDescriptors;
import org.apache.hadoop.hbase.util.MD5Hash;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.protobuf.ByteString;

@Category(SmallTests.class)
public class TestHRegionInfo {
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
    FSTableDescriptors fsTableDescriptors = new FSTableDescriptors(htu.getConfiguration());
    // Create a region.  That'll write the .regioninfo file.
    HRegion r = HRegion.createHRegion(hri, basedir, htu.getConfiguration(),
      fsTableDescriptors.get(TableName.META_TABLE_NAME));
    // Get modtime on the file.
    long modtime = getModTime(r);
    HRegion.closeHRegion(r);
    Thread.sleep(1001);
    r = HRegion.openHRegion(basedir, hri, fsTableDescriptors.get(TableName.META_TABLE_NAME),
      null, htu.getConfiguration());
    // Ensure the file is not written for a second time.
    long modtime2 = getModTime(r);
    assertEquals(modtime, modtime2);
    // Now load the file.
    HRegionInfo deserializedHri = HRegionFileSystem.loadRegionInfoFileContent(
        r.getRegionFileSystem().getFileSystem(), r.getRegionFileSystem().getRegionDir());
    assertTrue(hri.equals(deserializedHri));
  }

  long getModTime(final HRegion r) throws IOException {
    FileStatus[] statuses = r.getRegionFileSystem().getFileSystem().listStatus(
      new Path(r.getRegionFileSystem().getRegionDir(), HRegionFileSystem.REGION_INFO_FILE));
    assertTrue(statuses != null && statuses.length == 1);
    return statuses[0].getModificationTime();
  }

  @Test
  public void testCreateHRegionInfoName() throws Exception {
    String tableName = "tablename";
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
    HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf("testtable"));
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
  public void testLastRegionCompare() {
    HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf("testtable"));
    HRegionInfo hrip = new HRegionInfo(
        tableDesc.getTableName(), Bytes.toBytes("a"), new byte[0]);
    HRegionInfo hric = new HRegionInfo(
        tableDesc.getTableName(), Bytes.toBytes("a"), Bytes.toBytes("b"));
    assertTrue(hrip.compareTo(hric) > 0);
  }

  @Test
  public void testMetaTables() {
    assertTrue(HRegionInfo.FIRST_META_REGIONINFO.isMetaTable());
  }

  @Test
  public void testComparator() {
    TableName tablename = TableName.valueOf("comparatorTablename");
    byte[] empty = new byte[0];
    HRegionInfo older = new HRegionInfo(tablename, empty, empty, false, 0L);
    HRegionInfo newer = new HRegionInfo(tablename, empty, empty, false, 1L);
    assertTrue(older.compareTo(newer) < 0);
    assertTrue(newer.compareTo(older) > 0);
    assertTrue(older.compareTo(older) == 0);
    assertTrue(newer.compareTo(newer) == 0);
  }

  @Test
  public void testRegionNameForRegionReplicas() throws Exception {
    String tableName = "tablename";
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
    TableName tableName = TableName.valueOf("testParseName");
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
    TableName tableName = TableName.valueOf("ns1:table1");
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
    RegionInfo info = RegionInfo.newBuilder()
      .setTableName(HBaseProtos.TableName.newBuilder()
        .setQualifier(ByteString.copyFrom(tableName.getQualifier()))
        .setNamespace(ByteString.copyFrom(tableName.getNamespace()))
        .build())
      .setStartKey(ByteString.copyFrom(startKey))
      .setEndKey(ByteString.copyFrom(endKey))
      .setSplit(split)
      .setRegionId(regionId)
      .build();

    convertedHri = HRegionInfo.convert(info);
    HRegionInfo expectedHri = new HRegionInfo(tableName, startKey, endKey, split,
      regionId, 0); // expecting default replicaId

    assertEquals(expectedHri, convertedHri);
  }

}

