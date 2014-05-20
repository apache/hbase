/**
 * Copyright 2007 The Apache Software Foundation
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.SmallTests;
import org.apache.hadoop.hbase.ipc.HBaseRPCOptions;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.hbase.ipc.ThriftHRegionInterface;
import org.apache.hadoop.hbase.ipc.thrift.HBaseThriftRPC;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.MD5Hash;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.transport.TMemoryBuffer;
import org.junit.Test;

import com.facebook.swift.codec.ThriftCodec;
import com.facebook.swift.codec.ThriftCodecManager;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestHRegionInfo {
  @Test
  public void testCreateHRegionInfoName() throws Exception {
    String tableName = "tablename";
    final byte[] tn = Bytes.toBytes(tableName);
    String startKey = "startkey";
    final byte[] sk = Bytes.toBytes(startKey);
    String id = "id";

    // old format region name
    byte[] name = HRegionInfo.createRegionName(tn, sk, id, false);
    String nameStr = Bytes.toString(name);
    assertEquals(tableName + "," + startKey + "," + id, nameStr);

    // new format region name.
    String md5HashInHex = MD5Hash.getMD5AsHex(name);
    assertEquals(HRegionInfo.MD5_HEX_LENGTH, md5HashInHex.length());
    name = HRegionInfo.createRegionName(tn, sk, id, true);
    nameStr = Bytes.toString(name);
    assertEquals(tableName + "," + startKey + "," + id + "." + md5HashInHex
        + ".", nameStr);
  }

  @Test
  public void testContainsRange() {
    HTableDescriptor tableDesc = new HTableDescriptor("testtable");
    HRegionInfo hri = new HRegionInfo(tableDesc, Bytes.toBytes("a"),
        Bytes.toBytes("g"));
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
  public void testRootRegionName() {
    assertEquals("70236052", HRegionInfo.ROOT_REGION_ENCODED_NAME_STR);
  }

  /**
   * Tests if the HRegionInfo object is correctly serialized & deserialized
   *
   * @throws Exception
   */
  @Test
  public void testSwiftSerDe() throws Exception {
    ThriftCodec<HRegionInfo> codec = new ThriftCodecManager()
        .getCodec(HRegionInfo.class);
    TMemoryBuffer transport = new TMemoryBuffer(1000 * 1024);
    TCompactProtocol protocol = new TCompactProtocol(transport);
    String startKey = "start";
    String endKey = "end";
    boolean split = false;
    long regionId = 0;
    String tableDescString = "tableDesc";
    List<HColumnDescriptor> families = new ArrayList<HColumnDescriptor>();
    families.add(new HColumnDescriptor(Bytes.toBytes("family")));
    Map<byte[], byte[]> values = new HashMap<byte[], byte[]>();
    values.put(Bytes.toBytes("strudel"), Bytes.toBytes("cream"));
    values.put(Bytes.toBytes("foo"), Bytes.toBytes("bar"));

    // test a random region info
    HTableDescriptor tableDesc = new HTableDescriptor(Bytes
        .toBytes(tableDescString), families, values);
    HRegionInfo hregionInfo = new HRegionInfo(tableDesc,
        Bytes.toBytes(startKey), Bytes.toBytes(endKey), split, regionId, new byte[] { 1, 2, 3 }, false);
    assertTrue(!hregionInfo.getTableDesc().isMetaRegion());
    codec.write(hregionInfo, protocol);
    HRegionInfo hregionInfoCopy = codec.read(protocol);
    assertEquals(hregionInfo, hregionInfoCopy);
    assertEquals(hregionInfo.hashCode(), hregionInfoCopy.hashCode());

    // test a root region info
    hregionInfo = HRegionInfo.ROOT_REGIONINFO;
    assertTrue(hregionInfo.getTableDesc().isMetaRegion());
    codec.write(hregionInfo, protocol);
    hregionInfoCopy = codec.read(protocol);
    assertEquals(hregionInfo, hregionInfoCopy);
    assertEquals(hregionInfo.hashCode(), hregionInfoCopy.hashCode());

    // test a root region info with historian column
    hregionInfo = HRegionInfo.ROOT_REGIONINFO_WITH_HISTORIAN_COLUMN;
    assertTrue(hregionInfo.getTableDesc().isMetaRegion());
    codec.write(hregionInfo, protocol);
    hregionInfoCopy = codec.read(protocol);
    assertEquals(hregionInfo, hregionInfoCopy);
    assertEquals(hregionInfo.hashCode(), hregionInfoCopy.hashCode());

    assertTrue(Bytes.equals(HRegionInfo.ROOT_REGIONINFO.getRegionName(),
                 HRegionInfo.ROOT_REGIONINFO_WITH_HISTORIAN_COLUMN.getRegionName()));
    assertEquals(HRegionInfo.ROOT_REGIONINFO.getEncodedName(),
                 HRegionInfo.ROOT_REGIONINFO_WITH_HISTORIAN_COLUMN.getEncodedName());

    // test a meta region info
    hregionInfo = HRegionInfo.FIRST_META_REGIONINFO;
    assertTrue(hregionInfo.getTableDesc().isMetaRegion());
    codec.write(hregionInfo, protocol);
    hregionInfoCopy = codec.read(protocol);
    assertEquals(hregionInfo, hregionInfoCopy);
    assertEquals(hregionInfo.hashCode(), hregionInfoCopy.hashCode());
  }

  /**
   * Tests the thrift call of getRegionInfo.
   *
   * @throws Exception
   */
  @Test
  public void testGetRegionInfo() throws Exception {
    byte[] table = Bytes.toBytes("table");
    byte[] family = Bytes.toBytes("family");

    HBaseTestingUtility testUtil = new HBaseTestingUtility();
    testUtil.startMiniCluster();
    Configuration conf = testUtil.getConfiguration();

    testUtil.createTable(table, new byte[][] { family }, 3,
        Bytes.toBytes("bbb"), Bytes.toBytes("yyy"), 25);

    List<HRegion> regions = testUtil.getMiniHBaseCluster().getRegions(table);
    HRegion region = regions.get(0);

    InetSocketAddress addr = new InetSocketAddress(region.getRegionServer()
        .getServerInfo().getHostname(),
       region.getRegionServer().getThriftServerPort());
    HRegionInterface server = (HRegionInterface) HBaseThriftRPC.getClient(addr,
        conf, ThriftHRegionInterface.Async.class, HBaseRPCOptions.DEFAULT);
    HRegionInfo regionInfo = server.getRegionInfo(region.getRegionName());
    assertEquals(region.getRegionInfo(), regionInfo);
    testUtil.shutdownMiniCluster();
  }

  /**
   * This is a special test case for verifying that the HRegionInfo objects are
   * serialized correctly for the -ROOT- table. This case led to discovery of
   * problems with the HColumnDescriptor annotation, hence adding this.
   *
   * @throws Exception
   */
  @Test
  public void testGetRegionInfoForRoot() throws Exception {
    byte[] table = HConstants.ROOT_TABLE_NAME;

    HBaseTestingUtility testUtil = new HBaseTestingUtility();
    testUtil.startMiniCluster();

    List<HRegion> regions = testUtil.getMiniHBaseCluster().getRegions(table);
    for (HRegion region : regions) {
      HRegionInfo hri = region.getRegionInfo();
      HRegionInfo hriCopy = Bytes.readThriftBytes(
          Bytes.writeThriftBytes(hri, HRegionInfo.class), HRegionInfo.class);
      assertEquals(hri, hriCopy);
    }
    testUtil.shutdownMiniCluster();
  }
}
