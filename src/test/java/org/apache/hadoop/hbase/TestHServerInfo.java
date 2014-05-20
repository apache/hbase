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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HServerLoad.RegionLoad;
import org.apache.hadoop.hbase.ipc.HBaseRPCOptions;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.hbase.ipc.ThriftHRegionInterface;
import org.apache.hadoop.hbase.ipc.thrift.HBaseThriftRPC;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.transport.TMemoryBuffer;
import org.junit.Test;

import com.facebook.swift.codec.ThriftCodec;
import com.facebook.swift.codec.ThriftCodecManager;
import org.junit.experimental.categories.Category;

@Category(MediumTests.class)
public class TestHServerInfo {

  @Test
  public void testHashCodeAndEquals() {
    HServerAddress hsa1 = new HServerAddress("localhost", 1234);
    HServerInfo hsi1 = new HServerInfo(hsa1, 1L);
    HServerInfo hsi2 = new HServerInfo(hsa1, 1L);
    HServerInfo hsi3 = new HServerInfo(hsa1, 2L);
    HServerInfo hsi4 = new HServerInfo(hsa1, 1L);
    HServerAddress hsa2 = new HServerAddress("localhost", 1235);
    HServerInfo hsi5 = new HServerInfo(hsa2, 1L);
    assertEquals(hsi1.hashCode(), hsi2.hashCode());
    assertTrue(hsi1.equals(hsi2));
    assertFalse(hsi1.hashCode() == hsi3.hashCode());
    assertFalse(hsi1.equals(hsi3));
    assertEquals(hsi1.hashCode(), hsi4.hashCode());
    assertTrue(hsi1.equals(hsi4));
    assertFalse(hsi1.hashCode() == hsi5.hashCode());
    assertFalse(hsi1.equals(hsi5));
  }

  @Test
  public void testHServerInfoHServerInfo() {
    HServerAddress hsa1 = new HServerAddress("localhost", 1234);
    HServerInfo hsi1 = new HServerInfo(hsa1, 1L);
    HServerInfo hsi2 = new HServerInfo(hsi1);
    assertEquals(hsi1, hsi2);
  }

  @Test
  public void testGetServerAddress() {
    HServerAddress hsa1 = new HServerAddress("localhost", 1234);
    HServerInfo hsi1 = new HServerInfo(hsa1, 1L);
    assertEquals(hsi1.getServerAddress(), hsa1);
  }

  @Test
  public void testToString() {
    HServerAddress hsa1 = new HServerAddress("localhost", 1234);
    HServerInfo hsi1 = new HServerInfo(hsa1, 1L);
    System.out.println(hsi1.toString());
  }

  @Test
  public void testReadFields() throws IOException {
    HServerAddress hsa1 = new HServerAddress("localhost", 1234);
    HServerInfo hsi1 = new HServerInfo(hsa1, 1L);
    HServerAddress hsa2 = new HServerAddress("localhost", 1235);
    HServerInfo hsi2 = new HServerInfo(hsa2, 1L);
    byte [] bytes = Writables.getBytes(hsi1);
    HServerInfo deserialized =
      (HServerInfo)Writables.getWritable(bytes, new HServerInfo());
    assertEquals(hsi1, deserialized);
    bytes = Writables.getBytes(hsi2);
    deserialized = (HServerInfo)Writables.getWritable(bytes, new HServerInfo());
    assertNotSame(hsa1, deserialized);
  }

  @Test
  public void testCompareTo() {
    HServerAddress hsa1 = new HServerAddress("localhost", 1234);
    HServerInfo hsi1 = new HServerInfo(hsa1, 1L);
    HServerAddress hsa2 = new HServerAddress("localhost", 1235);
    HServerInfo hsi2 = new HServerInfo(hsa2, 1L);
    assertTrue(hsi1.compareTo(hsi1) == 0);
    assertTrue(hsi2.compareTo(hsi2) == 0);
    int compare1 = hsi1.compareTo(hsi2);
    int compare2 = hsi2.compareTo(hsi1);
    assertTrue((compare1 > 0)? compare2 < 0: compare2 > 0);
  }

  @Test
  public void testHostNameToString() throws IOException {

    // Constructor test
    HServerAddress hsa1 = new HServerAddress("localhost", 1234);
    assertTrue(hsa1.toString().equals("127.0.0.1:1234"));

    // Writable
    File tempFile = new File("test.out");
    tempFile.createNewFile();
    assertTrue(tempFile.exists());

    FileOutputStream fos = new FileOutputStream("test.out");

    DataOutputStream dos = new DataOutputStream(fos);
    dos.writeUTF("localhost");
    dos.writeInt(1234);
    dos.flush();
    dos.close();

    FileInputStream fis = new FileInputStream("test.out");
    DataInputStream dis = new DataInputStream(fis);

    HServerAddress hsa2 = new HServerAddress();
    hsa2.readFields(dis);

    assertTrue(hsa2.toString().equals("127.0.0.1:1234"));
    dis.close();

    assertTrue(tempFile.delete());

   }


  @Test
  public void testFromServerName() {
    String host = "127.0.0.1";
    int port = 60020;
    long startCode = 1343258056696L;
    String serverName = host + HServerInfo.SERVERNAME_SEPARATOR + port +
        HServerInfo.SERVERNAME_SEPARATOR + startCode;
    HServerInfo hsi = HServerInfo.fromServerName(serverName);
    assertEquals(host, hsi.getHostname());
    assertEquals(port, hsi.getServerAddress().getPort());
    assertEquals(startCode, hsi.getStartCode());
    assertTrue(HServerInfo.isValidServerName(serverName));
    assertEquals(serverName, hsi.getServerName());
  }

  @Test
  public void testIsValidServerName() {
    assertTrue(HServerInfo.isValidServerName("foo.bar,60020," + Long.MAX_VALUE));
    assertTrue(HServerInfo.isValidServerName("127.0.0.1,60020," + Long.MAX_VALUE));
    assertTrue(HServerInfo.isValidServerName("www.acme.com,80," + Long.MIN_VALUE));
    assertFalse(HServerInfo.isValidServerName(",www.acme.com,80,0"));
    assertTrue(HServerInfo.isValidServerName("foo.bar,60020," + Long.MAX_VALUE));
    assertFalse(HServerInfo.isValidServerName("foo.bar,60020," + Long.MAX_VALUE + "a"));
    assertFalse(HServerInfo.isValidServerName(",60020," + Long.MAX_VALUE));
    assertFalse(HServerInfo.isValidServerName(" ,60020," + Long.MAX_VALUE));
    assertFalse(HServerInfo.isValidServerName("  ,60020," + Long.MAX_VALUE));
    assertFalse(HServerInfo.isValidServerName("!>;@#@#localhost,60020," +
                                              Long.MAX_VALUE));
  }

  /**
   * Tests if the HServerInfo object was correctly serialized and deserialized
   * @throws Exception
   */
  @Test
  public void testSwiftSerializationDeserialization() throws Exception {
    ThriftCodec<HServerInfo> codec = new ThriftCodecManager()
    .getCodec(HServerInfo.class);
    TMemoryBuffer transport = new TMemoryBuffer(1000 * 1024);
    TCompactProtocol protocol = new TCompactProtocol(transport);
    Map<byte[], Long> flushedSequenceIdByRegion = new ConcurrentSkipListMap<>(Bytes.BYTES_COMPARATOR);
    flushedSequenceIdByRegion.put(new byte[] {1,  2, 3}, 1L);
    flushedSequenceIdByRegion.put(new byte[] {4,  5, 6}, 5L);
    //create HServerLoad object using the thrift constructor
    ArrayList<RegionLoad> regionLoad = new ArrayList<>();
    regionLoad.add(new RegionLoad(new byte[]{10, 11, 12}, 5, 30, 512, 530, 450, 410, 456878, 135487));
    HServerLoad serverLoad = new HServerLoad(3, 100, 50, 3000, regionLoad, 10000, 20000);
    HServerInfo info = new HServerInfo(new HServerAddress("127.0.0.1:60020"), 1, "localhost", flushedSequenceIdByRegion, true, "bla", serverLoad);

    codec.write(info, protocol);
    HServerInfo infoCopy = codec.read(protocol);
    Assert.assertEquals(info, infoCopy);
  }


  /**
   * Tests the getHserverInfo call using thrift.
   * @throws Exception
   */
  @Test
  public void testGetHserverInfo() throws Exception {
    byte[] table = Bytes.toBytes("table");
    byte[] family = Bytes.toBytes("family");

    HBaseTestingUtility testUtil = new HBaseTestingUtility();
    testUtil.startMiniCluster();
    Configuration conf = testUtil.getConfiguration();

    testUtil.createTable(table, family);

    List<HRegion> regions = testUtil.getMiniHBaseCluster().getRegions(table);
    HRegion region = regions.get(0);
    HRegionServer regionServer = region.getRegionServer();

    HRegionInterface client = (HRegionInterface) HBaseThriftRPC.getClient(
        regionServer.getServerInfo().getServerAddress().getInetSocketAddress(),
        conf, ThriftHRegionInterface.Async.class, HBaseRPCOptions.DEFAULT);

    assertEquals(regionServer.getHServerInfo(), client.getHServerInfo());
  }

}


