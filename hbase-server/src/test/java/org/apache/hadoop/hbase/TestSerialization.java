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
package org.apache.hadoop.hbase;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.ByteBufferUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.io.DataInputBuffer;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos;

/**
 * Test HBase Writables serializations
 */
@Category({ MiscTests.class, SmallTests.class })
public class TestSerialization {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestSerialization.class);

  @Test
  public void testKeyValue() throws Exception {
    final String name = "testKeyValue2";
    byte[] row = Bytes.toBytes(name);
    byte[] fam = Bytes.toBytes("fam");
    byte[] qf = Bytes.toBytes("qf");
    long ts = EnvironmentEdgeManager.currentTime();
    byte[] val = Bytes.toBytes("val");
    KeyValue kv = new KeyValue(row, fam, qf, ts, val);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    KeyValueUtil.write(kv, dos);
    dos.close();
    byte[] mb = baos.toByteArray();
    ByteArrayInputStream bais = new ByteArrayInputStream(mb);
    DataInputStream dis = new DataInputStream(bais);
    KeyValue deserializedKv = KeyValueUtil.create(dis);
    assertTrue(Bytes.equals(kv.getBuffer(), deserializedKv.getBuffer()));
    assertEquals(kv.getOffset(), deserializedKv.getOffset());
    assertEquals(kv.getLength(), deserializedKv.getLength());
  }

  @Test
  public void testCreateKeyValueInvalidNegativeLength() {

    KeyValue kv_0 = new KeyValue(Bytes.toBytes("myRow"), Bytes.toBytes("myCF"), // 51 bytes
      Bytes.toBytes("myQualifier"), 12345L, Bytes.toBytes("my12345"));

    KeyValue kv_1 = new KeyValue(Bytes.toBytes("myRow"), Bytes.toBytes("myCF"), // 49 bytes
      Bytes.toBytes("myQualifier"), 12345L, Bytes.toBytes("my123"));

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);

    long l = 0;
    try {
      ByteBufferUtils.putInt(dos, kv_0.getSerializedSize(false));
      l =  (long) kv_0.write(dos, false) + Bytes.SIZEOF_INT;
      ByteBufferUtils.putInt(dos, kv_1.getSerializedSize(false));
      l += (long) kv_1.write(dos, false) + Bytes.SIZEOF_INT;
      assertEquals(100L, l);
    } catch (IOException e) {
      fail("Unexpected IOException" + e.getMessage());
    }

    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    DataInputStream dis = new DataInputStream(bais);

    try {
      KeyValueUtil.create(dis);
      assertTrue(kv_0.equals(kv_1));
    } catch (Exception e) {
      fail("Unexpected Exception" + e.getMessage());
    }

    // length -1
    try {
      // even if we have a good kv now in dis we will just pass length with -1 for simplicity
      KeyValueUtil.create(-1, dis);
      fail("Expected corrupt stream");
    } catch (Exception e) {
      assertEquals("Failed read -1 bytes, stream corrupt?", e.getMessage());
    }

  }

  @Test
  public void testCompareFilter() throws Exception {
    Filter f =
      new RowFilter(CompareOperator.EQUAL, new BinaryComparator(Bytes.toBytes("testRowOne-2")));
    byte[] bytes = f.toByteArray();
    Filter ff = RowFilter.parseFrom(bytes);
    assertNotNull(ff);
  }

  @Test
  public void testTableDescriptor() throws Exception {
    final String name = "testTableDescriptor";
    TableDescriptor htd = createTableDescriptor(name);
    byte[] mb = TableDescriptorBuilder.toByteArray(htd);
    TableDescriptor deserializedHtd = TableDescriptorBuilder.parseFrom(mb);
    assertEquals(htd.getTableName(), deserializedHtd.getTableName());
  }

  /**
   * Test RegionInfo serialization
   */
  @Test
  public void testRegionInfo() throws Exception {
    RegionInfo hri = createRandomRegion("testRegionInfo");

    // test toByteArray()
    byte[] hrib = RegionInfo.toByteArray(hri);
    RegionInfo deserializedHri = RegionInfo.parseFrom(hrib);
    assertEquals(hri.getEncodedName(), deserializedHri.getEncodedName());
    assertEquals(hri, deserializedHri);

    // test toDelimitedByteArray()
    hrib = RegionInfo.toDelimitedByteArray(hri);
    DataInputBuffer buf = new DataInputBuffer();
    try {
      buf.reset(hrib, hrib.length);
      deserializedHri = RegionInfo.parseFrom(buf);
      assertEquals(hri.getEncodedName(), deserializedHri.getEncodedName());
      assertEquals(hri, deserializedHri);
    } finally {
      buf.close();
    }
  }

  @Test
  public void testRegionInfos() throws Exception {
    RegionInfo hri = createRandomRegion("testRegionInfos");
    byte[] triple = RegionInfo.toDelimitedByteArray(hri, hri, hri);
    List<RegionInfo> regions = RegionInfo.parseDelimitedFrom(triple, 0, triple.length);
    assertTrue(regions.size() == 3);
    assertTrue(regions.get(0).equals(regions.get(1)));
    assertTrue(regions.get(0).equals(regions.get(2)));
  }

  private RegionInfo createRandomRegion(final String name) {
    TableDescriptorBuilder tableDescriptorBuilder =
      TableDescriptorBuilder.newBuilder(TableName.valueOf(name));
    String[] families = new String[] { "info", "anchor" };
    for (int i = 0; i < families.length; i++) {
      ColumnFamilyDescriptor columnFamilyDescriptor =
        ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(families[i])).build();
      tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptor);
    }
    TableDescriptor tableDescriptor = tableDescriptorBuilder.build();
    return RegionInfoBuilder.newBuilder(tableDescriptor.getTableName()).build();
  }

  @Test
  public void testGet() throws Exception {
    byte[] row = Bytes.toBytes("row");
    byte[] fam = Bytes.toBytes("fam");
    byte[] qf1 = Bytes.toBytes("qf1");
    long ts = EnvironmentEdgeManager.currentTime();
    int maxVersions = 2;

    Get get = new Get(row);
    get.addColumn(fam, qf1);
    get.setTimeRange(ts, ts + 1);
    get.readVersions(maxVersions);

    ClientProtos.Get getProto = ProtobufUtil.toGet(get);
    Get desGet = ProtobufUtil.toGet(getProto);

    assertTrue(Bytes.equals(get.getRow(), desGet.getRow()));
    Set<byte[]> set = null;
    Set<byte[]> desSet = null;

    for (Map.Entry<byte[], NavigableSet<byte[]>> entry : get.getFamilyMap().entrySet()) {
      assertTrue(desGet.getFamilyMap().containsKey(entry.getKey()));
      set = entry.getValue();
      desSet = desGet.getFamilyMap().get(entry.getKey());
      for (byte[] qualifier : set) {
        assertTrue(desSet.contains(qualifier));
      }
    }

    assertEquals(get.getMaxVersions(), desGet.getMaxVersions());
    TimeRange tr = get.getTimeRange();
    TimeRange desTr = desGet.getTimeRange();
    assertEquals(tr.getMax(), desTr.getMax());
    assertEquals(tr.getMin(), desTr.getMin());
  }

  @Test
  public void testScan() throws Exception {

    byte[] startRow = Bytes.toBytes("startRow");
    byte[] stopRow = Bytes.toBytes("stopRow");
    byte[] fam = Bytes.toBytes("fam");
    byte[] qf1 = Bytes.toBytes("qf1");
    long ts = EnvironmentEdgeManager.currentTime();
    int maxVersions = 2;

    Scan scan = new Scan().withStartRow(startRow).withStopRow(stopRow);
    scan.addColumn(fam, qf1);
    scan.setTimeRange(ts, ts + 1);
    scan.readVersions(maxVersions);

    ClientProtos.Scan scanProto = ProtobufUtil.toScan(scan);
    Scan desScan = ProtobufUtil.toScan(scanProto);

    assertTrue(Bytes.equals(scan.getStartRow(), desScan.getStartRow()));
    assertTrue(Bytes.equals(scan.getStopRow(), desScan.getStopRow()));
    assertEquals(scan.getCacheBlocks(), desScan.getCacheBlocks());
    Set<byte[]> set = null;
    Set<byte[]> desSet = null;

    for (Map.Entry<byte[], NavigableSet<byte[]>> entry : scan.getFamilyMap().entrySet()) {
      assertTrue(desScan.getFamilyMap().containsKey(entry.getKey()));
      set = entry.getValue();
      desSet = desScan.getFamilyMap().get(entry.getKey());
      for (byte[] column : set) {
        assertTrue(desSet.contains(column));
      }

      // Test filters are serialized properly.
      scan = new Scan().withStartRow(startRow);
      final String name = "testScan";
      byte[] prefix = Bytes.toBytes(name);
      scan.setFilter(new PrefixFilter(prefix));
      scanProto = ProtobufUtil.toScan(scan);
      desScan = ProtobufUtil.toScan(scanProto);
      Filter f = desScan.getFilter();
      assertTrue(f instanceof PrefixFilter);
    }

    assertEquals(scan.getMaxVersions(), desScan.getMaxVersions());
    TimeRange tr = scan.getTimeRange();
    TimeRange desTr = desScan.getTimeRange();
    assertEquals(tr.getMax(), desTr.getMax());
    assertEquals(tr.getMin(), desTr.getMin());
  }

  protected static final int MAXVERSIONS = 3;
  protected final static byte[] fam1 = Bytes.toBytes("colfamily1");
  protected final static byte[] fam2 = Bytes.toBytes("colfamily2");
  protected final static byte[] fam3 = Bytes.toBytes("colfamily3");
  protected static final byte[][] COLUMNS = { fam1, fam2, fam3 };

  /**
   * Create a table of name <code>name</code> with {@link #COLUMNS} for families.
   * @param name Name to give table.
   * @return Column descriptor.
   */
  protected TableDescriptor createTableDescriptor(final String name) {
    return createTableDescriptor(name, MAXVERSIONS);
  }

  /**
   * Create a table of name <code>name</code> with {@link #COLUMNS} for families.
   * @param name     Name to give table.
   * @param versions How many versions to allow per column.
   * @return Column descriptor.
   */
  protected TableDescriptor createTableDescriptor(final String name, final int versions) {
    TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(TableName.valueOf(name));
    builder
      .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(fam1).setMaxVersions(versions)
        .setBlockCacheEnabled(false).build())
      .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(fam2).setMaxVersions(versions)
        .setBlockCacheEnabled(false).build())
      .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(fam3).setMaxVersions(versions)
        .setBlockCacheEnabled(false).build());
    return builder.build();
  }
}
