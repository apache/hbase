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
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DataInputBuffer;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos;

/**
 * Test HBase Writables serializations
 */
@Category({MiscTests.class, SmallTests.class})
public class TestSerialization {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestSerialization.class);

  @Test public void testKeyValue() throws Exception {
    final String name = "testKeyValue2";
    byte[] row = Bytes.toBytes(name);
    byte[] fam = Bytes.toBytes("fam");
    byte[] qf = Bytes.toBytes("qf");
    long ts = System.currentTimeMillis();
    byte[] val = Bytes.toBytes("val");
    KeyValue kv = new KeyValue(row, fam, qf, ts, val);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    long l = KeyValueUtil.write(kv, dos);
    dos.close();
    byte [] mb = baos.toByteArray();
    ByteArrayInputStream bais = new ByteArrayInputStream(mb);
    DataInputStream dis = new DataInputStream(bais);
    KeyValue deserializedKv = KeyValueUtil.create(dis);
    assertTrue(Bytes.equals(kv.getBuffer(), deserializedKv.getBuffer()));
    assertEquals(kv.getOffset(), deserializedKv.getOffset());
    assertEquals(kv.getLength(), deserializedKv.getLength());
  }

  @Test public void testCreateKeyValueInvalidNegativeLength() {

    KeyValue kv_0 = new KeyValue(Bytes.toBytes("myRow"), Bytes.toBytes("myCF"),       // 51 bytes
                                 Bytes.toBytes("myQualifier"), 12345L, Bytes.toBytes("my12345"));

    KeyValue kv_1 = new KeyValue(Bytes.toBytes("myRow"), Bytes.toBytes("myCF"),       // 49 bytes
                                 Bytes.toBytes("myQualifier"), 12345L, Bytes.toBytes("my123"));

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);

    long l = 0;
    try {
      l  = KeyValue.oswrite(kv_0, dos, false);
      l += KeyValue.oswrite(kv_1, dos, false);
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

  @Test public void testCompareFilter() throws Exception {
    Filter f = new RowFilter(CompareOperator.EQUAL,
      new BinaryComparator(Bytes.toBytes("testRowOne-2")));
    byte [] bytes = f.toByteArray();
    Filter ff = RowFilter.parseFrom(bytes);
    assertNotNull(ff);
  }

  @Test public void testTableDescriptor() throws Exception {
    final String name = "testTableDescriptor";
    HTableDescriptor htd = createTableDescriptor(name);
    byte [] mb = htd.toByteArray();
    HTableDescriptor deserializedHtd = HTableDescriptor.parseFrom(mb);
    assertEquals(htd.getTableName(), deserializedHtd.getTableName());
  }

  /**
   * Test RegionInfo serialization
   * @throws Exception
   */
  @Test public void testRegionInfo() throws Exception {
    HRegionInfo hri = createRandomRegion("testRegionInfo");

    //test toByteArray()
    byte [] hrib = hri.toByteArray();
    HRegionInfo deserializedHri = HRegionInfo.parseFrom(hrib);
    assertEquals(hri.getEncodedName(), deserializedHri.getEncodedName());
    assertEquals(hri, deserializedHri);

    //test toDelimitedByteArray()
    hrib = hri.toDelimitedByteArray();
    DataInputBuffer buf = new DataInputBuffer();
    try {
      buf.reset(hrib, hrib.length);
      deserializedHri = HRegionInfo.parseFrom(buf);
      assertEquals(hri.getEncodedName(), deserializedHri.getEncodedName());
      assertEquals(hri, deserializedHri);
    } finally {
      buf.close();
    }
  }

  @Test public void testRegionInfos() throws Exception {
    HRegionInfo hri = createRandomRegion("testRegionInfos");
    byte[] triple = HRegionInfo.toDelimitedByteArray(hri, hri, hri);
    List<HRegionInfo> regions = HRegionInfo.parseDelimitedFrom(triple, 0, triple.length);
    assertTrue(regions.size() == 3);
    assertTrue(regions.get(0).equals(regions.get(1)));
    assertTrue(regions.get(0).equals(regions.get(2)));
  }

  private HRegionInfo createRandomRegion(final String name) {
    TableDescriptorBuilder tableDescriptorBuilder =
      TableDescriptorBuilder.newBuilder(TableName.valueOf(name));
    String[] families = new String[]{"info", "anchor"};
    for (int i = 0; i < families.length; i++) {
      ColumnFamilyDescriptor columnFamilyDescriptor =
        ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(families[i])).build();
      tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptor);
    }
    TableDescriptor tableDescriptor = tableDescriptorBuilder.build();
    return new HRegionInfo(tableDescriptor.getTableName(), HConstants.EMPTY_START_ROW,
      HConstants.EMPTY_END_ROW);
  }

  /*
   * TODO
  @Test public void testPut() throws Exception{
    byte[] row = "row".getBytes();
    byte[] fam = "fam".getBytes();
    byte[] qf1 = "qf1".getBytes();
    byte[] qf2 = "qf2".getBytes();
    byte[] qf3 = "qf3".getBytes();
    byte[] qf4 = "qf4".getBytes();
    byte[] qf5 = "qf5".getBytes();
    byte[] qf6 = "qf6".getBytes();
    byte[] qf7 = "qf7".getBytes();
    byte[] qf8 = "qf8".getBytes();

    long ts = System.currentTimeMillis();
    byte[] val = "val".getBytes();

    Put put = new Put(row);
    put.setWriteToWAL(false);
    put.add(fam, qf1, ts, val);
    put.add(fam, qf2, ts, val);
    put.add(fam, qf3, ts, val);
    put.add(fam, qf4, ts, val);
    put.add(fam, qf5, ts, val);
    put.add(fam, qf6, ts, val);
    put.add(fam, qf7, ts, val);
    put.add(fam, qf8, ts, val);

    byte[] sb = Writables.getBytes(put);
    Put desPut = (Put)Writables.getWritable(sb, new Put());

    //Timing test
//    long start = System.nanoTime();
//    desPut = (Put)Writables.getWritable(sb, new Put());
//    long stop = System.nanoTime();
//    System.out.println("timer " +(stop-start));

    assertTrue(Bytes.equals(put.getRow(), desPut.getRow()));
    List<KeyValue> list = null;
    List<KeyValue> desList = null;
    for(Map.Entry<byte[], List<KeyValue>> entry : put.getFamilyMap().entrySet()){
      assertTrue(desPut.getFamilyMap().containsKey(entry.getKey()));
      list = entry.getValue();
      desList = desPut.getFamilyMap().get(entry.getKey());
      for(int i=0; i<list.size(); i++){
        assertTrue(list.get(i).equals(desList.get(i)));
      }
    }
  }


  @Test public void testPut2() throws Exception{
    byte[] row = "testAbort,,1243116656250".getBytes();
    byte[] fam = "historian".getBytes();
    byte[] qf1 = "creation".getBytes();

    long ts = 9223372036854775807L;
    byte[] val = "dont-care".getBytes();

    Put put = new Put(row);
    put.add(fam, qf1, ts, val);

    byte[] sb = Writables.getBytes(put);
    Put desPut = (Put)Writables.getWritable(sb, new Put());

    assertTrue(Bytes.equals(put.getRow(), desPut.getRow()));
    List<KeyValue> list = null;
    List<KeyValue> desList = null;
    for(Map.Entry<byte[], List<KeyValue>> entry : put.getFamilyMap().entrySet()){
      assertTrue(desPut.getFamilyMap().containsKey(entry.getKey()));
      list = entry.getValue();
      desList = desPut.getFamilyMap().get(entry.getKey());
      for(int i=0; i<list.size(); i++){
        assertTrue(list.get(i).equals(desList.get(i)));
      }
    }
  }


  @Test public void testDelete() throws Exception{
    byte[] row = "row".getBytes();
    byte[] fam = "fam".getBytes();
    byte[] qf1 = "qf1".getBytes();

    long ts = System.currentTimeMillis();

    Delete delete = new Delete(row);
    delete.deleteColumn(fam, qf1, ts);

    byte[] sb = Writables.getBytes(delete);
    Delete desDelete = (Delete)Writables.getWritable(sb, new Delete());

    assertTrue(Bytes.equals(delete.getRow(), desDelete.getRow()));
    List<KeyValue> list = null;
    List<KeyValue> desList = null;
    for(Map.Entry<byte[], List<KeyValue>> entry :
        delete.getFamilyMap().entrySet()){
      assertTrue(desDelete.getFamilyMap().containsKey(entry.getKey()));
      list = entry.getValue();
      desList = desDelete.getFamilyMap().get(entry.getKey());
      for(int i=0; i<list.size(); i++){
        assertTrue(list.get(i).equals(desList.get(i)));
      }
    }
  }
  */

  @Test public void testGet() throws Exception{
    byte[] row = Bytes.toBytes("row");
    byte[] fam = Bytes.toBytes("fam");
    byte[] qf1 = Bytes.toBytes("qf1");

    long ts = System.currentTimeMillis();
    int maxVersions = 2;

    Get get = new Get(row);
    get.addColumn(fam, qf1);
    get.setTimeRange(ts, ts+1);
    get.readVersions(maxVersions);

    ClientProtos.Get getProto = ProtobufUtil.toGet(get);
    Get desGet = ProtobufUtil.toGet(getProto);

    assertTrue(Bytes.equals(get.getRow(), desGet.getRow()));
    Set<byte[]> set = null;
    Set<byte[]> desSet = null;

    for(Map.Entry<byte[], NavigableSet<byte[]>> entry :
        get.getFamilyMap().entrySet()){
      assertTrue(desGet.getFamilyMap().containsKey(entry.getKey()));
      set = entry.getValue();
      desSet = desGet.getFamilyMap().get(entry.getKey());
      for(byte [] qualifier : set){
        assertTrue(desSet.contains(qualifier));
      }
    }

    assertEquals(get.getMaxVersions(), desGet.getMaxVersions());
    TimeRange tr = get.getTimeRange();
    TimeRange desTr = desGet.getTimeRange();
    assertEquals(tr.getMax(), desTr.getMax());
    assertEquals(tr.getMin(), desTr.getMin());
  }


  @Test public void testScan() throws Exception {

    byte[] startRow = Bytes.toBytes("startRow");
    byte[] stopRow  = Bytes.toBytes("stopRow");
    byte[] fam = Bytes.toBytes("fam");
    byte[] qf1 = Bytes.toBytes("qf1");

    long ts = System.currentTimeMillis();
    int maxVersions = 2;

    Scan scan = new Scan().withStartRow(startRow).withStopRow(stopRow);
    scan.addColumn(fam, qf1);
    scan.setTimeRange(ts, ts+1);
    scan.readVersions(maxVersions);

    ClientProtos.Scan scanProto = ProtobufUtil.toScan(scan);
    Scan desScan = ProtobufUtil.toScan(scanProto);

    assertTrue(Bytes.equals(scan.getStartRow(), desScan.getStartRow()));
    assertTrue(Bytes.equals(scan.getStopRow(), desScan.getStopRow()));
    assertEquals(scan.getCacheBlocks(), desScan.getCacheBlocks());
    Set<byte[]> set = null;
    Set<byte[]> desSet = null;

    for(Map.Entry<byte[], NavigableSet<byte[]>> entry :
        scan.getFamilyMap().entrySet()){
      assertTrue(desScan.getFamilyMap().containsKey(entry.getKey()));
      set = entry.getValue();
      desSet = desScan.getFamilyMap().get(entry.getKey());
      for(byte[] column : set){
        assertTrue(desSet.contains(column));
      }

      // Test filters are serialized properly.
      scan = new Scan().withStartRow(startRow);
      final String name = "testScan";
      byte [] prefix = Bytes.toBytes(name);
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

  /*
   * TODO
  @Test public void testResultEmpty() throws Exception {
    List<KeyValue> keys = new ArrayList<KeyValue>();
    Result r = Result.newResult(keys);
    assertTrue(r.isEmpty());
    byte [] rb = Writables.getBytes(r);
    Result deserializedR = (Result)Writables.getWritable(rb, new Result());
    assertTrue(deserializedR.isEmpty());
  }


  @Test public void testResult() throws Exception {
    byte [] rowA = Bytes.toBytes("rowA");
    byte [] famA = Bytes.toBytes("famA");
    byte [] qfA = Bytes.toBytes("qfA");
    byte [] valueA = Bytes.toBytes("valueA");

    byte [] rowB = Bytes.toBytes("rowB");
    byte [] famB = Bytes.toBytes("famB");
    byte [] qfB = Bytes.toBytes("qfB");
    byte [] valueB = Bytes.toBytes("valueB");

    KeyValue kvA = new KeyValue(rowA, famA, qfA, valueA);
    KeyValue kvB = new KeyValue(rowB, famB, qfB, valueB);

    Result result = Result.newResult(new KeyValue[]{kvA, kvB});

    byte [] rb = Writables.getBytes(result);
    Result deResult = (Result)Writables.getWritable(rb, new Result());

    assertTrue("results are not equivalent, first key mismatch",
        result.raw()[0].equals(deResult.raw()[0]));

    assertTrue("results are not equivalent, second key mismatch",
        result.raw()[1].equals(deResult.raw()[1]));

    // Test empty Result
    Result r = new Result();
    byte [] b = Writables.getBytes(r);
    Result deserialized = (Result)Writables.getWritable(b, new Result());
    assertEquals(r.size(), deserialized.size());
  }

  @Test public void testResultDynamicBuild() throws Exception {
    byte [] rowA = Bytes.toBytes("rowA");
    byte [] famA = Bytes.toBytes("famA");
    byte [] qfA = Bytes.toBytes("qfA");
    byte [] valueA = Bytes.toBytes("valueA");

    byte [] rowB = Bytes.toBytes("rowB");
    byte [] famB = Bytes.toBytes("famB");
    byte [] qfB = Bytes.toBytes("qfB");
    byte [] valueB = Bytes.toBytes("valueB");

    KeyValue kvA = new KeyValue(rowA, famA, qfA, valueA);
    KeyValue kvB = new KeyValue(rowB, famB, qfB, valueB);

    Result result = Result.newResult(new KeyValue[]{kvA, kvB});

    byte [] rb = Writables.getBytes(result);


    // Call getRow() first
    Result deResult = (Result)Writables.getWritable(rb, new Result());
    byte [] row = deResult.getRow();
    assertTrue(Bytes.equals(row, rowA));

    // Call sorted() first
    deResult = (Result)Writables.getWritable(rb, new Result());
    assertTrue("results are not equivalent, first key mismatch",
        result.raw()[0].equals(deResult.raw()[0]));
    assertTrue("results are not equivalent, second key mismatch",
        result.raw()[1].equals(deResult.raw()[1]));

    // Call raw() first
    deResult = (Result)Writables.getWritable(rb, new Result());
    assertTrue("results are not equivalent, first key mismatch",
        result.raw()[0].equals(deResult.raw()[0]));
    assertTrue("results are not equivalent, second key mismatch",
        result.raw()[1].equals(deResult.raw()[1]));


  }

  @Test public void testResultArray() throws Exception {
    byte [] rowA = Bytes.toBytes("rowA");
    byte [] famA = Bytes.toBytes("famA");
    byte [] qfA = Bytes.toBytes("qfA");
    byte [] valueA = Bytes.toBytes("valueA");

    byte [] rowB = Bytes.toBytes("rowB");
    byte [] famB = Bytes.toBytes("famB");
    byte [] qfB = Bytes.toBytes("qfB");
    byte [] valueB = Bytes.toBytes("valueB");

    KeyValue kvA = new KeyValue(rowA, famA, qfA, valueA);
    KeyValue kvB = new KeyValue(rowB, famB, qfB, valueB);


    Result result1 = Result.newResult(new KeyValue[]{kvA, kvB});
    Result result2 = Result.newResult(new KeyValue[]{kvB});
    Result result3 = Result.newResult(new KeyValue[]{kvB});

    Result [] results = new Result [] {result1, result2, result3};

    ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(byteStream);
    Result.writeArray(out, results);

    byte [] rb = byteStream.toByteArray();

    DataInputBuffer in = new DataInputBuffer();
    in.reset(rb, 0, rb.length);

    Result [] deResults = Result.readArray(in);

    assertTrue(results.length == deResults.length);

    for(int i=0;i<results.length;i++) {
      KeyValue [] keysA = results[i].raw();
      KeyValue [] keysB = deResults[i].raw();
      assertTrue(keysA.length == keysB.length);
      for(int j=0;j<keysA.length;j++) {
        assertTrue("Expected equivalent keys but found:\n" +
            "KeyA : " + keysA[j].toString() + "\n" +
            "KeyB : " + keysB[j].toString() + "\n" +
            keysA.length + " total keys, " + i + "th so far"
            ,keysA[j].equals(keysB[j]));
      }
    }

  }

  @Test public void testResultArrayEmpty() throws Exception {
    List<KeyValue> keys = new ArrayList<KeyValue>();
    Result r = Result.newResult(keys);
    Result [] results = new Result [] {r};

    ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(byteStream);

    Result.writeArray(out, results);

    results = null;

    byteStream = new ByteArrayOutputStream();
    out = new DataOutputStream(byteStream);
    Result.writeArray(out, results);

    byte [] rb = byteStream.toByteArray();

    DataInputBuffer in = new DataInputBuffer();
    in.reset(rb, 0, rb.length);

    Result [] deResults = Result.readArray(in);

    assertTrue(deResults.length == 0);

    results = new Result[0];

    byteStream = new ByteArrayOutputStream();
    out = new DataOutputStream(byteStream);
    Result.writeArray(out, results);

    rb = byteStream.toByteArray();

    in = new DataInputBuffer();
    in.reset(rb, 0, rb.length);

    deResults = Result.readArray(in);

    assertTrue(deResults.length == 0);

  }
  */

  protected static final int MAXVERSIONS = 3;
  protected final static byte [] fam1 = Bytes.toBytes("colfamily1");
  protected final static byte [] fam2 = Bytes.toBytes("colfamily2");
  protected final static byte [] fam3 = Bytes.toBytes("colfamily3");
  protected static final byte [][] COLUMNS = {fam1, fam2, fam3};

  /**
   * Create a table of name <code>name</code> with {@link #COLUMNS} for
   * families.
   * @param name Name to give table.
   * @return Column descriptor.
   */
  protected HTableDescriptor createTableDescriptor(final String name) {
    return createTableDescriptor(name, MAXVERSIONS);
  }

  /**
   * Create a table of name <code>name</code> with {@link #COLUMNS} for
   * families.
   * @param name Name to give table.
   * @param versions How many versions to allow per column.
   * @return Column descriptor.
   */
  protected HTableDescriptor createTableDescriptor(final String name,
      final int versions) {
    TableDescriptorBuilder.ModifyableTableDescriptor tableDescriptor =
      new TableDescriptorBuilder.ModifyableTableDescriptor(TableName.valueOf(name));
    tableDescriptor.setColumnFamily(
      new ColumnFamilyDescriptorBuilder.ModifyableColumnFamilyDescriptor(fam1)
        .setMaxVersions(versions)
        .setBlockCacheEnabled(false)
    );
    tableDescriptor.setColumnFamily(
      new ColumnFamilyDescriptorBuilder.ModifyableColumnFamilyDescriptor(fam2)
        .setMaxVersions(versions)
        .setBlockCacheEnabled(false)
    );
    tableDescriptor.setColumnFamily(
      new ColumnFamilyDescriptorBuilder.ModifyableColumnFamilyDescriptor(fam3)
        .setMaxVersions(versions)
        .setBlockCacheEnabled(false)
    );
    return new HTableDescriptor(tableDescriptor);
  }
}
