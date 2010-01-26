/*
 * Copyright 2010 The Apache Software Foundation
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

import junit.framework.Assert;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestCase;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.idx.IdxColumnDescriptor;
import org.apache.hadoop.hbase.client.idx.IdxIndexDescriptor;
import org.apache.hadoop.hbase.client.idx.IdxQualifierType;
import org.apache.hadoop.hbase.client.idx.IdxScan;
import org.apache.hadoop.hbase.client.idx.exp.And;
import org.apache.hadoop.hbase.client.idx.exp.Comparison;
import org.apache.hadoop.hbase.client.idx.exp.Or;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import javax.management.Attribute;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import javax.management.MBeanOperationInfo;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;


/**
 * Tests the indexed region implemention.
 */
public class TestIdxRegion extends HBaseTestCase {
  static final Log LOG = LogFactory.getLog(TestIdxRegion.class);


  IdxRegion region = null;
  private final String DIR = "test/build/data/TestIdxRegion/";

  private final byte[] qualLong = Bytes.toBytes("qualLong");
  private final byte[] qualBytes = Bytes.toBytes("qualBytes");
  private final byte[] qualDouble = Bytes.toBytes("qualDouble");

  private void initIdxRegion(byte[] tableName, String callingMethod,
    HBaseConfiguration conf, Pair<byte[],
      IdxIndexDescriptor[]>... families)
    throws Exception {
    HTableDescriptor htd = new HTableDescriptor(tableName);
    for (Pair<byte[], IdxIndexDescriptor[]> familyPair : families) {
      IdxColumnDescriptor idxColumnDescriptor
        = new IdxColumnDescriptor(familyPair.getFirst());
      if (familyPair.getSecond() != null) {
        for (IdxIndexDescriptor descriptor : familyPair.getSecond())
          idxColumnDescriptor.addIndexDescriptor(descriptor);
      }
      htd.addFamily(idxColumnDescriptor);
    }
    HRegionInfo info = new HRegionInfo(htd, null, null, false);
    Path path = new Path(DIR + callingMethod);
    region = createIdxRegion(info, path, conf);
    verifyMBean(region);
  }

  @Override
  protected void tearDown() throws Exception {
    if (region != null) {
      verifyMBean(region);
    }
    super.tearDown();
  }

  /**
   * Convenience method creating new HRegions. Used by createTable and by the
   * bootstrap code in the HMaster constructor.
   * Note, this method creates an {@link HLog} for the created region. It
   * needs to be closed explicitly.  Use {@link HRegion#getLog()} to get
   * access.
   *
   * @param info    Info for region to create.
   * @param rootDir Root directory for HBase instance
   * @param conf
   * @return new HRegion
   * @throws IOException
   */
  public static IdxRegion createIdxRegion(final HRegionInfo info,
    final Path rootDir, final HBaseConfiguration conf) throws IOException {
    conf.setClass(HConstants.REGION_IMPL, IdxRegion.class, HRegion.class);
    Path tableDir =
      HTableDescriptor.getTableDir(rootDir, info.getTableDesc().getName());
    Path regionDir = HRegion.getRegionDir(tableDir, info.getEncodedName());
    FileSystem fs = FileSystem.get(conf);
    fs.mkdirs(regionDir);
    IdxRegion region = new IdxRegion(tableDir,
      new HLog(fs, new Path(regionDir, HRegion.HREGION_LOGDIR_NAME),
        conf, null),
      fs, conf, info, null);
    region.initialize(null, null);
    return region;
  }

  /**
   * Tests that the start row takes effect when scanning with an index.
   *
   * @throws IOException exception
   */
  public void testIndexedScanWithStartRow() throws Exception {
    byte[] tableName = Bytes.toBytes("testIndexedScanWithStartRow");
    byte[] family = Bytes.toBytes("family");
    IdxIndexDescriptor indexDescriptor
      = new IdxIndexDescriptor(qualLong, IdxQualifierType.LONG);

    // Setting up region
    String method = "testIndexedScanWithStartRow";
    initIdxRegion(tableName, method, new HBaseConfiguration(),
      Pair.of(family, new IdxIndexDescriptor[]{indexDescriptor}));

    for (long i = 1; i <= 100; i++) {
      byte[] row = Bytes.toBytes(i);
      Put put = new Put(row);
      put.add(family, qualLong, Bytes.toBytes(i));
      region.put(put);
    }

    region.flushcache();

    byte[] startRow = Bytes.toBytes(75L);

    IdxScan idxScan = new IdxScan();
    idxScan.addFamily(family);
    idxScan.setStartRow(startRow);
    idxScan.setExpression(Comparison.comparison(family, qualLong,
      Comparison.Operator.GTE, Bytes.toBytes(50L)));
    InternalScanner scanner = region.getScanner(idxScan);

    List<KeyValue> res = new ArrayList<KeyValue>();

    int counter = 0;
    while (scanner.next(res)) {
      Assert.assertTrue("The startRow doesn't seem to be working",
        region.comparator.compareRows(res.get(0), startRow) >= 0);
      counter++;
    }

    assertEquals(25, counter);
  }

  /**
   * A minimal tests which adds one row to a table with one family and one
   * qualifier and checks that it functions as expected.
   *
   * @throws IOException exception
   */
  public void testIndexedScanWithOneRow() throws Exception {
    byte[] tableName = Bytes.toBytes("testIndexedScanWithOneRow");
    byte[] family = Bytes.toBytes("family");
    IdxIndexDescriptor indexDescriptor
      = new IdxIndexDescriptor(qualLong, IdxQualifierType.LONG);
    byte[] row1 = Bytes.toBytes("row1");

    //Setting up region
    String method = "testIndexedScanWithOneRow";
    initIdxRegion(tableName, method, new HBaseConfiguration(),
      Pair.of(family, new IdxIndexDescriptor[]{indexDescriptor}));

    Put put = new Put(row1);
    put.add(family, qualLong, Bytes.toBytes(42L));
    region.put(put);

    checkScanWithOneRow(family, false);

    region.flushcache();

    checkScanWithOneRow(family, true);
  }

  private void checkScanWithOneRow(byte[] family, boolean memStoreEmpty)
    throws IOException {
    /**
     * Scan without the index
     */
    Scan scan = new Scan();
    scan.addFamily(family);
    InternalScanner scanner = region.getScanner(scan);
    List<KeyValue> res = new ArrayList<KeyValue>();

    while (scanner.next(res)) ;
    assertEquals(1, res.size());
    assertEquals(Bytes.toLong(res.get(0).getValue()), 42L);

    /**
     * Scan the index with a matching expression
     */
    IdxScan idxScan = new IdxScan();
    idxScan.addFamily(family);
    idxScan.setExpression(Comparison.comparison(family, qualLong,
      Comparison.Operator.EQ, Bytes.toBytes(42L)));
    scanner = region.getScanner(idxScan);
    res.clear();

    while (scanner.next(res)) ;
    assertEquals(1, res.size());
    assertEquals(Bytes.toLong(res.get(0).getValue()), 42L);

    /**
     * Scan the index with a non matching expression
     */
    idxScan = new IdxScan();
    idxScan.addFamily(family);
    idxScan.setExpression(Comparison.comparison(family, qualLong,
      Comparison.Operator.EQ, Bytes.toBytes(24L)));
    if (!memStoreEmpty) {
      idxScan.setFilter(new ValueFilter(CompareFilter.CompareOp.EQUAL,
        new BinaryComparator(Bytes.toBytes(24L))));
    }
    scanner = region.getScanner(idxScan);
    res.clear();

    while (scanner.next(res)) ;
    assertEquals(0, res.size());
  }

  public void testIndexedScanWithOneIndexAndOneColumn() throws Exception {
    byte[] tableName = Bytes.toBytes("testIndexedScanWithOneIndexAndOneColumn");
    byte[] family = Bytes.toBytes("family");
    IdxIndexDescriptor indexDescriptor = new IdxIndexDescriptor(qualLong,
      IdxQualifierType.LONG);
    int numRows = 10000;

    Random random = new Random(1431974L);  // pseudo random order of row insertions

    //Setting up region
    String method = "testIndexedScanWithOneRow";
    initIdxRegion(tableName, method, new HBaseConfiguration(), Pair.of(family,
      new IdxIndexDescriptor[]{indexDescriptor}));
    for (long i = 0; i < numRows; i++) {
      Put put = new Put(Bytes.toBytes(random.nextLong() + "." + i));
      put.add(family, qualLong, Bytes.toBytes(i));
      region.put(put);
    }

    /**
     * Check when indexes are empty and memstore is full
     */
    checkScanWithOneIndexAndOneColumn(family, false, numRows, 1);

    region.flushcache();

    /**
     * Check when indexes are full and memstore is empty
     */
    checkScanWithOneIndexAndOneColumn(family, true, numRows, 1);


    for (long i = numRows; i < numRows + 1000; i++) {
      Put put = new Put(Bytes.toBytes(random.nextLong() + "." + i));
      put.add(family, qualLong, Bytes.toBytes(i));
      region.put(put);
    }

    /**
     * check when both the index and the memstore contain entries
     */
    checkScanWithOneIndexAndOneColumn(family, false, numRows + 1000, 1);
  }

  private void checkScanWithOneIndexAndOneColumn(byte[] family,
    boolean memStoreEmpty, int numRows, int numColumns) throws IOException {
    /**
     * Scan without the index for everything
     */
    Scan scan = new Scan();
    scan.addFamily(family);
    InternalScanner scanner = region.getScanner(scan);
    List<KeyValue> res = new ArrayList<KeyValue>();

    while (scanner.next(res)) ;
    assertEquals(numRows * numColumns, res.size());
    res.clear();

    /**
     * Scan without the index for one
     */
    scan = new Scan();
    scan.addFamily(family);
    scan.setFilter(new SingleColumnValueFilter(family, qualLong,
      CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes(42L))));
    scanner = region.getScanner(scan);
    res.clear();

    while (scanner.next(res)) ;
    assertEquals(numColumns, res.size());
    for (KeyValue kv : res) {
      if (Bytes.equals(kv.getQualifier(), qualLong)) {
        assertEquals(42L, Bytes.toLong(kv.getValue()));
      }
    }


    /**
     * Scan the index with a matching expression
     */
    IdxScan idxScan = new IdxScan();
    idxScan.addFamily(family);
    idxScan.setExpression(Comparison.comparison(family, qualLong,
      Comparison.Operator.EQ, Bytes.toBytes(42L)));
    if (!memStoreEmpty) {
      idxScan.setFilter(new SingleColumnValueFilter(family, qualLong,
        CompareFilter.CompareOp.EQUAL,
        new BinaryComparator(Bytes.toBytes(42L))));
    }
    scanner = region.getScanner(idxScan);
    res.clear();

    //long start = System.nanoTime();
    while (scanner.next(res)) ;
    //long end = System.nanoTime();
    //System.out.println("memStoreEmpty=" + memStoreEmpty + ", time=" + (end - start) / 1000000D);
    assertEquals(numColumns, res.size());
    for (KeyValue kv : res) {
      if (Bytes.equals(kv.getQualifier(), qualLong)) {
        assertEquals(42L, Bytes.toLong(kv.getValue()));
      }
    }

    /**
     * Scan the index with a non matching expression
     */
    idxScan = new IdxScan();
    idxScan.addFamily(family);
    idxScan.setExpression(Comparison.comparison(family, qualLong,
      Comparison.Operator.EQ, Bytes.toBytes(1000000000L)));
    if (!memStoreEmpty) {
      idxScan.setFilter(new SingleColumnValueFilter(family, qualLong,
        CompareFilter.CompareOp.EQUAL,
        new BinaryComparator(Bytes.toBytes(1000000000L))));
    }
    scanner = region.getScanner(idxScan);
    res.clear();

    while (scanner.next(res)) ;
    assertEquals(0, res.size());


    /**
     * Scan for all the records which are greater than 49499
     */
    idxScan = new IdxScan();
    idxScan.addFamily(family);
    long min = numRows - 500L;
    idxScan.setExpression(Comparison.comparison(family, qualLong,
      Comparison.Operator.GTE, Bytes.toBytes(min)));
    if (!memStoreEmpty) {
      idxScan.setFilter(new SingleColumnValueFilter(family, qualLong,
        CompareFilter.CompareOp.GREATER_OR_EQUAL,
        new BinaryComparator(Bytes.toBytes(min))));
    }
    scanner = region.getScanner(idxScan);
    res.clear();

    //long start = System.nanoTime();
    while (scanner.next(res)) ;
    //long end = System.nanoTime();
    //System.out.println("scan for val >= min memStoreEmpty=" + memStoreEmpty + ", time=" + (end - start)/1000000D);
    assertEquals(500 * numColumns, res.size());

    /**
     * Scan for all the records which are greater than 49499
     */
    idxScan = new IdxScan();
    idxScan.addFamily(family);
    min = numRows / 2;
    long delta = 100;
    long max = min + delta;
    idxScan.setExpression(
      And.and(Comparison.comparison(family, qualLong, Comparison.Operator.GTE, Bytes.toBytes(min)),
        Comparison.comparison(family, qualLong, Comparison.Operator.LT, Bytes.toBytes(max))));
    if (!memStoreEmpty) {
      idxScan.setFilter(new FilterList(Arrays.<Filter>asList(
        new SingleColumnValueFilter(family, qualLong, CompareFilter.CompareOp.GREATER_OR_EQUAL,
          new BinaryComparator(Bytes.toBytes(min))),
        new SingleColumnValueFilter(family, qualLong, CompareFilter.CompareOp.LESS,
          new BinaryComparator(Bytes.toBytes(max))))
      ));
    }
    scanner = region.getScanner(idxScan);
    res.clear();

    //long start = System.nanoTime();
    while (scanner.next(res)) ;
    //long end = System.nanoTime();
    //System.out.println("scan for min <= val < max memStoreEmpty=" + memStoreEmpty + ", time=" + (end - start) / 1000000D);
    assertEquals(delta * numColumns, res.size());
  }


  public void testIndexedScanWithThreeColumns() throws Exception {
    byte[] tableName = Bytes.toBytes("testIndexedScanWithThreeColumns");
    byte[] family = Bytes.toBytes("family");
    IdxIndexDescriptor indexDescriptor1 = new IdxIndexDescriptor(qualLong, IdxQualifierType.LONG);
    IdxIndexDescriptor indexDescriptor2 = new IdxIndexDescriptor(qualDouble, IdxQualifierType.DOUBLE);
    IdxIndexDescriptor indexDescriptor3 = new IdxIndexDescriptor(qualBytes, IdxQualifierType.BYTE_ARRAY);
    int numRows = 10000;

    Random random = new Random(24122008L);  // pseudo random order of row insertions

    //Setting up region
    String method = "testIndexedScanWithThreeColumns";
    initIdxRegion(tableName, method, new HBaseConfiguration(), Pair.of(family,
      new IdxIndexDescriptor[]{indexDescriptor1, indexDescriptor2, indexDescriptor3}));
    for (long i = 0; i < numRows; i++) {
      Put put = new Put(Bytes.toBytes(random.nextLong() + "." + i));
      put.add(family, qualLong, Bytes.toBytes(i));
      put.add(family, qualDouble, Bytes.toBytes((double) i));
      put.add(family, qualBytes, Bytes.toBytes("str" + (10 + (i % 50))));
      region.put(put);
    }

    /**
     * Check when indexes are empty and memstore is full
     */
    checkScanWithOneIndexAndOneColumn(family, false, numRows, 3);
    checkScanWithThreeColumns(family, false, numRows, 3);

    region.flushcache();

    /**
     * Check when indexes are full and memstore is empty
     */
    checkScanWithOneIndexAndOneColumn(family, true, numRows, 3);
    checkScanWithThreeColumns(family, true, numRows, 3);


    int numAdditionalRows = 1000;
    for (long i = numRows; i < numRows + numAdditionalRows; i++) {
      Put put = new Put(Bytes.toBytes(random.nextLong() + "." + i));
      put.add(family, qualLong, Bytes.toBytes(i));
      put.add(family, qualDouble, Bytes.toBytes((double) i));
      put.add(family, qualBytes, Bytes.toBytes("str" + (10 + (i % 50))));
      region.put(put);
    }

    /**
     * check when both the index and the memstore contain entries
     */
    checkScanWithOneIndexAndOneColumn(family, false, numRows + numAdditionalRows, 3);
    checkScanWithThreeColumns(family, false, numRows + numAdditionalRows, 3);
  }

  private void checkScanWithThreeColumns(byte[] family, boolean memStoreEmpty, int numRows, int numColumns) throws IOException {

    /**
     * Scan the index with a matching or expression on two indices
     */
    IdxScan idxScan = new IdxScan();
    idxScan.addFamily(family);
    int low = numRows / 10;
    int high = numRows - low;
    idxScan.setExpression(Or.or(Comparison.comparison(family, qualLong, Comparison.Operator.GTE, Bytes.toBytes((long) high)),
      Comparison.comparison(family, qualDouble, Comparison.Operator.LT, Bytes.toBytes((double) low))));
    if (!memStoreEmpty) {
      idxScan.setFilter(new FilterList(FilterList.Operator.MUST_PASS_ONE,
        Arrays.<Filter>asList(
          new SingleColumnValueFilter(family, qualLong, CompareFilter.CompareOp.GREATER_OR_EQUAL,
            new BinaryComparator(Bytes.toBytes((long) high))),
          new SingleColumnValueFilter(family, qualDouble, CompareFilter.CompareOp.LESS,
            new BinaryComparator(Bytes.toBytes((double) low))))
      ));
    }
    InternalScanner scanner = region.getScanner(idxScan);
    List<KeyValue> res = new ArrayList<KeyValue>();

    //long start = System.nanoTime();
    while (scanner.next(res)) ;
    //long end = System.nanoTime();
    //System.out.println("[top and botoom 10%] memStoreEmpty=" + memStoreEmpty + ", time=" + (end - start)/1000000D);
    assertEquals(numRows / 5 * numColumns, res.size());

    /**
     * Scan the index with a matching and expression on two indices
     */
    idxScan = new IdxScan();
    idxScan.addFamily(family);
    int half = numRows / 2;
    idxScan.setExpression(And.and(Comparison.comparison(family, qualLong, Comparison.Operator.GTE, Bytes.toBytes((long) half)),
      Comparison.comparison(family, qualBytes, Comparison.Operator.EQ, Bytes.toBytes("str" + 30))));
    if (!memStoreEmpty) {
      idxScan.setFilter(new FilterList(FilterList.Operator.MUST_PASS_ALL,
        Arrays.<Filter>asList(
          new SingleColumnValueFilter(family, qualLong, CompareFilter.CompareOp.GREATER_OR_EQUAL,
            new BinaryComparator(Bytes.toBytes((long) half))),
          new SingleColumnValueFilter(family, qualBytes, CompareFilter.CompareOp.EQUAL,
            new BinaryComparator(Bytes.toBytes("str" + 30))))
      ));
    }
    scanner = region.getScanner(idxScan);
    res.clear();

    //long start = System.nanoTime();
    while (scanner.next(res)) ;
    //long end = System.nanoTime();
    //System.out.println("[top 50% which have a 1/50 string] memStoreEmpty=" + memStoreEmpty + ", time=" + (end - start)/1000000D);
    assertEquals((numRows / 100) * numColumns, res.size());
  }

  /**
   * Verifies that the indexed scan works correctly when scanning with multiple
   * families.
   *
   * @throws Exception throws by delegates
   */
  public void testIndexedScanWithTwoFamilies() throws Exception {
    byte[] tableName = Bytes.toBytes("testIndexedScanWithTwoFamilies");
    byte[] family1 = Bytes.toBytes("family1");
    byte[] family2 = Bytes.toBytes("family2");
    IdxIndexDescriptor indexDescriptor1 = new IdxIndexDescriptor(qualLong, IdxQualifierType.LONG);
    IdxIndexDescriptor indexDescriptor2 = new IdxIndexDescriptor(qualDouble, IdxQualifierType.DOUBLE);
    IdxIndexDescriptor indexDescriptor3 = new IdxIndexDescriptor(qualBytes, IdxQualifierType.BYTE_ARRAY);

    String method = "testIndexedScanWithTwoFamilies";
    initIdxRegion(tableName, method, new HBaseConfiguration(), Pair.of(family1,
      new IdxIndexDescriptor[]{indexDescriptor1, indexDescriptor2}),
      Pair.of(family2, new IdxIndexDescriptor[]{indexDescriptor3}));

    int numberOfRows = 1000;

    Random random = new Random(3505L);
    for (int row = 0; row < numberOfRows; row++) {
      Put put = new Put(Bytes.toBytes(random.nextLong()));
      int val = row % 10;
      put.add(family1, qualLong, Bytes.toBytes((long) val));
      put.add(family1, qualDouble, Bytes.toBytes((double) val));
      put.add(family2, qualBytes, Bytes.toBytes(String.format("%04d", val)));
      region.put(put);
    }

    checkScanWithTwoFamilies(family1, family2, false, numberOfRows, 3);

    region.flushcache();

    checkScanWithTwoFamilies(family1, family2, true, numberOfRows, 3);

    /**
     * Add some more to have results both in the index and in memstore
     */
    for (int row = 0; row < numberOfRows; row++) {
      Put put = new Put(Bytes.toBytes(random.nextLong()));
      int val = row % 10;
      put.add(family1, qualLong, Bytes.toBytes((long) val));
      put.add(family1, qualDouble, Bytes.toBytes((double) val));
      put.add(family2, qualBytes, Bytes.toBytes(String.format("%04d", val)));
      region.put(put);
    }

    checkScanWithTwoFamilies(family1, family2, false, numberOfRows * 2, 3);

  }

  private void checkScanWithTwoFamilies(byte[] family1, byte[] family2,
    boolean memStoreEmpty, int numRows, int numColumns) throws IOException {

    /**
     * Scan the index with a matching or expression on two indices
     */
    IdxScan idxScan = new IdxScan();
    final byte[] longVal = Bytes.toBytes((long) 1);
    final byte[] doubleVal = Bytes.toBytes((double) 4);
    final byte[] bytesVal = Bytes.toBytes(String.format("%04d", 9));
    idxScan.setExpression(
      Or.or(Comparison.comparison(family1, qualLong, Comparison.Operator.EQ, longVal),
        Comparison.comparison(family1, qualDouble, Comparison.Operator.EQ, doubleVal),
        Comparison.comparison(family2, qualBytes, Comparison.Operator.EQ, bytesVal)));
    if (!memStoreEmpty) {
      idxScan.setFilter(new FilterList(FilterList.Operator.MUST_PASS_ONE,
        Arrays.<Filter>asList(
          new SingleColumnValueFilter(family1, qualLong, CompareFilter.CompareOp.EQUAL,
            new BinaryComparator(longVal)),
          new SingleColumnValueFilter(family1, qualDouble, CompareFilter.CompareOp.EQUAL,
            new BinaryComparator(doubleVal)),
          new SingleColumnValueFilter(family2, qualBytes, CompareFilter.CompareOp.EQUAL,
            new BinaryComparator(bytesVal)))
      ));
    }
    InternalScanner scanner = region.getScanner(idxScan);
    List<KeyValue> res = new ArrayList<KeyValue>();
    int actualRows = 0;
    //long start = System.nanoTime();
    while (scanner.next(res)) {
      assertEquals(numColumns, res.size());
      actualRows++;
      res.clear();
    }
    //long end = System.nanoTime();
    //System.out.println("[top and botoom 10%] memStoreEmpty=" + memStoreEmpty + ", time=" + (end - start)/1000000D);
    assertEquals(numRows / 10 * 3, actualRows);
  }


  public void testIndexedScanWithMultipleVersions() throws Exception {
    byte[] tableName = Bytes.toBytes("testIndexedScanWithMultipleVersions");
    byte[] family = Bytes.toBytes("family");
    IdxIndexDescriptor indexDescriptor1 = new IdxIndexDescriptor(qualLong, IdxQualifierType.LONG);
    IdxIndexDescriptor indexDescriptor2 = new IdxIndexDescriptor(qualDouble, IdxQualifierType.DOUBLE);
    int numRows = 10000;

    Random random = new Random(27101973L);  // pseudo random order of row insertions

    //Setting up region
    String method = "testIndexedScanWithMultipleVersions";
    initIdxRegion(tableName, method, new HBaseConfiguration(), Pair.of(family,
      new IdxIndexDescriptor[]{indexDescriptor1, indexDescriptor2}));
    for (long i = 0; i < numRows; i++) {
      Put put = new Put(Bytes.toBytes(random.nextLong() + "." + i));
      long value = i % 10;
      put.add(family, qualLong, Bytes.toBytes(value));
      put.add(family, qualDouble, Bytes.toBytes((double) i));
      region.put(put);
    }

    /**
     * Check when indexes are empty and memstore is full
     */
    checkIndexedScanWithMultipleVersions(family, false, numRows, 1);


    random = new Random(27101973L);  // pseudo random order of row insertions
    for (long i = 0; i < numRows; i++) {
      Put put = new Put(Bytes.toBytes(random.nextLong() + "." + i));
      long value = 10 + i % 10;
      put.add(family, qualLong, Bytes.toBytes(value));
      put.add(family, qualDouble, Bytes.toBytes((double) i));
      region.put(put);
    }

    /**
     * Check when indexes are full and memstore is empty
     */
    checkIndexedScanWithMultipleVersions(family, false, numRows, 2);

    region.flushcache();

    /**
     * Check when indexes are full and memstore is empty
     */
    checkIndexedScanWithMultipleVersions(family, true, numRows, 2);

    random = new Random(27101973L);  // pseudo random order of row insertions
    for (long i = 0; i < numRows; i++) {
      Put put = new Put(Bytes.toBytes(random.nextLong() + "." + i));
      long value = 20 + i % 10;
      put.add(family, qualLong, Bytes.toBytes(value));
      put.add(family, qualDouble, Bytes.toBytes((double) i));
      region.put(put);
    }

    checkIndexedScanWithMultipleVersions(family, false, numRows, 3);
    region.flushcache();

    /**
     * Check when indexes are full and memstore is empty
     */
    checkIndexedScanWithMultipleVersions(family, true, numRows, 3);
  }

  private void checkIndexedScanWithMultipleVersions(byte[] family, boolean memStoreEmpty, int numRows, int numVersions) throws IOException {

    /**
     * Scan the index with a matching or expression on two indices
     */
    IdxScan idxScan = new IdxScan();
    idxScan.addFamily(family);

    int[] values = new int[numVersions];
    for (int i = 0; i < numVersions; i++) {
      values[i] = 10 * i + 7;
    }

    /**
     * Scan all the pervious versions - expect zero results.
     */
    for (int i = 0; i < numVersions - 1; i++) {
      checkVersionedScan(family, memStoreEmpty, idxScan, (long) values[i], 0);
    }

    checkVersionedScan(family, memStoreEmpty, idxScan, (long) values[numVersions - 1], numRows / 5);
  }

  private void checkVersionedScan(byte[] family, boolean memStoreEmpty, IdxScan idxScan, long value, int exepctedNumberOfResults) throws IOException {
    idxScan.setExpression(Comparison.comparison(family, qualLong, Comparison.Operator.EQ, Bytes.toBytes(value)));
    if (!memStoreEmpty) {
      idxScan.setFilter(new SingleColumnValueFilter(family, qualLong, CompareFilter.CompareOp.EQUAL,
        new BinaryComparator(Bytes.toBytes(value))));
    }
    InternalScanner scanner = region.getScanner(idxScan);
    List<KeyValue> res = new ArrayList<KeyValue>();

    //long start = System.nanoTime();
    while (scanner.next(res)) ;
    //long end = System.nanoTime();
    //System.out.println("memStoreEmpty=" + memStoreEmpty + ", time=" + (end - start)/1000000D);
    assertEquals(exepctedNumberOfResults, res.size());
  }

  public void testIndexedScanWithDeletedRows() throws Exception {
    byte[] tableName = Bytes.toBytes("testIndexedScanWithDeletedRows");
    byte[] family = Bytes.toBytes("family");
    IdxIndexDescriptor indexDescriptor1
      = new IdxIndexDescriptor(qualLong, IdxQualifierType.LONG);
    IdxIndexDescriptor indexDescriptor2
      = new IdxIndexDescriptor(qualDouble, IdxQualifierType.DOUBLE);
    int numRows = 10000;

    Random random = new Random(10121986L);  // pseudo random order of row insertions

    long timestamp = 0;

    //Setting up region
    String method = "testIndexedScanWithDeletedRows";
    initIdxRegion(tableName, method, new HBaseConfiguration(), Pair.of(family,
      new IdxIndexDescriptor[]{indexDescriptor1, indexDescriptor2}));
    for (long i = 0; i < numRows; i++) {
      Put put = new Put(Bytes.toBytes(random.nextLong() + "." + i));
      long value = i % 10;
      put.add(family, qualLong, timestamp, Bytes.toBytes(value));
      put.add(family, qualDouble, timestamp, Bytes.toBytes((double) i));
      region.put(put);
    }

    checkIndexedScanWithDeletedRows(family, false, 7L, numRows / 5);
    checkIndexedScanWithDeletedRows(family, false, 6L, 8L, 3 * numRows / 5);

    timestamp++;

    // delete some rows
    random = new Random(10121986L);  // pseudo random order of row insertions
    for (long i = 0; i < numRows; i++) {
      byte[] rowId = Bytes.toBytes(random.nextLong() + "." + i);
      if (i % 10 == 7) {
        Delete delete = new Delete(rowId, timestamp, null);
        region.delete(delete, null, true);
      }
    }

    checkIndexedScanWithDeletedRows(family, false, 7L, 0);
    checkIndexedScanWithDeletedRows(family, false, 6L, 8L, 2 * numRows / 5);

    /**
     * Flush and verify
     */
    region.flushcache();

    checkIndexedScanWithDeletedRows(family, true, 7L, 0);
    checkIndexedScanWithDeletedRows(family, true, 6L, 8L, 2 * numRows / 5);

    /**
     * New check - now the index should find the 4's  and the memstore
     * should only contains deleted rows - override the index's findings
     */
    checkIndexedScanWithDeletedRows(family, true, 4L, numRows / 5);
    checkIndexedScanWithDeletedRows(family, true, 3L, 8L, numRows);

    timestamp++;

    random = new Random(10121986L);  // pseudo random order of row insertions
    for (long i = 0; i < numRows; i++) {
      byte[] rowId = Bytes.toBytes(random.nextLong() + "." + i);
      if (i % 10 == 4) {
        Delete delete = new Delete(rowId, timestamp, null);
        region.delete(delete, null, true);
      }
    }

    checkIndexedScanWithDeletedRows(family, false, 4L, 0);
    checkIndexedScanWithDeletedRows(family, false, 3L, 8L, 4 * numRows / 5);

    region.flushcache();
    checkIndexedScanWithDeletedRows(family, true, 4L, 0);
    checkIndexedScanWithDeletedRows(family, true, 3L, 8L, 4 * numRows / 5);

    timestamp++;
    /**
     * New check - put some records back and verify
     */
    for (long i = 0; i < numRows / 10; i++) {
      Put put = new Put(Bytes.toBytes(random.nextLong() + "." + i));
      long value = 7L;
      put.add(family, qualLong, timestamp, Bytes.toBytes(value));
      put.add(family, qualDouble, timestamp, Bytes.toBytes((double) i));
      region.put(put);
    }

    checkIndexedScanWithDeletedRows(family, false, 7L, numRows / 5);
    checkIndexedScanWithDeletedRows(family, false, 4L, 0);
    checkIndexedScanWithDeletedRows(family, false, 3L, 8L, numRows);

    region.flushcache();
    checkIndexedScanWithDeletedRows(family, true, 7L, numRows / 5);
    checkIndexedScanWithDeletedRows(family, true, 4L, 0);
    checkIndexedScanWithDeletedRows(family, true, 3L, 8L, numRows);


  }

  private void checkIndexedScanWithDeletedRows(byte[] family,
    boolean memStoreEmpty, long value, int expectedCount) throws IOException {

    /**
     * Scan the index with a matching or expression on two indices
     */
    IdxScan idxScan = new IdxScan();
    idxScan.addFamily(family);
    idxScan.setExpression(Comparison.comparison(family, qualLong,
      Comparison.Operator.EQ, Bytes.toBytes(value)));
    if (!memStoreEmpty) {
      idxScan.setFilter(new SingleColumnValueFilter(family, qualLong,
        CompareFilter.CompareOp.EQUAL,
        new BinaryComparator(Bytes.toBytes(value))));
    }
    InternalScanner scanner = region.getScanner(idxScan);
    List<KeyValue> res = new ArrayList<KeyValue>();

    //long start = System.nanoTime();
    while (scanner.next(res)) ;
    //long end = System.nanoTime();
    //System.out.println("memStoreEmpty=" + memStoreEmpty + ", time=" + (end - start)/1000000D);
    assertEquals(expectedCount, res.size());
  }

  private void checkIndexedScanWithDeletedRows(byte[] family,
    boolean memStoreEmpty, long minValue, long maxValue, int expectedCount)
    throws IOException {

    /**
     * Scan the index with a matching or expression on two indices
     */
    IdxScan idxScan = new IdxScan();
    idxScan.addFamily(family);
    idxScan.setExpression(
      Comparison.and(Comparison.comparison(family, qualLong,
        Comparison.Operator.GTE, Bytes.toBytes(minValue)),
        Comparison.comparison(family, qualLong,
          Comparison.Operator.LTE, Bytes.toBytes(maxValue))));
    if (!memStoreEmpty) {
      idxScan.setFilter(new FilterList(FilterList.Operator.MUST_PASS_ALL,
        Arrays.<Filter>asList(new SingleColumnValueFilter(family, qualLong,
          CompareFilter.CompareOp.GREATER_OR_EQUAL,
          new BinaryComparator(Bytes.toBytes(minValue))),
          new SingleColumnValueFilter(family, qualLong,
            CompareFilter.CompareOp.LESS_OR_EQUAL,
            new BinaryComparator(Bytes.toBytes(maxValue)))
        )));
    }
    InternalScanner scanner = region.getScanner(idxScan);
    List<KeyValue> res = new ArrayList<KeyValue>();

    //long start = System.nanoTime();
    while (scanner.next(res)) ;
    //long end = System.nanoTime();
    //System.out.println("memStoreEmpty=" + memStoreEmpty + ", time=" + (end - start)/1000000D);
    assertEquals(expectedCount, res.size());
  }


  public void testIdxRegionSplit() throws Exception {
    byte[] tableName = Bytes.toBytes("testIDxRegionSplit");
    byte[] family = Bytes.toBytes("family");
    IdxIndexDescriptor indexDescriptor = new IdxIndexDescriptor(qualLong,
      IdxQualifierType.LONG);
    int numRows = 1000;

    //Setting up region
    String method = "testIdxRegionSplit";
    initIdxRegion(tableName, method, new HBaseConfiguration(), Pair.of(family,
      new IdxIndexDescriptor[]{indexDescriptor}));
    for (long i = 0; i < numRows; i++) {
      Put put = new Put(Bytes.toBytes(String.format("%08d", i)));
      put.add(family, qualLong, Bytes.toBytes(i));
      region.put(put);
    }

    IdxScan idxScan = new IdxScan();
    idxScan.addFamily(family);
    int low = numRows / 10;
    int high = numRows - low;
    idxScan.setExpression(Or.or(Comparison.comparison(family, qualLong, Comparison.Operator.GTE, Bytes.toBytes((long) high)),
      Comparison.comparison(family, qualLong, Comparison.Operator.LT, Bytes.toBytes((long) low))));
    idxScan.setFilter(new FilterList(FilterList.Operator.MUST_PASS_ONE,
      Arrays.<Filter>asList(
        new SingleColumnValueFilter(family, qualLong, CompareFilter.CompareOp.GREATER_OR_EQUAL,
          new BinaryComparator(Bytes.toBytes((long) high))),
        new SingleColumnValueFilter(family, qualLong, CompareFilter.CompareOp.LESS,
          new BinaryComparator(Bytes.toBytes((long) low))))
    ));
    InternalScanner scanner = region.getScanner(idxScan);
    List<KeyValue> res = new ArrayList<KeyValue>();

    while (scanner.next(res)) ;
    Assert.assertEquals(low * 2, res.size());

    HRegion[] split = region.splitRegion(Bytes.toBytes(String.format("%08d", numRows / 2)));
    Assert.assertEquals(IdxRegion.class, split[0].getClass());
    Assert.assertEquals(IdxRegion.class, split[1].getClass());
    Assert.assertTrue(region.isClosed());
    region = null;

    res.clear();
    scanner = openClosedRegion(split[0]).getScanner(idxScan);
    while (scanner.next(res)) ;
    Assert.assertEquals(low, res.size());

    res.clear();
    scanner = openClosedRegion(split[1]).getScanner(idxScan);
    while (scanner.next(res)) ;
    Assert.assertEquals(low, res.size());
  }

  public void testIdxRegionCompaction() throws Exception {
    checkIdxRegionCompaction(true);
    checkIdxRegionCompaction(false);
  }

  private void checkIdxRegionCompaction(boolean majorcompaction)
    throws Exception {
    byte[] tableName = Bytes.toBytes("testIdxRegionCompaction_" + majorcompaction);
    byte[] family = Bytes.toBytes("family");
    IdxIndexDescriptor indexDescriptor = new IdxIndexDescriptor(qualLong,
      IdxQualifierType.LONG);
    int numRows = 1000;
    int flushInterval = numRows / 5 + 1;

    //Setting up region
    String method = "testIdxRegionCompaction_" + majorcompaction;
    initIdxRegion(tableName, method, new HBaseConfiguration(), Pair.of(family,
      new IdxIndexDescriptor[]{indexDescriptor}));
    for (long i = 0; i < numRows; i++) {
      Put put = new Put(Bytes.toBytes(String.format("%08d", i)));
      put.add(family, qualLong, Bytes.toBytes(i));
      region.put(put);
      if (i != 0 && i % flushInterval == 0) {
        region.flushcache();
      }
    }
    Assert.assertEquals(1, region.stores.size());
    Store store = region.stores.values().iterator().next();
    Assert.assertTrue("number of files " + store.getNumberOfstorefiles(),
      store.getNumberOfstorefiles() > 1);
    Assert.assertTrue(store.memstore.size.get() > 0);

    IdxScan idxScan = new IdxScan();
    idxScan.addFamily(family);
    int low = numRows / 10;
    int high = numRows - low;
    idxScan.setExpression(Or.or(Comparison.comparison(family, qualLong, Comparison.Operator.GTE, Bytes.toBytes((long) high)),
      Comparison.comparison(family, qualLong, Comparison.Operator.LT, Bytes.toBytes((long) low))));
    idxScan.setFilter(new FilterList(FilterList.Operator.MUST_PASS_ONE,
      Arrays.<Filter>asList(
        new SingleColumnValueFilter(family, qualLong, CompareFilter.CompareOp.GREATER_OR_EQUAL,
          new BinaryComparator(Bytes.toBytes((long) high))),
        new SingleColumnValueFilter(family, qualLong, CompareFilter.CompareOp.LESS,
          new BinaryComparator(Bytes.toBytes((long) low))))
    ));
    List<KeyValue> res = new ArrayList<KeyValue>();

    InternalScanner scanner = region.getScanner(idxScan);
    while (scanner.next(res)) ;
    Assert.assertEquals(low * 2, res.size());

    long memStoreSize = store.memstore.size.get();
    region.compactStores(majorcompaction);
    Assert.assertEquals(store.getNumberOfstorefiles(), 1);
    Assert.assertEquals(store.memstore.size.get(), memStoreSize);

    res.clear();
    scanner = region.getScanner(idxScan);
    while (scanner.next(res)) ;
    Assert.assertEquals(low * 2, res.size());

  }

  public void testFlushCacheWhileScanning() throws Exception {
    byte[] tableName = Bytes.toBytes("testIdxRegionCompaction");
    byte[] family = Bytes.toBytes("family");
    IdxIndexDescriptor indexDescriptor = new IdxIndexDescriptor(qualLong,
      IdxQualifierType.LONG);
    int numRows = 1000;
    int flushAndScanInterval = 10;
    int compactInterval = 10 * flushAndScanInterval;

    String method = "testFlushCacheWhileScanning";
    initIdxRegion(tableName, method, new HBaseConfiguration(), Pair.of(family,
      new IdxIndexDescriptor[]{indexDescriptor}));
    FlushThread flushThread = new FlushThread();
    flushThread.start();

    IdxScan idxScan = new IdxScan();
    idxScan.addFamily(family);
    idxScan.setExpression(Comparison.comparison(family, qualLong,
      Comparison.Operator.EQ, Bytes.toBytes((5L))));
    idxScan.setFilter(new SingleColumnValueFilter(family, qualLong,
      CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes(5L))));

    int expectedCount = 0;
    List<KeyValue> res = new ArrayList<KeyValue>();

    boolean toggle = true;
    for (long i = 0; i < numRows; i++) {
      Put put = new Put(Bytes.toBytes(i));
      put.add(family, qualLong, Bytes.toBytes(i % 10));
      region.put(put);

      if (i != 0 && i % compactInterval == 0) {
        //System.out.println("iteration = " + i);
        region.compactStores(true);
      }

      if (i % 10 == 5L) {
        expectedCount++;
      }

      if (i != 0 && i % flushAndScanInterval == 0) {
        res.clear();
        if (toggle) {
          flushThread.flush();
        }
        InternalScanner scanner = region.getScanner(idxScan);
        if (!toggle) {
          flushThread.flush();
        }
        while (scanner.next(res)) ;
        Assert.assertEquals("i=" + i, expectedCount, res.size());
        toggle = !toggle;
      }

    }

    flushThread.done();
    flushThread.join();
  }


  private class FlushThread extends Thread {
    private boolean done;
    private Throwable error = null;

    public void done() {
      done = true;
      interrupt();
    }

    public void checkNoError() {
      Assert.assertNull(error);
    }

    @Override
    public void run() {
      done = false;
      while (!done) {
        synchronized (this) {
          try {
            wait();
          } catch (InterruptedException ignored) {
            if (done) {
              break;
            }
          }
        }
        try {
          region.internalFlushcache();
        } catch (IOException e) {
          error = e;
          break;
        }
      }
    }

    public void flush() {
      synchronized (this) {
        notifyAll();
      }

    }
  }

  /**
   * Verifies the jmx bean for the region.
   *
   * @param region the refion to verify.
   */
  private static void verifyMBean(IdxRegion region) throws Exception {
    final MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
    ObjectName objectName =
      IdxRegionMBeanImpl.generateObjectName(region.getRegionInfo());
    MBeanInfo mbeanInfo = mbs.getMBeanInfo(objectName);

    for (MBeanAttributeInfo aInfo : mbeanInfo.getAttributes()) {
      Object value = mbs.getAttribute(objectName, aInfo.getName());
      if (aInfo.isWritable()) {
        mbs.setAttribute(objectName, new Attribute(aInfo.getName(), value));
      }
    }

    for (MBeanOperationInfo oInfo : mbeanInfo.getOperations()) {
      if (oInfo.getSignature().length == 0) {
        mbs.invoke(objectName, oInfo.getName(), new Object[]{}, new String[]{});
      } else {
        LOG.warn("Did not test operation " + oInfo.getName());
      }
    }
  }
}
