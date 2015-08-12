/**
 * Copyright The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hbase.regionserver;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeepDeletedCells;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.io.hfile.HFileContextBuilder;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.collect.Lists;
/**
 * Test cases against ReversibleKeyValueScanner
 */
@Category(MediumTests.class)
public class TestReversibleScanners {
  private static final Log LOG = LogFactory.getLog(TestReversibleScanners.class);
  HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private static byte[] FAMILYNAME = Bytes.toBytes("testCf");
  private static long TS = System.currentTimeMillis();
  private static int MAXMVCC = 7;
  private static byte[] ROW = Bytes.toBytes("testRow");
  private static final int ROWSIZE = 200;
  private static byte[][] ROWS = makeN(ROW, ROWSIZE);
  private static byte[] QUAL = Bytes.toBytes("testQual");
  private static final int QUALSIZE = 5;
  private static byte[][] QUALS = makeN(QUAL, QUALSIZE);
  private static byte[] VALUE = Bytes.toBytes("testValue");
  private static final int VALUESIZE = 3;
  private static byte[][] VALUES = makeN(VALUE, VALUESIZE);

  @Test
  public void testReversibleStoreFileScanner() throws IOException {
    FileSystem fs = TEST_UTIL.getTestFileSystem();
    Path hfilePath = new Path(new Path(
        TEST_UTIL.getDataTestDir("testReversibleStoreFileScanner"),
        "regionname"), "familyname");
    CacheConfig cacheConf = new CacheConfig(TEST_UTIL.getConfiguration());
    for (DataBlockEncoding encoding : DataBlockEncoding.values()) {
      HFileContextBuilder hcBuilder = new HFileContextBuilder();
      hcBuilder.withBlockSize(2 * 1024);
      hcBuilder.withDataBlockEncoding(encoding);
      HFileContext hFileContext = hcBuilder.build();
      StoreFile.Writer writer = new StoreFile.WriterBuilder(
          TEST_UTIL.getConfiguration(), cacheConf, fs).withOutputDir(hfilePath)
          .withFileContext(hFileContext).build();
      writeStoreFile(writer);

      StoreFile sf = new StoreFile(fs, writer.getPath(),
          TEST_UTIL.getConfiguration(), cacheConf, BloomType.NONE);

      List<StoreFileScanner> scanners = StoreFileScanner
          .getScannersForStoreFiles(Collections.singletonList(sf),
              false, true, false, false, Long.MAX_VALUE);
      StoreFileScanner scanner = scanners.get(0);
      seekTestOfReversibleKeyValueScanner(scanner);
      for (int readPoint = 0; readPoint < MAXMVCC; readPoint++) {
        LOG.info("Setting read point to " + readPoint);
        scanners = StoreFileScanner.getScannersForStoreFiles(
            Collections.singletonList(sf), false, true, false, false, readPoint);
        seekTestOfReversibleKeyValueScannerWithMVCC(scanners.get(0), readPoint);
      }
    }

  }

  @Test
  public void testReversibleMemstoreScanner() throws IOException {
    MemStore memstore = new DefaultMemStore();
    writeMemstore(memstore);
    List<KeyValueScanner> scanners = memstore.getScanners(Long.MAX_VALUE);
    seekTestOfReversibleKeyValueScanner(scanners.get(0));
    for (int readPoint = 0; readPoint < MAXMVCC; readPoint++) {
      LOG.info("Setting read point to " + readPoint);
      scanners = memstore.getScanners(readPoint);
      seekTestOfReversibleKeyValueScannerWithMVCC(scanners.get(0), readPoint);
    }

  }

  @Test
  public void testReversibleKeyValueHeap() throws IOException {
    // write data to one memstore and two store files
    FileSystem fs = TEST_UTIL.getTestFileSystem();
    Path hfilePath = new Path(new Path(
        TEST_UTIL.getDataTestDir("testReversibleKeyValueHeap"), "regionname"),
        "familyname");
    CacheConfig cacheConf = new CacheConfig(TEST_UTIL.getConfiguration());
    HFileContextBuilder hcBuilder = new HFileContextBuilder();
    hcBuilder.withBlockSize(2 * 1024);
    HFileContext hFileContext = hcBuilder.build();
    StoreFile.Writer writer1 = new StoreFile.WriterBuilder(
        TEST_UTIL.getConfiguration(), cacheConf, fs).withOutputDir(
        hfilePath).withFileContext(hFileContext).build();
    StoreFile.Writer writer2 = new StoreFile.WriterBuilder(
        TEST_UTIL.getConfiguration(), cacheConf, fs).withOutputDir(
        hfilePath).withFileContext(hFileContext).build();

    MemStore memstore = new DefaultMemStore();
    writeMemstoreAndStoreFiles(memstore, new StoreFile.Writer[] { writer1,
        writer2 });

    StoreFile sf1 = new StoreFile(fs, writer1.getPath(),
        TEST_UTIL.getConfiguration(), cacheConf, BloomType.NONE);

    StoreFile sf2 = new StoreFile(fs, writer2.getPath(),
        TEST_UTIL.getConfiguration(), cacheConf, BloomType.NONE);
    /**
     * Test without MVCC
     */
    int startRowNum = ROWSIZE / 2;
    ReversedKeyValueHeap kvHeap = getReversibleKeyValueHeap(memstore, sf1, sf2,
        ROWS[startRowNum], MAXMVCC);
    internalTestSeekAndNextForReversibleKeyValueHeap(kvHeap, startRowNum);

    startRowNum = ROWSIZE - 1;
    kvHeap = getReversibleKeyValueHeap(memstore, sf1, sf2,
        HConstants.EMPTY_START_ROW, MAXMVCC);
    internalTestSeekAndNextForReversibleKeyValueHeap(kvHeap, startRowNum);

    /**
     * Test with MVCC
     */
    for (int readPoint = 0; readPoint < MAXMVCC; readPoint++) {
      LOG.info("Setting read point to " + readPoint);
      startRowNum = ROWSIZE - 1;
      kvHeap = getReversibleKeyValueHeap(memstore, sf1, sf2,
          HConstants.EMPTY_START_ROW, readPoint);
      for (int i = startRowNum; i >= 0; i--) {
        if (i - 2 < 0) break;
        i = i - 2;
        kvHeap.seekToPreviousRow(KeyValueUtil.createFirstOnRow(ROWS[i + 1]));
        Pair<Integer, Integer> nextReadableNum = getNextReadableNumWithBackwardScan(
            i, 0, readPoint);
        if (nextReadableNum == null) break;
        KeyValue expecedKey = makeKV(nextReadableNum.getFirst(),
            nextReadableNum.getSecond());
        assertEquals(expecedKey, kvHeap.peek());
        i = nextReadableNum.getFirst();
        int qualNum = nextReadableNum.getSecond();
        if (qualNum + 1 < QUALSIZE) {
          kvHeap.backwardSeek(makeKV(i, qualNum + 1));
          nextReadableNum = getNextReadableNumWithBackwardScan(i, qualNum + 1,
              readPoint);
          if (nextReadableNum == null) break;
          expecedKey = makeKV(nextReadableNum.getFirst(),
              nextReadableNum.getSecond());
          assertEquals(expecedKey, kvHeap.peek());
          i = nextReadableNum.getFirst();
          qualNum = nextReadableNum.getSecond();
        }

        kvHeap.next();

        if (qualNum + 1 >= QUALSIZE) {
          nextReadableNum = getNextReadableNumWithBackwardScan(i - 1, 0,
              readPoint);
        } else {
          nextReadableNum = getNextReadableNumWithBackwardScan(i, qualNum + 1,
              readPoint);
        }
        if (nextReadableNum == null) break;
        expecedKey = makeKV(nextReadableNum.getFirst(),
            nextReadableNum.getSecond());
        assertEquals(expecedKey, kvHeap.peek());
        i = nextReadableNum.getFirst();
      }
    }
  }

  @Test
  public void testReversibleStoreScanner() throws IOException {
    // write data to one memstore and two store files
    FileSystem fs = TEST_UTIL.getTestFileSystem();
    Path hfilePath = new Path(new Path(
        TEST_UTIL.getDataTestDir("testReversibleStoreScanner"), "regionname"),
        "familyname");
    CacheConfig cacheConf = new CacheConfig(TEST_UTIL.getConfiguration());
    HFileContextBuilder hcBuilder = new HFileContextBuilder();
    hcBuilder.withBlockSize(2 * 1024);
    HFileContext hFileContext = hcBuilder.build();
    StoreFile.Writer writer1 = new StoreFile.WriterBuilder(
        TEST_UTIL.getConfiguration(), cacheConf, fs).withOutputDir(
        hfilePath).withFileContext(hFileContext).build();
    StoreFile.Writer writer2 = new StoreFile.WriterBuilder(
        TEST_UTIL.getConfiguration(), cacheConf, fs).withOutputDir(
        hfilePath).withFileContext(hFileContext).build();

    MemStore memstore = new DefaultMemStore();
    writeMemstoreAndStoreFiles(memstore, new StoreFile.Writer[] { writer1,
        writer2 });

    StoreFile sf1 = new StoreFile(fs, writer1.getPath(),
        TEST_UTIL.getConfiguration(), cacheConf, BloomType.NONE);

    StoreFile sf2 = new StoreFile(fs, writer2.getPath(),
        TEST_UTIL.getConfiguration(), cacheConf, BloomType.NONE);

    ScanType scanType = ScanType.USER_SCAN;
    ScanInfo scanInfo = new ScanInfo(FAMILYNAME, 0, Integer.MAX_VALUE,
        Long.MAX_VALUE, KeepDeletedCells.FALSE, 0, KeyValue.COMPARATOR);

    // Case 1.Test a full reversed scan
    Scan scan = new Scan();
    scan.setReversed(true);
    StoreScanner storeScanner = getReversibleStoreScanner(memstore, sf1, sf2,
        scan, scanType, scanInfo, MAXMVCC);
    verifyCountAndOrder(storeScanner, QUALSIZE * ROWSIZE, ROWSIZE, false);

    // Case 2.Test reversed scan with a specified start row
    int startRowNum = ROWSIZE / 2;
    byte[] startRow = ROWS[startRowNum];
    scan.setStartRow(startRow);
    storeScanner = getReversibleStoreScanner(memstore, sf1, sf2, scan,
        scanType, scanInfo, MAXMVCC);
    verifyCountAndOrder(storeScanner, QUALSIZE * (startRowNum + 1),
        startRowNum + 1, false);

    // Case 3.Test reversed scan with a specified start row and specified
    // qualifiers
    assertTrue(QUALSIZE > 2);
    scan.addColumn(FAMILYNAME, QUALS[0]);
    scan.addColumn(FAMILYNAME, QUALS[2]);
    storeScanner = getReversibleStoreScanner(memstore, sf1, sf2, scan,
        scanType, scanInfo, MAXMVCC);
    verifyCountAndOrder(storeScanner, 2 * (startRowNum + 1), startRowNum + 1,
        false);

    // Case 4.Test reversed scan with mvcc based on case 3
    for (int readPoint = 0; readPoint < MAXMVCC; readPoint++) {
      LOG.info("Setting read point to " + readPoint);
      storeScanner = getReversibleStoreScanner(memstore, sf1, sf2, scan,
          scanType, scanInfo, readPoint);
      int expectedRowCount = 0;
      int expectedKVCount = 0;
      for (int i = startRowNum; i >= 0; i--) {
        int kvCount = 0;
        if (makeMVCC(i, 0) <= readPoint) {
          kvCount++;
        }
        if (makeMVCC(i, 2) <= readPoint) {
          kvCount++;
        }
        if (kvCount > 0) {
          expectedRowCount++;
          expectedKVCount += kvCount;
        }
      }
      verifyCountAndOrder(storeScanner, expectedKVCount, expectedRowCount,
          false);
    }
  }

  @Test
  public void testReversibleRegionScanner() throws IOException {
    byte[] tableName = Bytes.toBytes("testtable");
    byte[] FAMILYNAME2 = Bytes.toBytes("testCf2");
    Configuration conf = HBaseConfiguration.create();
    HRegion region = TEST_UTIL.createLocalHRegion(tableName, null, null,
        "testReversibleRegionScanner", conf, false, Durability.SYNC_WAL, null,
        FAMILYNAME, FAMILYNAME2);
    loadDataToRegion(region, FAMILYNAME2);

    // verify row count with forward scan
    Scan scan = new Scan();
    InternalScanner scanner = region.getScanner(scan);
    verifyCountAndOrder(scanner, ROWSIZE * QUALSIZE * 2, ROWSIZE, true);

    // Case1:Full reversed scan
    scan.setReversed(true);
    scanner = region.getScanner(scan);
    verifyCountAndOrder(scanner, ROWSIZE * QUALSIZE * 2, ROWSIZE, false);

    // Case2:Full reversed scan with one family
    scan = new Scan();
    scan.setReversed(true);
    scan.addFamily(FAMILYNAME);
    scanner = region.getScanner(scan);
    verifyCountAndOrder(scanner, ROWSIZE * QUALSIZE, ROWSIZE, false);

    // Case3:Specify qualifiers + One family
    byte[][] specifiedQualifiers = { QUALS[1], QUALS[2] };
    for (byte[] specifiedQualifier : specifiedQualifiers)
      scan.addColumn(FAMILYNAME, specifiedQualifier);
    scanner = region.getScanner(scan);
    verifyCountAndOrder(scanner, ROWSIZE * 2, ROWSIZE, false);

    // Case4:Specify qualifiers + Two families
    for (byte[] specifiedQualifier : specifiedQualifiers)
      scan.addColumn(FAMILYNAME2, specifiedQualifier);
    scanner = region.getScanner(scan);
    verifyCountAndOrder(scanner, ROWSIZE * 2 * 2, ROWSIZE, false);

    // Case5: Case4 + specify start row
    int startRowNum = ROWSIZE * 3 / 4;
    scan.setStartRow(ROWS[startRowNum]);
    scanner = region.getScanner(scan);
    verifyCountAndOrder(scanner, (startRowNum + 1) * 2 * 2, (startRowNum + 1),
        false);

    // Case6: Case4 + specify stop row
    int stopRowNum = ROWSIZE / 4;
    scan.setStartRow(HConstants.EMPTY_BYTE_ARRAY);
    scan.setStopRow(ROWS[stopRowNum]);
    scanner = region.getScanner(scan);
    verifyCountAndOrder(scanner, (ROWSIZE - stopRowNum - 1) * 2 * 2, (ROWSIZE
        - stopRowNum - 1), false);

    // Case7: Case4 + specify start row + specify stop row
    scan.setStartRow(ROWS[startRowNum]);
    scanner = region.getScanner(scan);
    verifyCountAndOrder(scanner, (startRowNum - stopRowNum) * 2 * 2,
        (startRowNum - stopRowNum), false);

    // Case8: Case7 + SingleColumnValueFilter
    int valueNum = startRowNum % VALUESIZE;
    Filter filter = new SingleColumnValueFilter(FAMILYNAME,
        specifiedQualifiers[0], CompareOp.EQUAL, VALUES[valueNum]);
    scan.setFilter(filter);
    scanner = region.getScanner(scan);
    int unfilteredRowNum = (startRowNum - stopRowNum) / VALUESIZE
        + (stopRowNum / VALUESIZE == valueNum ? 0 : 1);
    verifyCountAndOrder(scanner, unfilteredRowNum * 2 * 2, unfilteredRowNum,
        false);

    // Case9: Case7 + PageFilter
    int pageSize = 10;
    filter = new PageFilter(pageSize);
    scan.setFilter(filter);
    scanner = region.getScanner(scan);
    int expectedRowNum = pageSize;
    verifyCountAndOrder(scanner, expectedRowNum * 2 * 2, expectedRowNum, false);

    // Case10: Case7 + FilterList+MUST_PASS_ONE
    SingleColumnValueFilter scvFilter1 = new SingleColumnValueFilter(
        FAMILYNAME, specifiedQualifiers[0], CompareOp.EQUAL, VALUES[0]);
    SingleColumnValueFilter scvFilter2 = new SingleColumnValueFilter(
        FAMILYNAME, specifiedQualifiers[0], CompareOp.EQUAL, VALUES[1]);
    expectedRowNum = 0;
    for (int i = startRowNum; i > stopRowNum; i--) {
      if (i % VALUESIZE == 0 || i % VALUESIZE == 1) {
        expectedRowNum++;
      }
    }
    filter = new FilterList(Operator.MUST_PASS_ONE, scvFilter1, scvFilter2);
    scan.setFilter(filter);
    scanner = region.getScanner(scan);
    verifyCountAndOrder(scanner, expectedRowNum * 2 * 2, expectedRowNum, false);

    // Case10: Case7 + FilterList+MUST_PASS_ALL
    filter = new FilterList(Operator.MUST_PASS_ALL, scvFilter1, scvFilter2);
    expectedRowNum = 0;
    scan.setFilter(filter);
    scanner = region.getScanner(scan);
    verifyCountAndOrder(scanner, expectedRowNum * 2 * 2, expectedRowNum, false);
  }

  private StoreScanner getReversibleStoreScanner(MemStore memstore,
      StoreFile sf1, StoreFile sf2, Scan scan, ScanType scanType,
      ScanInfo scanInfo, int readPoint) throws IOException {
    List<KeyValueScanner> scanners = getScanners(memstore, sf1, sf2, null,
        false, readPoint);
    NavigableSet<byte[]> columns = null;
    for (Map.Entry<byte[], NavigableSet<byte[]>> entry : scan.getFamilyMap()
        .entrySet()) {
      // Should only one family
      columns = entry.getValue();
    }
    StoreScanner storeScanner = new ReversedStoreScanner(scan, scanInfo,
        scanType, columns, scanners);
    return storeScanner;
  }

  private void verifyCountAndOrder(InternalScanner scanner,
      int expectedKVCount, int expectedRowCount, boolean forward)
      throws IOException {
    List<Cell> kvList = new ArrayList<Cell>();
    Result lastResult = null;
    int rowCount = 0;
    int kvCount = 0;
    try {
      while (scanner.next(kvList)) {
        if (kvList.isEmpty()) continue;
        rowCount++;
        kvCount += kvList.size();
        if (lastResult != null) {
          Result curResult = Result.create(kvList);
          assertEquals("LastResult:" + lastResult + "CurResult:" + curResult,
              forward,
              Bytes.compareTo(curResult.getRow(), lastResult.getRow()) > 0);
        }
        lastResult = Result.create(kvList);
        kvList.clear();
      }
    } finally {
      scanner.close();
    }
    if (!kvList.isEmpty()) {
      rowCount++;
      kvCount += kvList.size();
      kvList.clear();
    }
    assertEquals(expectedKVCount, kvCount);
    assertEquals(expectedRowCount, rowCount);
  }

  private void internalTestSeekAndNextForReversibleKeyValueHeap(
      ReversedKeyValueHeap kvHeap, int startRowNum) throws IOException {
    // Test next and seek
    for (int i = startRowNum; i >= 0; i--) {
      if (i % 2 == 1 && i - 2 >= 0) {
        i = i - 2;
        kvHeap.seekToPreviousRow(KeyValueUtil.createFirstOnRow(ROWS[i + 1]));
      }
      for (int j = 0; j < QUALSIZE; j++) {
        if (j % 2 == 1 && (j + 1) < QUALSIZE) {
          j = j + 1;
          kvHeap.backwardSeek(makeKV(i, j));
        }
        assertEquals(makeKV(i, j), kvHeap.peek());
        kvHeap.next();
      }
    }
    assertEquals(null, kvHeap.peek());
  }

  private ReversedKeyValueHeap getReversibleKeyValueHeap(MemStore memstore,
      StoreFile sf1, StoreFile sf2, byte[] startRow, int readPoint)
      throws IOException {
    List<KeyValueScanner> scanners = getScanners(memstore, sf1, sf2, startRow,
        true, readPoint);
    ReversedKeyValueHeap kvHeap = new ReversedKeyValueHeap(scanners,
        KeyValue.COMPARATOR);
    return kvHeap;
  }

  private List<KeyValueScanner> getScanners(MemStore memstore, StoreFile sf1,
      StoreFile sf2, byte[] startRow, boolean doSeek, int readPoint)
      throws IOException {
    List<StoreFileScanner> fileScanners = StoreFileScanner
        .getScannersForStoreFiles(Lists.newArrayList(sf1, sf2), false, true,
            false, false, readPoint);
    List<KeyValueScanner> memScanners = memstore.getScanners(readPoint);
    List<KeyValueScanner> scanners = new ArrayList<KeyValueScanner>(
        fileScanners.size() + 1);
    scanners.addAll(fileScanners);
    scanners.addAll(memScanners);

    if (doSeek) {
      if (Bytes.equals(HConstants.EMPTY_START_ROW, startRow)) {
        for (KeyValueScanner scanner : scanners) {
          scanner.seekToLastRow();
        }
      } else {
        KeyValue startKey = KeyValueUtil.createFirstOnRow(startRow);
        for (KeyValueScanner scanner : scanners) {
          scanner.backwardSeek(startKey);
        }
      }
    }
    return scanners;
  }

  private void seekTestOfReversibleKeyValueScanner(KeyValueScanner scanner)
      throws IOException {
    /**
     * Test without MVCC
     */
    // Test seek to last row
    assertTrue(scanner.seekToLastRow());
    assertEquals(makeKV(ROWSIZE - 1, 0), scanner.peek());

    // Test backward seek in three cases
    // Case1: seek in the same row in backwardSeek
    KeyValue seekKey = makeKV(ROWSIZE - 2, QUALSIZE - 2);
    assertTrue(scanner.backwardSeek(seekKey));
    assertEquals(seekKey, scanner.peek());

    // Case2: seek to the previous row in backwardSeek
    int seekRowNum = ROWSIZE - 2;
    assertTrue(scanner.backwardSeek(KeyValueUtil.createLastOnRow(ROWS[seekRowNum])));
    KeyValue expectedKey = makeKV(seekRowNum - 1, 0);
    assertEquals(expectedKey, scanner.peek());

    // Case3: unable to backward seek
    assertFalse(scanner.backwardSeek(KeyValueUtil.createLastOnRow(ROWS[0])));
    assertEquals(null, scanner.peek());

    // Test seek to previous row
    seekRowNum = ROWSIZE - 4;
    assertTrue(scanner.seekToPreviousRow(KeyValueUtil
        .createFirstOnRow(ROWS[seekRowNum])));
    expectedKey = makeKV(seekRowNum - 1, 0);
    assertEquals(expectedKey, scanner.peek());

    // Test seek to previous row for the first row
    assertFalse(scanner.seekToPreviousRow(makeKV(0, 0)));
    assertEquals(null, scanner.peek());

  }

  private void seekTestOfReversibleKeyValueScannerWithMVCC(
      KeyValueScanner scanner, int readPoint) throws IOException {
    /**
     * Test with MVCC
     */
      // Test seek to last row
      KeyValue expectedKey = getNextReadableKeyValueWithBackwardScan(
          ROWSIZE - 1, 0, readPoint);
      assertEquals(expectedKey != null, scanner.seekToLastRow());
      assertEquals(expectedKey, scanner.peek());

      // Test backward seek in two cases
      // Case1: seek in the same row in backwardSeek
      expectedKey = getNextReadableKeyValueWithBackwardScan(ROWSIZE - 2,
          QUALSIZE - 2, readPoint);
      assertEquals(expectedKey != null, scanner.backwardSeek(expectedKey));
      assertEquals(expectedKey, scanner.peek());

      // Case2: seek to the previous row in backwardSeek
    int seekRowNum = ROWSIZE - 3;
    KeyValue seekKey = KeyValueUtil.createLastOnRow(ROWS[seekRowNum]);
      expectedKey = getNextReadableKeyValueWithBackwardScan(seekRowNum - 1, 0,
          readPoint);
      assertEquals(expectedKey != null, scanner.backwardSeek(seekKey));
      assertEquals(expectedKey, scanner.peek());

      // Test seek to previous row
      seekRowNum = ROWSIZE - 4;
      expectedKey = getNextReadableKeyValueWithBackwardScan(seekRowNum - 1, 0,
          readPoint);
      assertEquals(expectedKey != null, scanner.seekToPreviousRow(KeyValueUtil
          .createFirstOnRow(ROWS[seekRowNum])));
      assertEquals(expectedKey, scanner.peek());
  }

  private KeyValue getNextReadableKeyValueWithBackwardScan(int startRowNum,
      int startQualNum, int readPoint) {
    Pair<Integer, Integer> nextReadableNum = getNextReadableNumWithBackwardScan(
        startRowNum, startQualNum, readPoint);
    if (nextReadableNum == null)
      return null;
    return makeKV(nextReadableNum.getFirst(), nextReadableNum.getSecond());
  }

  private Pair<Integer, Integer> getNextReadableNumWithBackwardScan(
      int startRowNum, int startQualNum, int readPoint) {
    Pair<Integer, Integer> nextReadableNum = null;
    boolean findExpected = false;
    for (int i = startRowNum; i >= 0; i--) {
      for (int j = (i == startRowNum ? startQualNum : 0); j < QUALSIZE; j++) {
        if (makeMVCC(i, j) <= readPoint) {
          nextReadableNum = new Pair<Integer, Integer>(i, j);
          findExpected = true;
          break;
        }
      }
      if (findExpected)
        break;
    }
    return nextReadableNum;
  }

  private static void loadDataToRegion(Region region, byte[] additionalFamily)
      throws IOException {
    for (int i = 0; i < ROWSIZE; i++) {
      Put put = new Put(ROWS[i]);
      for (int j = 0; j < QUALSIZE; j++) {
        put.add(makeKV(i, j));
        // put additional family
        put.add(makeKV(i, j, additionalFamily));
      }
      region.put(put);
      if (i == ROWSIZE / 3 || i == ROWSIZE * 2 / 3) {
        region.flush(true);
      }
    }
  }

  private static void writeMemstoreAndStoreFiles(MemStore memstore,
      final StoreFile.Writer[] writers) throws IOException {
    try {
      for (int i = 0; i < ROWSIZE; i++) {
        for (int j = 0; j < QUALSIZE; j++) {
          if (i % 2 == 0) {
            memstore.add(makeKV(i, j));
          } else {
            writers[(i + j) % writers.length].append(makeKV(i, j));
          }
        }
      }
    } finally {
      for (int i = 0; i < writers.length; i++) {
        writers[i].close();
      }
    }
  }

  private static void writeStoreFile(final StoreFile.Writer writer)
      throws IOException {
    try {
      for (int i = 0; i < ROWSIZE; i++) {
        for (int j = 0; j < QUALSIZE; j++) {
          writer.append(makeKV(i, j));
        }
      }
    } finally {
      writer.close();
    }
  }

  private static void writeMemstore(MemStore memstore) throws IOException {
    // Add half of the keyvalues to memstore
    for (int i = 0; i < ROWSIZE; i++) {
      for (int j = 0; j < QUALSIZE; j++) {
        if ((i + j) % 2 == 0) {
          memstore.add(makeKV(i, j));
        }
      }
    }
    memstore.snapshot();
    // Add another half of the keyvalues to snapshot
    for (int i = 0; i < ROWSIZE; i++) {
      for (int j = 0; j < QUALSIZE; j++) {
        if ((i + j) % 2 == 1) {
          memstore.add(makeKV(i, j));
        }
      }
    }
  }

  private static KeyValue makeKV(int rowNum, int cqNum) {
    return makeKV(rowNum, cqNum, FAMILYNAME);
  }

  private static KeyValue makeKV(int rowNum, int cqNum, byte[] familyName) {
    KeyValue kv = new KeyValue(ROWS[rowNum], familyName, QUALS[cqNum], TS,
        VALUES[rowNum % VALUESIZE]);
    kv.setSequenceId(makeMVCC(rowNum, cqNum));
    return kv;
  }

  private static long makeMVCC(int rowNum, int cqNum) {
    return (rowNum + cqNum) % (MAXMVCC + 1);
  }

  private static byte[][] makeN(byte[] base, int n) {
    byte[][] ret = new byte[n][];
    for (int i = 0; i < n; i++) {
      ret[i] = Bytes.add(base, Bytes.toBytes(String.format("%04d", i)));
    }
    return ret;
  }
}
