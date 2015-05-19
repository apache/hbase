/*
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

import static org.apache.hadoop.hbase.regionserver.KeyValueScanFixture.scanFixture;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.NavigableSet;
import java.util.TreeSet;

import junit.framework.TestCase;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeepDeletedCells;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueTestUtil;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdge;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManagerTestHelper;
import org.junit.experimental.categories.Category;

// Can't be small as it plays with EnvironmentEdgeManager
@Category({RegionServerTests.class, MediumTests.class})
public class TestStoreScanner extends TestCase {
  private static final String CF_STR = "cf";
  final byte [] CF = Bytes.toBytes(CF_STR);
  private ScanInfo scanInfo = new ScanInfo(CF, 0, Integer.MAX_VALUE,
      Long.MAX_VALUE, KeepDeletedCells.FALSE, 0, CellComparator.COMPARATOR);
  private ScanType scanType = ScanType.USER_SCAN;

  public void setUp() throws Exception {
    super.setUp();
  }

  /*
   * Test utility for building a NavigableSet for scanners.
   * @param strCols
   * @return
   */
  NavigableSet<byte[]> getCols(String ...strCols) {
    NavigableSet<byte[]> cols = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);
    for (String col : strCols) {
      byte[] bytes = Bytes.toBytes(col);
      cols.add(bytes);
    }
    return cols;
  }

  public void testScanTimeRange() throws IOException {
    String r1 = "R1";
    // returns only 1 of these 2 even though same timestamp
    KeyValue [] kvs = new KeyValue[] {
        KeyValueTestUtil.create(r1, CF_STR, "a", 1, KeyValue.Type.Put, "dont-care"),
        KeyValueTestUtil.create(r1, CF_STR, "a", 2, KeyValue.Type.Put, "dont-care"),
        KeyValueTestUtil.create(r1, CF_STR, "a", 3, KeyValue.Type.Put, "dont-care"),
        KeyValueTestUtil.create(r1, CF_STR, "a", 4, KeyValue.Type.Put, "dont-care"),
        KeyValueTestUtil.create(r1, CF_STR, "a", 5, KeyValue.Type.Put, "dont-care"),
    };
    List<KeyValueScanner> scanners = Arrays.<KeyValueScanner>asList(
        new KeyValueScanner[] {
            new KeyValueScanFixture(CellComparator.COMPARATOR, kvs)
    });
    Scan scanSpec = new Scan(Bytes.toBytes(r1));
    scanSpec.setTimeRange(0, 6);
    scanSpec.setMaxVersions();
    StoreScanner scan = new StoreScanner(scanSpec, scanInfo, scanType,
        getCols("a"), scanners);
    List<Cell> results = new ArrayList<Cell>();
    assertEquals(true, scan.next(results));
    assertEquals(5, results.size());
    assertEquals(kvs[kvs.length - 1], results.get(0));
    // Scan limited TimeRange
    scanSpec = new Scan(Bytes.toBytes(r1));
    scanSpec.setTimeRange(1, 3);
    scanSpec.setMaxVersions();
    scan = new StoreScanner(scanSpec, scanInfo, scanType, getCols("a"),
        scanners);
    results = new ArrayList<Cell>();
    assertEquals(true, scan.next(results));
    assertEquals(2, results.size());
    // Another range.
    scanSpec = new Scan(Bytes.toBytes(r1));
    scanSpec.setTimeRange(5, 10);
    scanSpec.setMaxVersions();
    scan = new StoreScanner(scanSpec, scanInfo, scanType, getCols("a"),
        scanners);
    results = new ArrayList<Cell>();
    assertEquals(true, scan.next(results));
    assertEquals(1, results.size());
    // See how TimeRange and Versions interact.
    // Another range.
    scanSpec = new Scan(Bytes.toBytes(r1));
    scanSpec.setTimeRange(0, 10);
    scanSpec.setMaxVersions(3);
    scan = new StoreScanner(scanSpec, scanInfo, scanType, getCols("a"),
        scanners);
    results = new ArrayList<Cell>();
    assertEquals(true, scan.next(results));
    assertEquals(3, results.size());
  }

  public void testScanSameTimestamp() throws IOException {
    // returns only 1 of these 2 even though same timestamp
    KeyValue [] kvs = new KeyValue[] {
        KeyValueTestUtil.create("R1", "cf", "a", 1, KeyValue.Type.Put, "dont-care"),
        KeyValueTestUtil.create("R1", "cf", "a", 1, KeyValue.Type.Put, "dont-care"),
    };
    List<KeyValueScanner> scanners = Arrays.asList(
        new KeyValueScanner[] {
            new KeyValueScanFixture(CellComparator.COMPARATOR, kvs)
        });

    Scan scanSpec = new Scan(Bytes.toBytes("R1"));
    // this only uses maxVersions (default=1) and TimeRange (default=all)
    StoreScanner scan = new StoreScanner(scanSpec, scanInfo, scanType,
        getCols("a"), scanners);

    List<Cell> results = new ArrayList<Cell>();
    assertEquals(true, scan.next(results));
    assertEquals(1, results.size());
    assertEquals(kvs[0], results.get(0));
  }

  /*
   * Test test shows exactly how the matcher's return codes confuses the StoreScanner
   * and prevent it from doing the right thing.  Seeking once, then nexting twice
   * should return R1, then R2, but in this case it doesnt.
   * TODO this comment makes no sense above. Appears to do the right thing.
   * @throws IOException
   */
  public void testWontNextToNext() throws IOException {
    // build the scan file:
    KeyValue [] kvs = new KeyValue[] {
        KeyValueTestUtil.create("R1", "cf", "a", 2, KeyValue.Type.Put, "dont-care"),
        KeyValueTestUtil.create("R1", "cf", "a", 1, KeyValue.Type.Put, "dont-care"),
        KeyValueTestUtil.create("R2", "cf", "a", 1, KeyValue.Type.Put, "dont-care")
    };
    List<KeyValueScanner> scanners = scanFixture(kvs);

    Scan scanSpec = new Scan(Bytes.toBytes("R1"));
    // this only uses maxVersions (default=1) and TimeRange (default=all)
    StoreScanner scan = new StoreScanner(scanSpec, scanInfo, scanType,
        getCols("a"), scanners);

    List<Cell> results = new ArrayList<Cell>();
    scan.next(results);
    assertEquals(1, results.size());
    assertEquals(kvs[0], results.get(0));
    // should be ok...
    // now scan _next_ again.
    results.clear();
    scan.next(results);
    assertEquals(1, results.size());
    assertEquals(kvs[2], results.get(0));

    results.clear();
    scan.next(results);
    assertEquals(0, results.size());

  }


  public void testDeleteVersionSameTimestamp() throws IOException {
    KeyValue [] kvs = new KeyValue [] {
        KeyValueTestUtil.create("R1", "cf", "a", 1, KeyValue.Type.Put, "dont-care"),
        KeyValueTestUtil.create("R1", "cf", "a", 1, KeyValue.Type.Delete, "dont-care"),
    };
    List<KeyValueScanner> scanners = scanFixture(kvs);
    Scan scanSpec = new Scan(Bytes.toBytes("R1"));
    StoreScanner scan = new StoreScanner(scanSpec, scanInfo, scanType,
        getCols("a"), scanners);

    List<Cell> results = new ArrayList<Cell>();
    assertFalse(scan.next(results));
    assertEquals(0, results.size());
  }

  /*
   * Test the case where there is a delete row 'in front of' the next row, the scanner
   * will move to the next row.
   */
  public void testDeletedRowThenGoodRow() throws IOException {
    KeyValue [] kvs = new KeyValue [] {
        KeyValueTestUtil.create("R1", "cf", "a", 1, KeyValue.Type.Put, "dont-care"),
        KeyValueTestUtil.create("R1", "cf", "a", 1, KeyValue.Type.Delete, "dont-care"),
        KeyValueTestUtil.create("R2", "cf", "a", 20, KeyValue.Type.Put, "dont-care")
    };
    List<KeyValueScanner> scanners = scanFixture(kvs);
    Scan scanSpec = new Scan(Bytes.toBytes("R1"));
    StoreScanner scan = new StoreScanner(scanSpec, scanInfo, scanType,
        getCols("a"), scanners);

    List<Cell> results = new ArrayList<Cell>();
    assertEquals(true, scan.next(results));
    assertEquals(0, results.size());

    assertEquals(true, scan.next(results));
    assertEquals(1, results.size());
    assertEquals(kvs[2], results.get(0));

    assertEquals(false, scan.next(results));
  }

  public void testDeleteVersionMaskingMultiplePuts() throws IOException {
    long now = System.currentTimeMillis();
    KeyValue [] kvs1 = new KeyValue[] {
        KeyValueTestUtil.create("R1", "cf", "a", now, KeyValue.Type.Put, "dont-care"),
        KeyValueTestUtil.create("R1", "cf", "a", now, KeyValue.Type.Delete, "dont-care")
    };
    KeyValue [] kvs2 = new KeyValue[] {
        KeyValueTestUtil.create("R1", "cf", "a", now-500, KeyValue.Type.Put, "dont-care"),
        KeyValueTestUtil.create("R1", "cf", "a", now-100, KeyValue.Type.Put, "dont-care"),
        KeyValueTestUtil.create("R1", "cf", "a", now, KeyValue.Type.Put, "dont-care")
    };
    List<KeyValueScanner> scanners = scanFixture(kvs1, kvs2);

    StoreScanner scan = new StoreScanner(new Scan(Bytes.toBytes("R1")),
        scanInfo, scanType, getCols("a"), scanners);
    List<Cell> results = new ArrayList<Cell>();
    // the two put at ts=now will be masked by the 1 delete, and
    // since the scan default returns 1 version we'll return the newest
    // key, which is kvs[2], now-100.
    assertEquals(true, scan.next(results));
    assertEquals(1, results.size());
    assertEquals(kvs2[1], results.get(0));
  }
  public void testDeleteVersionsMixedAndMultipleVersionReturn() throws IOException {
    long now = System.currentTimeMillis();
    KeyValue [] kvs1 = new KeyValue[] {
        KeyValueTestUtil.create("R1", "cf", "a", now, KeyValue.Type.Put, "dont-care"),
        KeyValueTestUtil.create("R1", "cf", "a", now, KeyValue.Type.Delete, "dont-care")
    };
    KeyValue [] kvs2 = new KeyValue[] {
        KeyValueTestUtil.create("R1", "cf", "a", now-500, KeyValue.Type.Put, "dont-care"),
        KeyValueTestUtil.create("R1", "cf", "a", now+500, KeyValue.Type.Put, "dont-care"),
        KeyValueTestUtil.create("R1", "cf", "a", now, KeyValue.Type.Put, "dont-care"),
        KeyValueTestUtil.create("R2", "cf", "z", now, KeyValue.Type.Put, "dont-care")
    };
    List<KeyValueScanner> scanners = scanFixture(kvs1, kvs2);

    Scan scanSpec = new Scan(Bytes.toBytes("R1")).setMaxVersions(2);
    StoreScanner scan = new StoreScanner(scanSpec, scanInfo, scanType,
        getCols("a"), scanners);
    List<Cell> results = new ArrayList<Cell>();
    assertEquals(true, scan.next(results));
    assertEquals(2, results.size());
    assertEquals(kvs2[1], results.get(0));
    assertEquals(kvs2[0], results.get(1));
  }

  public void testWildCardOneVersionScan() throws IOException {
    KeyValue [] kvs = new KeyValue [] {
        KeyValueTestUtil.create("R1", "cf", "a", 2, KeyValue.Type.Put, "dont-care"),
        KeyValueTestUtil.create("R1", "cf", "b", 1, KeyValue.Type.Put, "dont-care"),
        KeyValueTestUtil.create("R1", "cf", "a", 1, KeyValue.Type.DeleteColumn, "dont-care"),
    };
    List<KeyValueScanner> scanners = scanFixture(kvs);
    StoreScanner scan = new StoreScanner(new Scan(Bytes.toBytes("R1")),
        scanInfo, scanType, null, scanners);
    List<Cell> results = new ArrayList<Cell>();
    assertEquals(true, scan.next(results));
    assertEquals(2, results.size());
    assertEquals(kvs[0], results.get(0));
    assertEquals(kvs[1], results.get(1));
  }

  public void testWildCardScannerUnderDeletes() throws IOException {
    KeyValue [] kvs = new KeyValue [] {
        KeyValueTestUtil.create("R1", "cf", "a", 2, KeyValue.Type.Put, "dont-care"), // inc
        // orphaned delete column.
        KeyValueTestUtil.create("R1", "cf", "a", 1, KeyValue.Type.DeleteColumn, "dont-care"),
        // column b
        KeyValueTestUtil.create("R1", "cf", "b", 2, KeyValue.Type.Put, "dont-care"), // inc
        KeyValueTestUtil.create("R1", "cf", "b", 1, KeyValue.Type.Put, "dont-care"), // inc
        // column c
        KeyValueTestUtil.create("R1", "cf", "c", 10, KeyValue.Type.Delete, "dont-care"),
        KeyValueTestUtil.create("R1", "cf", "c", 10, KeyValue.Type.Put, "dont-care"), // no
        KeyValueTestUtil.create("R1", "cf", "c", 9, KeyValue.Type.Put, "dont-care"),  // inc
        // column d
        KeyValueTestUtil.create("R1", "cf", "d", 11, KeyValue.Type.Put, "dont-care"), // inc
        KeyValueTestUtil.create("R1", "cf", "d", 10, KeyValue.Type.DeleteColumn, "dont-care"),
        KeyValueTestUtil.create("R1", "cf", "d", 9, KeyValue.Type.Put, "dont-care"),  // no
        KeyValueTestUtil.create("R1", "cf", "d", 8, KeyValue.Type.Put, "dont-care"),  // no

    };
    List<KeyValueScanner> scanners = scanFixture(kvs);
    StoreScanner scan = new StoreScanner(new Scan().setMaxVersions(2),
        scanInfo, scanType, null, scanners);
    List<Cell> results = new ArrayList<Cell>();
    assertEquals(true, scan.next(results));
    assertEquals(5, results.size());
    assertEquals(kvs[0], results.get(0));
    assertEquals(kvs[2], results.get(1));
    assertEquals(kvs[3], results.get(2));
    assertEquals(kvs[6], results.get(3));
    assertEquals(kvs[7], results.get(4));
  }

  public void testDeleteFamily() throws IOException {
    KeyValue [] kvs = new KeyValue[] {
        KeyValueTestUtil.create("R1", "cf", "a", 100, KeyValue.Type.DeleteFamily, "dont-care"),
        KeyValueTestUtil.create("R1", "cf", "b", 11, KeyValue.Type.Put, "dont-care"),
        KeyValueTestUtil.create("R1", "cf", "c", 11, KeyValue.Type.Put, "dont-care"),
        KeyValueTestUtil.create("R1", "cf", "d", 11, KeyValue.Type.Put, "dont-care"),
        KeyValueTestUtil.create("R1", "cf", "e", 11, KeyValue.Type.Put, "dont-care"),
        KeyValueTestUtil.create("R1", "cf", "e", 11, KeyValue.Type.DeleteColumn, "dont-care"),
        KeyValueTestUtil.create("R1", "cf", "f", 11, KeyValue.Type.Put, "dont-care"),
        KeyValueTestUtil.create("R1", "cf", "g", 11, KeyValue.Type.Put, "dont-care"),
        KeyValueTestUtil.create("R1", "cf", "g", 11, KeyValue.Type.Delete, "dont-care"),
        KeyValueTestUtil.create("R1", "cf", "h", 11, KeyValue.Type.Put, "dont-care"),
        KeyValueTestUtil.create("R1", "cf", "i", 11, KeyValue.Type.Put, "dont-care"),
        KeyValueTestUtil.create("R2", "cf", "a", 11, KeyValue.Type.Put, "dont-care"),
    };
    List<KeyValueScanner> scanners = scanFixture(kvs);
    StoreScanner scan = new StoreScanner(
        new Scan().setMaxVersions(Integer.MAX_VALUE), scanInfo, scanType, null,
        scanners);
    List<Cell> results = new ArrayList<Cell>();
    assertEquals(true, scan.next(results));
    assertEquals(0, results.size());
    assertEquals(true, scan.next(results));
    assertEquals(1, results.size());
    assertEquals(kvs[kvs.length-1], results.get(0));

    assertEquals(false, scan.next(results));
  }

  public void testDeleteColumn() throws IOException {
    KeyValue [] kvs = new KeyValue[] {
        KeyValueTestUtil.create("R1", "cf", "a", 10, KeyValue.Type.DeleteColumn, "dont-care"),
        KeyValueTestUtil.create("R1", "cf", "a", 9, KeyValue.Type.Delete, "dont-care"),
        KeyValueTestUtil.create("R1", "cf", "a", 8, KeyValue.Type.Put, "dont-care"),
        KeyValueTestUtil.create("R1", "cf", "b", 5, KeyValue.Type.Put, "dont-care")
    };
    List<KeyValueScanner> scanners = scanFixture(kvs);
    StoreScanner scan = new StoreScanner(new Scan(), scanInfo, scanType, null,
        scanners);
    List<Cell> results = new ArrayList<Cell>();
    assertEquals(true, scan.next(results));
    assertEquals(1, results.size());
    assertEquals(kvs[3], results.get(0));
  }

  private static final  KeyValue [] kvs = new KeyValue[] {
        KeyValueTestUtil.create("R1", "cf", "a", 11, KeyValue.Type.Put, "dont-care"),
        KeyValueTestUtil.create("R1", "cf", "b", 11, KeyValue.Type.Put, "dont-care"),
        KeyValueTestUtil.create("R1", "cf", "c", 11, KeyValue.Type.Put, "dont-care"),
        KeyValueTestUtil.create("R1", "cf", "d", 11, KeyValue.Type.Put, "dont-care"),
        KeyValueTestUtil.create("R1", "cf", "e", 11, KeyValue.Type.Put, "dont-care"),
        KeyValueTestUtil.create("R1", "cf", "f", 11, KeyValue.Type.Put, "dont-care"),
        KeyValueTestUtil.create("R1", "cf", "g", 11, KeyValue.Type.Put, "dont-care"),
        KeyValueTestUtil.create("R1", "cf", "h", 11, KeyValue.Type.Put, "dont-care"),
        KeyValueTestUtil.create("R1", "cf", "i", 11, KeyValue.Type.Put, "dont-care"),
        KeyValueTestUtil.create("R2", "cf", "a", 11, KeyValue.Type.Put, "dont-care"),
    };

  public void testSkipColumn() throws IOException {
    List<KeyValueScanner> scanners = scanFixture(kvs);
    StoreScanner scan = new StoreScanner(new Scan(), scanInfo, scanType,
        getCols("a", "d"), scanners);

    List<Cell> results = new ArrayList<Cell>();
    assertEquals(true, scan.next(results));
    assertEquals(2, results.size());
    assertEquals(kvs[0], results.get(0));
    assertEquals(kvs[3], results.get(1));
    results.clear();

    assertEquals(true, scan.next(results));
    assertEquals(1, results.size());
    assertEquals(kvs[kvs.length-1], results.get(0));

    results.clear();
    assertEquals(false, scan.next(results));
  }

  /*
   * Test expiration of KeyValues in combination with a configured TTL for
   * a column family (as should be triggered in a major compaction).
   */
  public void testWildCardTtlScan() throws IOException {
    long now = System.currentTimeMillis();
    KeyValue [] kvs = new KeyValue[] {
        KeyValueTestUtil.create("R1", "cf", "a", now-1000, KeyValue.Type.Put, "dont-care"),
        KeyValueTestUtil.create("R1", "cf", "b", now-10, KeyValue.Type.Put, "dont-care"),
        KeyValueTestUtil.create("R1", "cf", "c", now-200, KeyValue.Type.Put, "dont-care"),
        KeyValueTestUtil.create("R1", "cf", "d", now-10000, KeyValue.Type.Put, "dont-care"),
        KeyValueTestUtil.create("R2", "cf", "a", now, KeyValue.Type.Put, "dont-care"),
        KeyValueTestUtil.create("R2", "cf", "b", now-10, KeyValue.Type.Put, "dont-care"),
        KeyValueTestUtil.create("R2", "cf", "c", now-200, KeyValue.Type.Put, "dont-care"),
        KeyValueTestUtil.create("R2", "cf", "c", now-1000, KeyValue.Type.Put, "dont-care")
    };
    List<KeyValueScanner> scanners = scanFixture(kvs);
    Scan scan = new Scan();
    scan.setMaxVersions(1);
    ScanInfo scanInfo = new ScanInfo(CF, 0, 1, 500, KeepDeletedCells.FALSE, 0,
        CellComparator.COMPARATOR);
    ScanType scanType = ScanType.USER_SCAN;
    StoreScanner scanner =
      new StoreScanner(scan, scanInfo, scanType,
          null, scanners);

    List<Cell> results = new ArrayList<Cell>();
    assertEquals(true, scanner.next(results));
    assertEquals(2, results.size());
    assertEquals(kvs[1], results.get(0));
    assertEquals(kvs[2], results.get(1));
    results.clear();

    assertEquals(true, scanner.next(results));
    assertEquals(3, results.size());
    assertEquals(kvs[4], results.get(0));
    assertEquals(kvs[5], results.get(1));
    assertEquals(kvs[6], results.get(2));
    results.clear();

    assertEquals(false, scanner.next(results));
  }

  public void testScannerReseekDoesntNPE() throws Exception {
    List<KeyValueScanner> scanners = scanFixture(kvs);
    StoreScanner scan = new StoreScanner(new Scan(), scanInfo, scanType,
        getCols("a", "d"), scanners);

    // Previously a updateReaders twice in a row would cause an NPE.  In test this would also
    // normally cause an NPE because scan.store is null.  So as long as we get through these
    // two calls we are good and the bug was quashed.

    scan.updateReaders();

    scan.updateReaders();

    scan.peek();
  }


  /**
   * TODO this fails, since we don't handle deletions, etc, in peek
   */
  public void SKIP_testPeek() throws Exception {
    KeyValue [] kvs = new KeyValue [] {
        KeyValueTestUtil.create("R1", "cf", "a", 1, KeyValue.Type.Put, "dont-care"),
        KeyValueTestUtil.create("R1", "cf", "a", 1, KeyValue.Type.Delete, "dont-care"),
    };
    List<KeyValueScanner> scanners = scanFixture(kvs);
    Scan scanSpec = new Scan(Bytes.toBytes("R1"));
    StoreScanner scan = new StoreScanner(scanSpec, scanInfo, scanType,
        getCols("a"), scanners);
    assertNull(scan.peek());
  }

  /**
   * Ensure that expired delete family markers don't override valid puts
   */
  public void testExpiredDeleteFamily() throws Exception {
    long now = System.currentTimeMillis();
    KeyValue [] kvs = new KeyValue[] {
        new KeyValue(Bytes.toBytes("R1"), Bytes.toBytes("cf"), null, now-1000,
            KeyValue.Type.DeleteFamily),
        KeyValueTestUtil.create("R1", "cf", "a", now-10, KeyValue.Type.Put,
            "dont-care"),
    };
    List<KeyValueScanner> scanners = scanFixture(kvs);
    Scan scan = new Scan();
    scan.setMaxVersions(1);
    // scanner with ttl equal to 500
    ScanInfo scanInfo = new ScanInfo(CF, 0, 1, 500, KeepDeletedCells.FALSE, 0,
        CellComparator.COMPARATOR);
    ScanType scanType = ScanType.USER_SCAN;
    StoreScanner scanner =
        new StoreScanner(scan, scanInfo, scanType, null, scanners);

    List<Cell> results = new ArrayList<Cell>();
    assertEquals(true, scanner.next(results));
    assertEquals(1, results.size());
    assertEquals(kvs[1], results.get(0));
    results.clear();

    assertEquals(false, scanner.next(results));
  }

  public void testDeleteMarkerLongevity() throws Exception {
    try {
      final long now = System.currentTimeMillis();
      EnvironmentEdgeManagerTestHelper.injectEdge(new EnvironmentEdge() {
        public long currentTime() {
          return now;
        }
      });
      KeyValue[] kvs = new KeyValue[]{
        /*0*/ new KeyValue(Bytes.toBytes("R1"), Bytes.toBytes("cf"), null,
        now - 100, KeyValue.Type.DeleteFamily), // live
        /*1*/ new KeyValue(Bytes.toBytes("R1"), Bytes.toBytes("cf"), null,
        now - 1000, KeyValue.Type.DeleteFamily), // expired
        /*2*/ KeyValueTestUtil.create("R1", "cf", "a", now - 50,
        KeyValue.Type.Put, "v3"), // live
        /*3*/ KeyValueTestUtil.create("R1", "cf", "a", now - 55,
        KeyValue.Type.Delete, "dontcare"), // live
        /*4*/ KeyValueTestUtil.create("R1", "cf", "a", now - 55,
        KeyValue.Type.Put, "deleted-version v2"), // deleted
        /*5*/ KeyValueTestUtil.create("R1", "cf", "a", now - 60,
        KeyValue.Type.Put, "v1"), // live
        /*6*/ KeyValueTestUtil.create("R1", "cf", "a", now - 65,
        KeyValue.Type.Put, "v0"), // max-version reached
        /*7*/ KeyValueTestUtil.create("R1", "cf", "a",
        now - 100, KeyValue.Type.DeleteColumn, "dont-care"), // max-version
        /*8*/ KeyValueTestUtil.create("R1", "cf", "b", now - 600,
        KeyValue.Type.DeleteColumn, "dont-care"), //expired
        /*9*/ KeyValueTestUtil.create("R1", "cf", "b", now - 70,
        KeyValue.Type.Put, "v2"), //live
        /*10*/ KeyValueTestUtil.create("R1", "cf", "b", now - 750,
        KeyValue.Type.Put, "v1"), //expired
        /*11*/ KeyValueTestUtil.create("R1", "cf", "c", now - 500,
        KeyValue.Type.Delete, "dontcare"), //expired
        /*12*/ KeyValueTestUtil.create("R1", "cf", "c", now - 600,
        KeyValue.Type.Put, "v1"), //expired
        /*13*/ KeyValueTestUtil.create("R1", "cf", "c", now - 1000,
        KeyValue.Type.Delete, "dontcare"), //expired
        /*14*/ KeyValueTestUtil.create("R1", "cf", "d", now - 60,
        KeyValue.Type.Put, "expired put"), //live
        /*15*/ KeyValueTestUtil.create("R1", "cf", "d", now - 100,
        KeyValue.Type.Delete, "not-expired delete"), //live
      };
      List<KeyValueScanner> scanners = scanFixture(kvs);
      Scan scan = new Scan();
      scan.setMaxVersions(2);
      ScanInfo scanInfo = new ScanInfo(Bytes.toBytes("cf"),
        0 /* minVersions */,
        2 /* maxVersions */, 500 /* ttl */,
        KeepDeletedCells.FALSE /* keepDeletedCells */,
        200, /* timeToPurgeDeletes */
        CellComparator.COMPARATOR);
      StoreScanner scanner =
        new StoreScanner(scan, scanInfo,
          ScanType.COMPACT_DROP_DELETES, null, scanners,
          HConstants.OLDEST_TIMESTAMP);
      List<Cell> results = new ArrayList<Cell>();
      results = new ArrayList<Cell>();
      assertEquals(true, scanner.next(results));
      assertEquals(kvs[0], results.get(0));
      assertEquals(kvs[2], results.get(1));
      assertEquals(kvs[3], results.get(2));
      assertEquals(kvs[5], results.get(3));
      assertEquals(kvs[9], results.get(4));
      assertEquals(kvs[14], results.get(5));
      assertEquals(kvs[15], results.get(6));
      assertEquals(7, results.size());
      scanner.close();
    }finally{
    EnvironmentEdgeManagerTestHelper.reset();
    }
  }

}

