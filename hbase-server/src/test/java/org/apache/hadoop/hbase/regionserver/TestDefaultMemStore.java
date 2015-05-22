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

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeepDeletedCells;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueTestUtil;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.TableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdge;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.junit.experimental.categories.Category;

import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

/** memstore test case */
@Category({RegionServerTests.class, MediumTests.class})
public class TestDefaultMemStore extends TestCase {
  private static final Log LOG = LogFactory.getLog(TestDefaultMemStore.class);
  private DefaultMemStore memstore;
  private static final int ROW_COUNT = 10;
  private static final int QUALIFIER_COUNT = ROW_COUNT;
  private static final byte [] FAMILY = Bytes.toBytes("column");
  private MultiVersionConsistencyControl mvcc;
  private AtomicLong startSeqNum = new AtomicLong(0); 

  @Override
  public void setUp() throws Exception {
    super.setUp();
    this.mvcc = new MultiVersionConsistencyControl();
    this.memstore = new DefaultMemStore();
  }

  public void testPutSameKey() {
    byte [] bytes = Bytes.toBytes(getName());
    KeyValue kv = new KeyValue(bytes, bytes, bytes, bytes);
    this.memstore.add(kv);
    byte [] other = Bytes.toBytes("somethingelse");
    KeyValue samekey = new KeyValue(bytes, bytes, bytes, other);
    this.memstore.add(samekey);
    Cell found = this.memstore.cellSet.first();
    assertEquals(1, this.memstore.cellSet.size());
    assertTrue(Bytes.toString(found.getValue()), CellUtil.matchingValue(samekey, found));
  }

  /**
   * Test memstore snapshot happening while scanning.
   * @throws IOException
   */
  public void testScanAcrossSnapshot() throws IOException {
    int rowCount = addRows(this.memstore);
    List<KeyValueScanner> memstorescanners = this.memstore.getScanners(0);
    Scan scan = new Scan();
    List<Cell> result = new ArrayList<Cell>();
    ScanInfo scanInfo =
        new ScanInfo(null, 0, 1, HConstants.LATEST_TIMESTAMP, KeepDeletedCells.FALSE, 0,
            this.memstore.comparator);
    ScanType scanType = ScanType.USER_SCAN;
    StoreScanner s = new StoreScanner(scan, scanInfo, scanType, null, memstorescanners);
    int count = 0;
    try {
      while (s.next(result)) {
        LOG.info(result);
        count++;
        // Row count is same as column count.
        assertEquals(rowCount, result.size());
        result.clear();
      }
    } finally {
      s.close();
    }
    assertEquals(rowCount, count);
    for (KeyValueScanner scanner : memstorescanners) {
      scanner.close();
    }

    memstorescanners = this.memstore.getScanners(mvcc.memstoreReadPoint());
    // Now assert can count same number even if a snapshot mid-scan.
    s = new StoreScanner(scan, scanInfo, scanType, null, memstorescanners);
    count = 0;
    try {
      while (s.next(result)) {
        LOG.info(result);
        // Assert the stuff is coming out in right order.
        assertTrue(CellUtil.matchingRow(result.get(0), Bytes.toBytes(count)));
        count++;
        // Row count is same as column count.
        assertEquals(rowCount, result.size());
        if (count == 2) {
          this.memstore.snapshot();
          LOG.info("Snapshotted");
        }
        result.clear();
      }
    } finally {
      s.close();
    }
    assertEquals(rowCount, count);
    for (KeyValueScanner scanner : memstorescanners) {
      scanner.close();
    }
    memstorescanners = this.memstore.getScanners(mvcc.memstoreReadPoint());
    // Assert that new values are seen in kvset as we scan.
    long ts = System.currentTimeMillis();
    s = new StoreScanner(scan, scanInfo, scanType, null, memstorescanners);
    count = 0;
    int snapshotIndex = 5;
    try {
      while (s.next(result)) {
        LOG.info(result);
        // Assert the stuff is coming out in right order.
        assertTrue(CellUtil.matchingRow(result.get(0), Bytes.toBytes(count)));
        // Row count is same as column count.
        assertEquals("count=" + count + ", result=" + result, rowCount, result.size());
        count++;
        if (count == snapshotIndex) {
          MemStoreSnapshot snapshot = this.memstore.snapshot();
          this.memstore.clearSnapshot(snapshot.getId());
          // Added more rows into kvset.  But the scanner wont see these rows.
          addRows(this.memstore, ts);
          LOG.info("Snapshotted, cleared it and then added values (which wont be seen)");
        }
        result.clear();
      }
    } finally {
      s.close();
    }
    assertEquals(rowCount, count);
  }

  /**
   * A simple test which verifies the 3 possible states when scanning across snapshot.
   * @throws IOException
   * @throws CloneNotSupportedException 
   */
  public void testScanAcrossSnapshot2() throws IOException, CloneNotSupportedException {
    // we are going to the scanning across snapshot with two kvs
    // kv1 should always be returned before kv2
    final byte[] one = Bytes.toBytes(1);
    final byte[] two = Bytes.toBytes(2);
    final byte[] f = Bytes.toBytes("f");
    final byte[] q = Bytes.toBytes("q");
    final byte[] v = Bytes.toBytes(3);

    final KeyValue kv1 = new KeyValue(one, f, q, v);
    final KeyValue kv2 = new KeyValue(two, f, q, v);

    // use case 1: both kvs in kvset
    this.memstore.add(kv1.clone());
    this.memstore.add(kv2.clone());
    verifyScanAcrossSnapshot2(kv1, kv2);

    // use case 2: both kvs in snapshot
    this.memstore.snapshot();
    verifyScanAcrossSnapshot2(kv1, kv2);

    // use case 3: first in snapshot second in kvset
    this.memstore = new DefaultMemStore();
    this.memstore.add(kv1.clone());
    this.memstore.snapshot();
    this.memstore.add(kv2.clone());
    verifyScanAcrossSnapshot2(kv1, kv2);
  }

  private void verifyScanAcrossSnapshot2(KeyValue kv1, KeyValue kv2)
      throws IOException {
    List<KeyValueScanner> memstorescanners = this.memstore.getScanners(mvcc.memstoreReadPoint());
    assertEquals(1, memstorescanners.size());
    final KeyValueScanner scanner = memstorescanners.get(0);
    scanner.seek(KeyValueUtil.createFirstOnRow(HConstants.EMPTY_START_ROW));
    assertEquals(kv1, scanner.next());
    assertEquals(kv2, scanner.next());
    assertNull(scanner.next());
  }

  private void assertScannerResults(KeyValueScanner scanner, KeyValue[] expected)
      throws IOException {
    scanner.seek(KeyValueUtil.createFirstOnRow(new byte[]{}));
    List<Cell> returned = Lists.newArrayList();

    while (true) {
      Cell next = scanner.next();
      if (next == null) break;
      returned.add(next);
    }

    assertTrue(
        "Got:\n" + Joiner.on("\n").join(returned) +
        "\nExpected:\n" + Joiner.on("\n").join(expected),
        Iterables.elementsEqual(Arrays.asList(expected), returned));
    assertNull(scanner.peek());
  }

  public void testMemstoreConcurrentControl() throws IOException {
    final byte[] row = Bytes.toBytes(1);
    final byte[] f = Bytes.toBytes("family");
    final byte[] q1 = Bytes.toBytes("q1");
    final byte[] q2 = Bytes.toBytes("q2");
    final byte[] v = Bytes.toBytes("value");

    MultiVersionConsistencyControl.WriteEntry w =
        mvcc.beginMemstoreInsertWithSeqNum(this.startSeqNum.incrementAndGet());

    KeyValue kv1 = new KeyValue(row, f, q1, v);
    kv1.setSequenceId(w.getWriteNumber());
    memstore.add(kv1);

    KeyValueScanner s = this.memstore.getScanners(mvcc.memstoreReadPoint()).get(0);
    assertScannerResults(s, new KeyValue[]{});

    mvcc.completeMemstoreInsert(w);

    s = this.memstore.getScanners(mvcc.memstoreReadPoint()).get(0);
    assertScannerResults(s, new KeyValue[]{kv1});

    w = mvcc.beginMemstoreInsertWithSeqNum(this.startSeqNum.incrementAndGet());
    KeyValue kv2 = new KeyValue(row, f, q2, v);
    kv2.setSequenceId(w.getWriteNumber());
    memstore.add(kv2);

    s = this.memstore.getScanners(mvcc.memstoreReadPoint()).get(0);
    assertScannerResults(s, new KeyValue[]{kv1});

    mvcc.completeMemstoreInsert(w);

    s = this.memstore.getScanners(mvcc.memstoreReadPoint()).get(0);
    assertScannerResults(s, new KeyValue[]{kv1, kv2});
  }

  /**
   * Regression test for HBASE-2616, HBASE-2670.
   * When we insert a higher-memstoreTS version of a cell but with
   * the same timestamp, we still need to provide consistent reads
   * for the same scanner.
   */
  public void testMemstoreEditsVisibilityWithSameKey() throws IOException {
    final byte[] row = Bytes.toBytes(1);
    final byte[] f = Bytes.toBytes("family");
    final byte[] q1 = Bytes.toBytes("q1");
    final byte[] q2 = Bytes.toBytes("q2");
    final byte[] v1 = Bytes.toBytes("value1");
    final byte[] v2 = Bytes.toBytes("value2");

    // INSERT 1: Write both columns val1
    MultiVersionConsistencyControl.WriteEntry w =
        mvcc.beginMemstoreInsertWithSeqNum(this.startSeqNum.incrementAndGet());

    KeyValue kv11 = new KeyValue(row, f, q1, v1);
    kv11.setSequenceId(w.getWriteNumber());
    memstore.add(kv11);

    KeyValue kv12 = new KeyValue(row, f, q2, v1);
    kv12.setSequenceId(w.getWriteNumber());
    memstore.add(kv12);
    mvcc.completeMemstoreInsert(w);

    // BEFORE STARTING INSERT 2, SEE FIRST KVS
    KeyValueScanner s = this.memstore.getScanners(mvcc.memstoreReadPoint()).get(0);
    assertScannerResults(s, new KeyValue[]{kv11, kv12});

    // START INSERT 2: Write both columns val2
    w = mvcc.beginMemstoreInsertWithSeqNum(this.startSeqNum.incrementAndGet());
    KeyValue kv21 = new KeyValue(row, f, q1, v2);
    kv21.setSequenceId(w.getWriteNumber());
    memstore.add(kv21);

    KeyValue kv22 = new KeyValue(row, f, q2, v2);
    kv22.setSequenceId(w.getWriteNumber());
    memstore.add(kv22);

    // BEFORE COMPLETING INSERT 2, SEE FIRST KVS
    s = this.memstore.getScanners(mvcc.memstoreReadPoint()).get(0);
    assertScannerResults(s, new KeyValue[]{kv11, kv12});

    // COMPLETE INSERT 2
    mvcc.completeMemstoreInsert(w);

    // NOW SHOULD SEE NEW KVS IN ADDITION TO OLD KVS.
    // See HBASE-1485 for discussion about what we should do with
    // the duplicate-TS inserts
    s = this.memstore.getScanners(mvcc.memstoreReadPoint()).get(0);
    assertScannerResults(s, new KeyValue[]{kv21, kv11, kv22, kv12});
  }

  /**
   * When we insert a higher-memstoreTS deletion of a cell but with
   * the same timestamp, we still need to provide consistent reads
   * for the same scanner.
   */
  public void testMemstoreDeletesVisibilityWithSameKey() throws IOException {
    final byte[] row = Bytes.toBytes(1);
    final byte[] f = Bytes.toBytes("family");
    final byte[] q1 = Bytes.toBytes("q1");
    final byte[] q2 = Bytes.toBytes("q2");
    final byte[] v1 = Bytes.toBytes("value1");
    // INSERT 1: Write both columns val1
    MultiVersionConsistencyControl.WriteEntry w =
        mvcc.beginMemstoreInsertWithSeqNum(this.startSeqNum.incrementAndGet());

    KeyValue kv11 = new KeyValue(row, f, q1, v1);
    kv11.setSequenceId(w.getWriteNumber());
    memstore.add(kv11);

    KeyValue kv12 = new KeyValue(row, f, q2, v1);
    kv12.setSequenceId(w.getWriteNumber());
    memstore.add(kv12);
    mvcc.completeMemstoreInsert(w);

    // BEFORE STARTING INSERT 2, SEE FIRST KVS
    KeyValueScanner s = this.memstore.getScanners(mvcc.memstoreReadPoint()).get(0);
    assertScannerResults(s, new KeyValue[]{kv11, kv12});

    // START DELETE: Insert delete for one of the columns
    w = mvcc.beginMemstoreInsertWithSeqNum(this.startSeqNum.incrementAndGet());
    KeyValue kvDel = new KeyValue(row, f, q2, kv11.getTimestamp(),
        KeyValue.Type.DeleteColumn);
    kvDel.setSequenceId(w.getWriteNumber());
    memstore.add(kvDel);

    // BEFORE COMPLETING DELETE, SEE FIRST KVS
    s = this.memstore.getScanners(mvcc.memstoreReadPoint()).get(0);
    assertScannerResults(s, new KeyValue[]{kv11, kv12});

    // COMPLETE DELETE
    mvcc.completeMemstoreInsert(w);

    // NOW WE SHOULD SEE DELETE
    s = this.memstore.getScanners(mvcc.memstoreReadPoint()).get(0);
    assertScannerResults(s, new KeyValue[]{kv11, kvDel, kv12});
  }


  private static class ReadOwnWritesTester extends Thread {
    static final int NUM_TRIES = 1000;

    final byte[] row;

    final byte[] f = Bytes.toBytes("family");
    final byte[] q1 = Bytes.toBytes("q1");

    final MultiVersionConsistencyControl mvcc;
    final MemStore memstore;
    final AtomicLong startSeqNum;

    AtomicReference<Throwable> caughtException;


    public ReadOwnWritesTester(int id,
                               MemStore memstore,
                               MultiVersionConsistencyControl mvcc,
                               AtomicReference<Throwable> caughtException,
                               AtomicLong startSeqNum)
    {
      this.mvcc = mvcc;
      this.memstore = memstore;
      this.caughtException = caughtException;
      row = Bytes.toBytes(id);
      this.startSeqNum = startSeqNum;
    }

    public void run() {
      try {
        internalRun();
      } catch (Throwable t) {
        caughtException.compareAndSet(null, t);
      }
    }

    private void internalRun() throws IOException {
      for (long i = 0; i < NUM_TRIES && caughtException.get() == null; i++) {
        MultiVersionConsistencyControl.WriteEntry w =
            mvcc.beginMemstoreInsertWithSeqNum(this.startSeqNum.incrementAndGet());

        // Insert the sequence value (i)
        byte[] v = Bytes.toBytes(i);

        KeyValue kv = new KeyValue(row, f, q1, i, v);
        kv.setSequenceId(w.getWriteNumber());
        memstore.add(kv);
        mvcc.completeMemstoreInsert(w);

        // Assert that we can read back
        KeyValueScanner s = this.memstore.getScanners(mvcc.memstoreReadPoint()).get(0);
        s.seek(kv);

        Cell ret = s.next();
        assertNotNull("Didnt find own write at all", ret);
        assertEquals("Didnt read own writes",
                     kv.getTimestamp(), ret.getTimestamp());
      }
    }
  }

  public void testReadOwnWritesUnderConcurrency() throws Throwable {

    int NUM_THREADS = 8;

    ReadOwnWritesTester threads[] = new ReadOwnWritesTester[NUM_THREADS];
    AtomicReference<Throwable> caught = new AtomicReference<Throwable>();

    for (int i = 0; i < NUM_THREADS; i++) {
      threads[i] = new ReadOwnWritesTester(i, memstore, mvcc, caught, this.startSeqNum);
      threads[i].start();
    }

    for (int i = 0; i < NUM_THREADS; i++) {
      threads[i].join();
    }

    if (caught.get() != null) {
      throw caught.get();
    }
  }

  /**
   * Test memstore snapshots
   * @throws IOException
   */
  public void testSnapshotting() throws IOException {
    final int snapshotCount = 5;
    // Add some rows, run a snapshot. Do it a few times.
    for (int i = 0; i < snapshotCount; i++) {
      addRows(this.memstore);
      runSnapshot(this.memstore);
      assertEquals("History not being cleared", 0, this.memstore.snapshot.size());
    }
  }

  public void testMultipleVersionsSimple() throws Exception {
    DefaultMemStore m = new DefaultMemStore(new Configuration(), CellComparator.COMPARATOR);
    byte [] row = Bytes.toBytes("testRow");
    byte [] family = Bytes.toBytes("testFamily");
    byte [] qf = Bytes.toBytes("testQualifier");
    long [] stamps = {1,2,3};
    byte [][] values = {Bytes.toBytes("value0"), Bytes.toBytes("value1"),
        Bytes.toBytes("value2")};
    KeyValue key0 = new KeyValue(row, family, qf, stamps[0], values[0]);
    KeyValue key1 = new KeyValue(row, family, qf, stamps[1], values[1]);
    KeyValue key2 = new KeyValue(row, family, qf, stamps[2], values[2]);

    m.add(key0);
    m.add(key1);
    m.add(key2);

    assertTrue("Expected memstore to hold 3 values, actually has " +
        m.cellSet.size(), m.cellSet.size() == 3);
  }

  //////////////////////////////////////////////////////////////////////////////
  // Get tests
  //////////////////////////////////////////////////////////////////////////////

  /** Test getNextRow from memstore
   * @throws InterruptedException
   */
  public void testGetNextRow() throws Exception {
    addRows(this.memstore);
    // Add more versions to make it a little more interesting.
    Thread.sleep(1);
    addRows(this.memstore);
    Cell closestToEmpty = this.memstore.getNextRow(KeyValue.LOWESTKEY);
    assertTrue(CellComparator.COMPARATOR.compareRows(closestToEmpty,
      new KeyValue(Bytes.toBytes(0), System.currentTimeMillis())) == 0);
    for (int i = 0; i < ROW_COUNT; i++) {
      Cell nr = this.memstore.getNextRow(new KeyValue(Bytes.toBytes(i),
        System.currentTimeMillis()));
      if (i + 1 == ROW_COUNT) {
        assertEquals(nr, null);
      } else {
        assertTrue(CellComparator.COMPARATOR.compareRows(nr,
          new KeyValue(Bytes.toBytes(i + 1), System.currentTimeMillis())) == 0);
      }
    }
    //starting from each row, validate results should contain the starting row
    for (int startRowId = 0; startRowId < ROW_COUNT; startRowId++) {
      ScanInfo scanInfo = new ScanInfo(FAMILY, 0, 1, Integer.MAX_VALUE, KeepDeletedCells.FALSE,
          0, this.memstore.comparator);
      ScanType scanType = ScanType.USER_SCAN;
      InternalScanner scanner = new StoreScanner(new Scan(
          Bytes.toBytes(startRowId)), scanInfo, scanType, null,
          memstore.getScanners(0));
      List<Cell> results = new ArrayList<Cell>();
      for (int i = 0; scanner.next(results); i++) {
        int rowId = startRowId + i;
        Cell left = results.get(0);
        byte[] row1 = Bytes.toBytes(rowId);
        assertTrue(
            "Row name",
            CellComparator.COMPARATOR.compareRows(left, row1, 0, row1.length) == 0);
        assertEquals("Count of columns", QUALIFIER_COUNT, results.size());
        List<Cell> row = new ArrayList<Cell>();
        for (Cell kv : results) {
          row.add(kv);
        }
        isExpectedRowWithoutTimestamps(rowId, row);
        // Clear out set.  Otherwise row results accumulate.
        results.clear();
      }
    }
  }

  public void testGet_memstoreAndSnapShot() throws IOException {
    byte [] row = Bytes.toBytes("testrow");
    byte [] fam = Bytes.toBytes("testfamily");
    byte [] qf1 = Bytes.toBytes("testqualifier1");
    byte [] qf2 = Bytes.toBytes("testqualifier2");
    byte [] qf3 = Bytes.toBytes("testqualifier3");
    byte [] qf4 = Bytes.toBytes("testqualifier4");
    byte [] qf5 = Bytes.toBytes("testqualifier5");
    byte [] val = Bytes.toBytes("testval");

    //Setting up memstore
    memstore.add(new KeyValue(row, fam ,qf1, val));
    memstore.add(new KeyValue(row, fam ,qf2, val));
    memstore.add(new KeyValue(row, fam ,qf3, val));
    //Creating a snapshot
    memstore.snapshot();
    assertEquals(3, memstore.snapshot.size());
    //Adding value to "new" memstore
    assertEquals(0, memstore.cellSet.size());
    memstore.add(new KeyValue(row, fam ,qf4, val));
    memstore.add(new KeyValue(row, fam ,qf5, val));
    assertEquals(2, memstore.cellSet.size());
  }

  //////////////////////////////////////////////////////////////////////////////
  // Delete tests
  //////////////////////////////////////////////////////////////////////////////
  public void testGetWithDelete() throws IOException {
    byte [] row = Bytes.toBytes("testrow");
    byte [] fam = Bytes.toBytes("testfamily");
    byte [] qf1 = Bytes.toBytes("testqualifier");
    byte [] val = Bytes.toBytes("testval");

    long ts1 = System.nanoTime();
    KeyValue put1 = new KeyValue(row, fam, qf1, ts1, val);
    long ts2 = ts1 + 1;
    KeyValue put2 = new KeyValue(row, fam, qf1, ts2, val);
    long ts3 = ts2 +1;
    KeyValue put3 = new KeyValue(row, fam, qf1, ts3, val);
    memstore.add(put1);
    memstore.add(put2);
    memstore.add(put3);

    assertEquals(3, memstore.cellSet.size());

    KeyValue del2 = new KeyValue(row, fam, qf1, ts2, KeyValue.Type.Delete, val);
    memstore.delete(del2);

    List<Cell> expected = new ArrayList<Cell>();
    expected.add(put3);
    expected.add(del2);
    expected.add(put2);
    expected.add(put1);

    assertEquals(4, memstore.cellSet.size());
    int i = 0;
    for(Cell cell : memstore.cellSet) {
      assertEquals(expected.get(i++), cell);
    }
  }

  public void testGetWithDeleteColumn() throws IOException {
    byte [] row = Bytes.toBytes("testrow");
    byte [] fam = Bytes.toBytes("testfamily");
    byte [] qf1 = Bytes.toBytes("testqualifier");
    byte [] val = Bytes.toBytes("testval");

    long ts1 = System.nanoTime();
    KeyValue put1 = new KeyValue(row, fam, qf1, ts1, val);
    long ts2 = ts1 + 1;
    KeyValue put2 = new KeyValue(row, fam, qf1, ts2, val);
    long ts3 = ts2 +1;
    KeyValue put3 = new KeyValue(row, fam, qf1, ts3, val);
    memstore.add(put1);
    memstore.add(put2);
    memstore.add(put3);

    assertEquals(3, memstore.cellSet.size());

    KeyValue del2 =
      new KeyValue(row, fam, qf1, ts2, KeyValue.Type.DeleteColumn, val);
    memstore.delete(del2);

    List<Cell> expected = new ArrayList<Cell>();
    expected.add(put3);
    expected.add(del2);
    expected.add(put2);
    expected.add(put1);


    assertEquals(4, memstore.cellSet.size());
    int i = 0;
    for (Cell cell: memstore.cellSet) {
      assertEquals(expected.get(i++), cell);
    }
  }


  public void testGetWithDeleteFamily() throws IOException {
    byte [] row = Bytes.toBytes("testrow");
    byte [] fam = Bytes.toBytes("testfamily");
    byte [] qf1 = Bytes.toBytes("testqualifier1");
    byte [] qf2 = Bytes.toBytes("testqualifier2");
    byte [] qf3 = Bytes.toBytes("testqualifier3");
    byte [] val = Bytes.toBytes("testval");
    long ts = System.nanoTime();

    KeyValue put1 = new KeyValue(row, fam, qf1, ts, val);
    KeyValue put2 = new KeyValue(row, fam, qf2, ts, val);
    KeyValue put3 = new KeyValue(row, fam, qf3, ts, val);
    KeyValue put4 = new KeyValue(row, fam, qf3, ts+1, val);

    memstore.add(put1);
    memstore.add(put2);
    memstore.add(put3);
    memstore.add(put4);

    KeyValue del =
      new KeyValue(row, fam, null, ts, KeyValue.Type.DeleteFamily, val);
    memstore.delete(del);

    List<Cell> expected = new ArrayList<Cell>();
    expected.add(del);
    expected.add(put1);
    expected.add(put2);
    expected.add(put4);
    expected.add(put3);



    assertEquals(5, memstore.cellSet.size());
    int i = 0;
    for (Cell cell: memstore.cellSet) {
      assertEquals(expected.get(i++), cell);
    }
  }

  public void testKeepDeleteInmemstore() {
    byte [] row = Bytes.toBytes("testrow");
    byte [] fam = Bytes.toBytes("testfamily");
    byte [] qf = Bytes.toBytes("testqualifier");
    byte [] val = Bytes.toBytes("testval");
    long ts = System.nanoTime();
    memstore.add(new KeyValue(row, fam, qf, ts, val));
    KeyValue delete = new KeyValue(row, fam, qf, ts, KeyValue.Type.Delete, val);
    memstore.delete(delete);
    assertEquals(2, memstore.cellSet.size());
    assertEquals(delete, memstore.cellSet.first());
  }

  public void testRetainsDeleteVersion() throws IOException {
    // add a put to memstore
    memstore.add(KeyValueTestUtil.create("row1", "fam", "a", 100, "dont-care"));

    // now process a specific delete:
    KeyValue delete = KeyValueTestUtil.create(
        "row1", "fam", "a", 100, KeyValue.Type.Delete, "dont-care");
    memstore.delete(delete);

    assertEquals(2, memstore.cellSet.size());
    assertEquals(delete, memstore.cellSet.first());
  }
  public void testRetainsDeleteColumn() throws IOException {
    // add a put to memstore
    memstore.add(KeyValueTestUtil.create("row1", "fam", "a", 100, "dont-care"));

    // now process a specific delete:
    KeyValue delete = KeyValueTestUtil.create("row1", "fam", "a", 100,
        KeyValue.Type.DeleteColumn, "dont-care");
    memstore.delete(delete);

    assertEquals(2, memstore.cellSet.size());
    assertEquals(delete, memstore.cellSet.first());
  }
  public void testRetainsDeleteFamily() throws IOException {
    // add a put to memstore
    memstore.add(KeyValueTestUtil.create("row1", "fam", "a", 100, "dont-care"));

    // now process a specific delete:
    KeyValue delete = KeyValueTestUtil.create("row1", "fam", "a", 100,
        KeyValue.Type.DeleteFamily, "dont-care");
    memstore.delete(delete);

    assertEquals(2, memstore.cellSet.size());
    assertEquals(delete, memstore.cellSet.first());
  }

  ////////////////////////////////////
  //Test for timestamps
  ////////////////////////////////////

  /**
   * Test to ensure correctness when using Memstore with multiple timestamps
   */
  public void testMultipleTimestamps() throws IOException {
    long[] timestamps = new long[] {20,10,5,1};
    Scan scan = new Scan();

    for (long timestamp: timestamps)
      addRows(memstore,timestamp);

    scan.setTimeRange(0, 2);
    assertTrue(memstore.shouldSeek(scan, Long.MIN_VALUE));

    scan.setTimeRange(20, 82);
    assertTrue(memstore.shouldSeek(scan, Long.MIN_VALUE));

    scan.setTimeRange(10, 20);
    assertTrue(memstore.shouldSeek(scan, Long.MIN_VALUE));

    scan.setTimeRange(8, 12);
    assertTrue(memstore.shouldSeek(scan, Long.MIN_VALUE));

    /*This test is not required for correctness but it should pass when
     * timestamp range optimization is on*/
    //scan.setTimeRange(28, 42);
    //assertTrue(!memstore.shouldSeek(scan));
  }

  ////////////////////////////////////
  //Test for upsert with MSLAB
  ////////////////////////////////////

  /**
   * Test a pathological pattern that shows why we can't currently
   * use the MSLAB for upsert workloads. This test inserts data
   * in the following pattern:
   *
   * - row0001 through row1000 (fills up one 2M Chunk)
   * - row0002 through row1001 (fills up another 2M chunk, leaves one reference
   *   to the first chunk
   * - row0003 through row1002 (another chunk, another dangling reference)
   *
   * This causes OOME pretty quickly if we use MSLAB for upsert
   * since each 2M chunk is held onto by a single reference.
   */
  public void testUpsertMSLAB() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    conf.setBoolean(DefaultMemStore.USEMSLAB_KEY, true);
    memstore = new DefaultMemStore(conf, CellComparator.COMPARATOR);

    int ROW_SIZE = 2048;
    byte[] qualifier = new byte[ROW_SIZE - 4];

    MemoryMXBean bean = ManagementFactory.getMemoryMXBean();
    for (int i = 0; i < 3; i++) { System.gc(); }
    long usageBefore = bean.getHeapMemoryUsage().getUsed();

    long size = 0;
    long ts=0;

    for (int newValue = 0; newValue < 1000; newValue++) {
      for (int row = newValue; row < newValue + 1000; row++) {
        byte[] rowBytes = Bytes.toBytes(row);
        size += memstore.updateColumnValue(rowBytes, FAMILY, qualifier, newValue, ++ts);
      }
    }
    System.out.println("Wrote " + ts + " vals");
    for (int i = 0; i < 3; i++) { System.gc(); }
    long usageAfter = bean.getHeapMemoryUsage().getUsed();
    System.out.println("Memory used: " + (usageAfter - usageBefore)
        + " (heapsize: " + memstore.heapSize() +
        " size: " + size + ")");
  }

  //////////////////////////////////////////////////////////////////////////////
  // Helpers
  //////////////////////////////////////////////////////////////////////////////
  private static byte [] makeQualifier(final int i1, final int i2){
    return Bytes.toBytes(Integer.toString(i1) + ";" +
        Integer.toString(i2));
  }

  /**
   * Add keyvalues with a fixed memstoreTs, and checks that memstore size is decreased
   * as older keyvalues are deleted from the memstore.
   * @throws Exception
   */
  public void testUpsertMemstoreSize() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    memstore = new DefaultMemStore(conf, CellComparator.COMPARATOR);
    long oldSize = memstore.size.get();

    List<Cell> l = new ArrayList<Cell>();
    KeyValue kv1 = KeyValueTestUtil.create("r", "f", "q", 100, "v");
    KeyValue kv2 = KeyValueTestUtil.create("r", "f", "q", 101, "v");
    KeyValue kv3 = KeyValueTestUtil.create("r", "f", "q", 102, "v");

    kv1.setSequenceId(1); kv2.setSequenceId(1);kv3.setSequenceId(1);
    l.add(kv1); l.add(kv2); l.add(kv3);

    this.memstore.upsert(l, 2);// readpoint is 2
    long newSize = this.memstore.size.get();
    assert(newSize > oldSize);
    //The kv1 should be removed.
    assert(memstore.cellSet.size() == 2);
    
    KeyValue kv4 = KeyValueTestUtil.create("r", "f", "q", 104, "v");
    kv4.setSequenceId(1);
    l.clear(); l.add(kv4);
    this.memstore.upsert(l, 3);
    assertEquals(newSize, this.memstore.size.get());
    //The kv2 should be removed.
    assert(memstore.cellSet.size() == 2);
    //this.memstore = null;
  }

  ////////////////////////////////////
  // Test for periodic memstore flushes 
  // based on time of oldest edit
  ////////////////////////////////////

  /**
   * Tests that the timeOfOldestEdit is updated correctly for the 
   * various edit operations in memstore.
   * @throws Exception
   */
  public void testUpdateToTimeOfOldestEdit() throws Exception {
    try {
      EnvironmentEdgeForMemstoreTest edge = new EnvironmentEdgeForMemstoreTest();
      EnvironmentEdgeManager.injectEdge(edge);
      DefaultMemStore memstore = new DefaultMemStore();
      long t = memstore.timeOfOldestEdit();
      assertEquals(t, Long.MAX_VALUE);

      // test the case that the timeOfOldestEdit is updated after a KV add
      memstore.add(KeyValueTestUtil.create("r", "f", "q", 100, "v"));
      t = memstore.timeOfOldestEdit();
      assertTrue(t == 1234);
      // snapshot() will reset timeOfOldestEdit. The method will also assert the 
      // value is reset to Long.MAX_VALUE
      t = runSnapshot(memstore);

      // test the case that the timeOfOldestEdit is updated after a KV delete
      memstore.delete(KeyValueTestUtil.create("r", "f", "q", 100, "v"));
      t = memstore.timeOfOldestEdit();
      assertTrue(t == 1234);
      t = runSnapshot(memstore);

      // test the case that the timeOfOldestEdit is updated after a KV upsert
      List<Cell> l = new ArrayList<Cell>();
      KeyValue kv1 = KeyValueTestUtil.create("r", "f", "q", 100, "v");
      kv1.setSequenceId(100);
      l.add(kv1);
      memstore.upsert(l, 1000);
      t = memstore.timeOfOldestEdit();
      assertTrue(t == 1234);
    } finally {
      EnvironmentEdgeManager.reset();
    }
  }

  /**
   * Tests the HRegion.shouldFlush method - adds an edit in the memstore
   * and checks that shouldFlush returns true, and another where it disables
   * the periodic flush functionality and tests whether shouldFlush returns
   * false. 
   * @throws Exception
   */
  public void testShouldFlush() throws Exception {
    Configuration conf = new Configuration();
    conf.setInt(HRegion.MEMSTORE_PERIODIC_FLUSH_INTERVAL, 1000);
    checkShouldFlush(conf, true);
    // test disable flush
    conf.setInt(HRegion.MEMSTORE_PERIODIC_FLUSH_INTERVAL, 0);
    checkShouldFlush(conf, false);
  }

  private void checkShouldFlush(Configuration conf, boolean expected) throws Exception {
    try {
      EnvironmentEdgeForMemstoreTest edge = new EnvironmentEdgeForMemstoreTest();
      EnvironmentEdgeManager.injectEdge(edge);
      HBaseTestingUtility hbaseUtility = HBaseTestingUtility.createLocalHTU(conf);
      HRegion region = hbaseUtility.createTestRegion("foobar", new HColumnDescriptor("foo"));

      List<Store> stores = region.getStores();
      assertTrue(stores.size() == 1);

      Store s = stores.iterator().next();
      edge.setCurrentTimeMillis(1234);
      s.add(KeyValueTestUtil.create("r", "f", "q", 100, "v"));
      edge.setCurrentTimeMillis(1234 + 100);
      StringBuffer sb = new StringBuffer();
      assertTrue(region.shouldFlush(sb) == false);
      edge.setCurrentTimeMillis(1234 + 10000);
      assertTrue(region.shouldFlush(sb) == expected);
    } finally {
      EnvironmentEdgeManager.reset();
    }
  }

  public void testShouldFlushMeta() throws Exception {
    // write an edit in the META and ensure the shouldFlush (that the periodic memstore
    // flusher invokes) returns true after META_CACHE_FLUSH_INTERVAL (even though
    // the MEMSTORE_PERIODIC_FLUSH_INTERVAL is set to a higher value)
    Configuration conf = new Configuration();
    conf.setInt(HRegion.MEMSTORE_PERIODIC_FLUSH_INTERVAL, HRegion.META_CACHE_FLUSH_INTERVAL * 10);
    HBaseTestingUtility hbaseUtility = HBaseTestingUtility.createLocalHTU(conf);
    Path testDir = hbaseUtility.getDataTestDir();
    EnvironmentEdgeForMemstoreTest edge = new EnvironmentEdgeForMemstoreTest();
    EnvironmentEdgeManager.injectEdge(edge);
    edge.setCurrentTimeMillis(1234);
    WALFactory wFactory = new WALFactory(conf, null, "1234");
    HRegion meta = HRegion.createHRegion(HRegionInfo.FIRST_META_REGIONINFO, testDir,
        conf, TableDescriptor.metaTableDescriptor(conf),
        wFactory.getMetaWAL(HRegionInfo.FIRST_META_REGIONINFO.
            getEncodedNameAsBytes()));
    HRegionInfo hri = new HRegionInfo(TableName.valueOf("testShouldFlushMeta"),
        Bytes.toBytes("row_0200"), Bytes.toBytes("row_0300"));
    HTableDescriptor desc = new HTableDescriptor(TableName.valueOf("testShouldFlushMeta"));
    desc.addFamily(new HColumnDescriptor("foo".getBytes()));
    HRegion r =
        HRegion.createHRegion(hri, testDir, conf, desc,
            wFactory.getWAL(hri.getEncodedNameAsBytes()));
    HRegion.addRegionToMETA(meta, r);
    edge.setCurrentTimeMillis(1234 + 100);
    StringBuffer sb = new StringBuffer();
    assertTrue(meta.shouldFlush(sb) == false);
    edge.setCurrentTimeMillis(edge.currentTime() + HRegion.META_CACHE_FLUSH_INTERVAL + 1);
    assertTrue(meta.shouldFlush(sb) == true);
  }

  private class EnvironmentEdgeForMemstoreTest implements EnvironmentEdge {
    long t = 1234;
    @Override
    public long currentTime() {
      return t; 
    }
    public void setCurrentTimeMillis(long t) {
      this.t = t;
    }
  }

  /**
   * Adds {@link #ROW_COUNT} rows and {@link #QUALIFIER_COUNT}
   * @param hmc Instance to add rows to.
   * @return How many rows we added.
   * @throws IOException
   */
  private int addRows(final MemStore hmc) {
    return addRows(hmc, HConstants.LATEST_TIMESTAMP);
  }

  /**
   * Adds {@link #ROW_COUNT} rows and {@link #QUALIFIER_COUNT}
   * @param hmc Instance to add rows to.
   * @return How many rows we added.
   * @throws IOException
   */
  private int addRows(final MemStore hmc, final long ts) {
    for (int i = 0; i < ROW_COUNT; i++) {
      long timestamp = ts == HConstants.LATEST_TIMESTAMP?
        System.currentTimeMillis(): ts;
      for (int ii = 0; ii < QUALIFIER_COUNT; ii++) {
        byte [] row = Bytes.toBytes(i);
        byte [] qf = makeQualifier(i, ii);
        hmc.add(new KeyValue(row, FAMILY, qf, timestamp, qf));
      }
    }
    return ROW_COUNT;
  }

  private long runSnapshot(final DefaultMemStore hmc) throws UnexpectedStateException {
    // Save off old state.
    int oldHistorySize = hmc.snapshot.size();
    MemStoreSnapshot snapshot = hmc.snapshot();
    // Make some assertions about what just happened.
    assertTrue("History size has not increased", oldHistorySize < hmc.snapshot.size());
    long t = memstore.timeOfOldestEdit();
    assertTrue("Time of oldest edit is not Long.MAX_VALUE", t == Long.MAX_VALUE);
    hmc.clearSnapshot(snapshot.getId());
    return t;
  }

  private void isExpectedRowWithoutTimestamps(final int rowIndex,
      List<Cell> kvs) {
    int i = 0;
    for (Cell kv: kvs) {
      byte[] expectedColname = makeQualifier(rowIndex, i++);
      assertTrue("Column name", CellUtil.matchingQualifier(kv, expectedColname));
      // Value is column name as bytes.  Usually result is
      // 100 bytes in size at least. This is the default size
      // for BytesWriteable.  For comparison, convert bytes to
      // String and trim to remove trailing null bytes.
      assertTrue("Content", CellUtil.matchingValue(kv, expectedColname));
    }
  }

  private static void addRows(int count, final MemStore mem) {
    long nanos = System.nanoTime();

    for (int i = 0 ; i < count ; i++) {
      if (i % 1000 == 0) {

        System.out.println(i + " Took for 1k usec: " + (System.nanoTime() - nanos)/1000);
        nanos = System.nanoTime();
      }
      long timestamp = System.currentTimeMillis();

      for (int ii = 0; ii < QUALIFIER_COUNT ; ii++) {
        byte [] row = Bytes.toBytes(i);
        byte [] qf = makeQualifier(i, ii);
        mem.add(new KeyValue(row, FAMILY, qf, timestamp, qf));
      }
    }
  }


  static void doScan(MemStore ms, int iteration) throws IOException {
    long nanos = System.nanoTime();
    KeyValueScanner s = ms.getScanners(0).get(0);
    s.seek(KeyValueUtil.createFirstOnRow(new byte[]{}));

    System.out.println(iteration + " create/seek took: " + (System.nanoTime() - nanos)/1000);
    int cnt=0;
    while(s.next() != null) ++cnt;

    System.out.println(iteration + " took usec: " + (System.nanoTime() - nanos) / 1000 + " for: "
        + cnt);

  }

  public static void main(String [] args) throws IOException {
    MemStore ms = new DefaultMemStore();

    long n1 = System.nanoTime();
    addRows(25000, ms);
    System.out.println("Took for insert: " + (System.nanoTime()-n1)/1000);

    System.out.println("foo");

    for (int i = 0 ; i < 50 ; i++)
      doScan(ms, i);
  }
}

