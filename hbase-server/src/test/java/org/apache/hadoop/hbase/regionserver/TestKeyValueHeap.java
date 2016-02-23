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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.HBaseTestCase;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CollectionBackedScanner;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({RegionServerTests.class, SmallTests.class})
public class TestKeyValueHeap extends HBaseTestCase {
  private byte[] row1 = Bytes.toBytes("row1");
  private byte[] fam1 = Bytes.toBytes("fam1");
  private byte[] col1 = Bytes.toBytes("col1");
  private byte[] data = Bytes.toBytes("data");

  private byte[] row2 = Bytes.toBytes("row2");
  private byte[] fam2 = Bytes.toBytes("fam2");
  private byte[] col2 = Bytes.toBytes("col2");

  private byte[] col3 = Bytes.toBytes("col3");
  private byte[] col4 = Bytes.toBytes("col4");
  private byte[] col5 = Bytes.toBytes("col5");

  // Variable name encoding. kv<row#><fam#><col#>
  Cell kv111 = new KeyValue(row1, fam1, col1, data);
  Cell kv112 = new KeyValue(row1, fam1, col2, data);
  Cell kv113 = new KeyValue(row1, fam1, col3, data);
  Cell kv114 = new KeyValue(row1, fam1, col4, data);
  Cell kv115 = new KeyValue(row1, fam1, col5, data);
  Cell kv121 = new KeyValue(row1, fam2, col1, data);
  Cell kv122 = new KeyValue(row1, fam2, col2, data);
  Cell kv211 = new KeyValue(row2, fam1, col1, data);
  Cell kv212 = new KeyValue(row2, fam1, col2, data);
  Cell kv213 = new KeyValue(row2, fam1, col3, data);

  TestScanner s1 = new TestScanner(Arrays.asList(kv115, kv211, kv212));
  TestScanner s2 = new TestScanner(Arrays.asList(kv111, kv112));
  TestScanner s3 = new TestScanner(Arrays.asList(kv113, kv114, kv121, kv122, kv213));

  List<KeyValueScanner> scanners = new ArrayList<KeyValueScanner>(Arrays.asList(s1, s2, s3));

  /*
   * Uses {@code scanners} to build a KeyValueHeap, iterates over it and asserts that returned
   * Cells are same as {@code expected}.
   * @return List of Cells returned from scanners.
   */
  public List<Cell> assertCells(List<Cell> expected, List<KeyValueScanner> scanners)
      throws IOException {
    //Creating KeyValueHeap
    KeyValueHeap kvh = new KeyValueHeap(scanners, CellComparator.COMPARATOR);

    List<Cell> actual = new ArrayList<>();
    while(kvh.peek() != null){
      actual.add(kvh.next());
    }

    assertEquals(expected, actual);
    return actual;
  }

  @Before
  public void setUp() throws Exception {
    super.setUp();
  }

  @Test
  public void testSorted() throws IOException{
    //Cases that need to be checked are:
    //1. The "smallest" Cell is in the same scanners as current
    //2. Current scanner gets empty

    List<Cell> expected = Arrays.asList(
        kv111, kv112, kv113, kv114, kv115, kv121, kv122, kv211, kv212, kv213);

    List<Cell> actual = assertCells(expected, scanners);

    //Check if result is sorted according to Comparator
    for(int i=0; i<actual.size()-1; i++){
      int ret = CellComparator.COMPARATOR.compare(actual.get(i), actual.get(i+1));
      assertTrue(ret < 0);
    }
  }

  @Test
  public void testSeek() throws IOException {
    //Cases:
    //1. Seek Cell that is not in scanner
    //2. Check that smallest that is returned from a seek is correct

    List<Cell> expected = Arrays.asList(kv211);

    //Creating KeyValueHeap
    KeyValueHeap kvh =
      new KeyValueHeap(scanners, CellComparator.COMPARATOR);

    Cell seekKv = new KeyValue(row2, fam1, null, null);
    kvh.seek(seekKv);

    List<Cell> actual = Arrays.asList(kvh.peek());

    assertEquals("Expected = " + Arrays.toString(expected.toArray())
        + "\n Actual = " + Arrays.toString(actual.toArray()), expected, actual);
  }

  @Test
  public void testScannerLeak() throws IOException {
    // Test for unclosed scanners (HBASE-1927)

    TestScanner s4 = new TestScanner(new ArrayList<Cell>());
    scanners.add(s4);

    //Creating KeyValueHeap
    KeyValueHeap kvh = new KeyValueHeap(scanners, CellComparator.COMPARATOR);

    while(kvh.next() != null);
    // Once the internal scanners go out of Cells, those will be removed from KVHeap's priority
    // queue and added to a Set for lazy close. The actual close will happen only on KVHeap#close()
    assertEquals(4, kvh.scannersForDelayedClose.size());
    assertTrue(kvh.scannersForDelayedClose.contains(s1));
    assertTrue(kvh.scannersForDelayedClose.contains(s2));
    assertTrue(kvh.scannersForDelayedClose.contains(s3));
    assertTrue(kvh.scannersForDelayedClose.contains(s4));
    kvh.close();
    for(KeyValueScanner scanner : scanners) {
      assertTrue(((TestScanner)scanner).isClosed());
    }
  }

  @Test
  public void testScannerException() throws IOException {
    // Test for NPE issue when exception happens in scanners (HBASE-13835)

    TestScanner s1 = new SeekTestScanner(Arrays.asList(kv115, kv211, kv212));
    TestScanner s2 = new SeekTestScanner(Arrays.asList(kv111, kv112));
    TestScanner s3 = new SeekTestScanner(Arrays.asList(kv113, kv114, kv121, kv122, kv213));
    TestScanner s4 = new SeekTestScanner(new ArrayList<Cell>());

    List<KeyValueScanner> scanners = new ArrayList<KeyValueScanner>(Arrays.asList(s1, s2, s3, s4));

    // Creating KeyValueHeap
    KeyValueHeap kvh = new KeyValueHeap(scanners, CellComparator.COMPARATOR);

    try {
      for (KeyValueScanner scanner : scanners) {
        ((SeekTestScanner) scanner).setRealSeekDone(false);
      }
      while (kvh.next() != null);
      // The pollRealKV should throw IOE.
      assertTrue(false);
    } catch (IOException ioe) {
      kvh.close();
    }

    // It implies there is no NPE thrown from kvh.close() if getting here
    for (KeyValueScanner scanner : scanners) {
      // Verify that close is called and only called once for each scanner
      assertTrue(((SeekTestScanner) scanner).isClosed());
      assertEquals(((SeekTestScanner) scanner).getClosedNum(), 1);
    }
  }

  @Test
  public void testPriorityId() throws IOException {
    Cell kv113A = new KeyValue(row1, fam1, col3, Bytes.toBytes("aaa"));
    Cell kv113B = new KeyValue(row1, fam1, col3, Bytes.toBytes("bbb"));
    {
      TestScanner scan1 = new TestScanner(Arrays.asList(kv111, kv112, kv113A), 1);
      TestScanner scan2 = new TestScanner(Arrays.asList(kv113B), 2);
      List<Cell> expected = Arrays.asList(kv111, kv112, kv113B, kv113A);
      assertCells(expected, new ArrayList<KeyValueScanner>(Arrays.asList(scan1, scan2)));
    }
    {
      TestScanner scan1 = new TestScanner(Arrays.asList(kv111, kv112, kv113A), 2);
      TestScanner scan2 = new TestScanner(Arrays.asList(kv113B), 1);
      List<Cell> expected = Arrays.asList(kv111, kv112, kv113A, kv113B);
      assertCells(expected, new ArrayList<KeyValueScanner>(Arrays.asList(scan1, scan2)));
    }
  }

  private static class TestScanner extends CollectionBackedScanner {
    private boolean closed = false;
    private long scannerOrder = 0;

    public TestScanner(List<Cell> list) {
      super(list);
    }

    public TestScanner(List<Cell> list, long scannerOrder) {
      this(list);
      this.scannerOrder = scannerOrder;
    }

    @Override
    public long getScannerOrder() {
      return scannerOrder;
    }

    @Override
    public void close(){
      closed = true;
    }

    public boolean isClosed() {
      return closed;
    }
  }

  private static class SeekTestScanner extends TestScanner {
    private int closedNum = 0;
    private boolean realSeekDone = true;

    public SeekTestScanner(List<Cell> list) {
      super(list);
    }

    @Override
    public void close() {
      super.close();
      closedNum++;
    }

    public int getClosedNum() {
      return closedNum;
    }

    @Override
    public boolean realSeekDone() {
      return realSeekDone;
    }

    public void setRealSeekDone(boolean done) {
      realSeekDone = done;
    }

    @Override
    public void enforceSeek() throws IOException {
      throw new IOException("enforceSeek must not be called on a " + "non-lazy scanner");
    }
  }
}
