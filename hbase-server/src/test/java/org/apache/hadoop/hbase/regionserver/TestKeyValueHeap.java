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
import java.util.Iterator;
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
  private static final boolean PRINT = false;

  List<KeyValueScanner> scanners = new ArrayList<KeyValueScanner>();

  private byte[] row1;
  private byte[] fam1;
  private byte[] col1;
  private byte[] data;

  private byte[] row2;
  private byte[] fam2;
  private byte[] col2;

  private byte[] col3;
  private byte[] col4;
  private byte[] col5;

  @Before
  public void setUp() throws Exception {
    super.setUp();
    data = Bytes.toBytes("data");
    row1 = Bytes.toBytes("row1");
    fam1 = Bytes.toBytes("fam1");
    col1 = Bytes.toBytes("col1");
    row2 = Bytes.toBytes("row2");
    fam2 = Bytes.toBytes("fam2");
    col2 = Bytes.toBytes("col2");
    col3 = Bytes.toBytes("col3");
    col4 = Bytes.toBytes("col4");
    col5 = Bytes.toBytes("col5");
  }

  @Test
  public void testSorted() throws IOException{
    //Cases that need to be checked are:
    //1. The "smallest" KeyValue is in the same scanners as current
    //2. Current scanner gets empty

    List<Cell> l1 = new ArrayList<Cell>();
    l1.add(new KeyValue(row1, fam1, col5, data));
    l1.add(new KeyValue(row2, fam1, col1, data));
    l1.add(new KeyValue(row2, fam1, col2, data));
    scanners.add(new Scanner(l1));

    List<Cell> l2 = new ArrayList<Cell>();
    l2.add(new KeyValue(row1, fam1, col1, data));
    l2.add(new KeyValue(row1, fam1, col2, data));
    scanners.add(new Scanner(l2));

    List<Cell> l3 = new ArrayList<Cell>();
    l3.add(new KeyValue(row1, fam1, col3, data));
    l3.add(new KeyValue(row1, fam1, col4, data));
    l3.add(new KeyValue(row1, fam2, col1, data));
    l3.add(new KeyValue(row1, fam2, col2, data));
    l3.add(new KeyValue(row2, fam1, col3, data));
    scanners.add(new Scanner(l3));

    List<KeyValue> expected = new ArrayList<KeyValue>();
    expected.add(new KeyValue(row1, fam1, col1, data));
    expected.add(new KeyValue(row1, fam1, col2, data));
    expected.add(new KeyValue(row1, fam1, col3, data));
    expected.add(new KeyValue(row1, fam1, col4, data));
    expected.add(new KeyValue(row1, fam1, col5, data));
    expected.add(new KeyValue(row1, fam2, col1, data));
    expected.add(new KeyValue(row1, fam2, col2, data));
    expected.add(new KeyValue(row2, fam1, col1, data));
    expected.add(new KeyValue(row2, fam1, col2, data));
    expected.add(new KeyValue(row2, fam1, col3, data));

    //Creating KeyValueHeap
    KeyValueHeap kvh =
      new KeyValueHeap(scanners, CellComparator.COMPARATOR);

    List<Cell> actual = new ArrayList<Cell>();
    while(kvh.peek() != null){
      actual.add(kvh.next());
    }

    assertEquals(expected.size(), actual.size());
    for(int i=0; i<expected.size(); i++){
      assertEquals(expected.get(i), actual.get(i));
      if(PRINT){
        System.out.println("expected " +expected.get(i)+
            "\nactual   " +actual.get(i) +"\n");
      }
    }

    //Check if result is sorted according to Comparator
    for(int i=0; i<actual.size()-1; i++){
      int ret = CellComparator.COMPARATOR.compare(actual.get(i), actual.get(i+1));
      assertTrue(ret < 0);
    }

  }

  @Test
  public void testSeek() throws IOException {
    //Cases:
    //1. Seek KeyValue that is not in scanner
    //2. Check that smallest that is returned from a seek is correct

    List<Cell> l1 = new ArrayList<Cell>();
    l1.add(new KeyValue(row1, fam1, col5, data));
    l1.add(new KeyValue(row2, fam1, col1, data));
    l1.add(new KeyValue(row2, fam1, col2, data));
    scanners.add(new Scanner(l1));

    List<Cell> l2 = new ArrayList<Cell>();
    l2.add(new KeyValue(row1, fam1, col1, data));
    l2.add(new KeyValue(row1, fam1, col2, data));
    scanners.add(new Scanner(l2));

    List<Cell> l3 = new ArrayList<Cell>();
    l3.add(new KeyValue(row1, fam1, col3, data));
    l3.add(new KeyValue(row1, fam1, col4, data));
    l3.add(new KeyValue(row1, fam2, col1, data));
    l3.add(new KeyValue(row1, fam2, col2, data));
    l3.add(new KeyValue(row2, fam1, col3, data));
    scanners.add(new Scanner(l3));

    List<KeyValue> expected = new ArrayList<KeyValue>();
    expected.add(new KeyValue(row2, fam1, col1, data));

    //Creating KeyValueHeap
    KeyValueHeap kvh =
      new KeyValueHeap(scanners, CellComparator.COMPARATOR);

    KeyValue seekKv = new KeyValue(row2, fam1, null, null);
    kvh.seek(seekKv);

    List<Cell> actual = new ArrayList<Cell>();
    actual.add(kvh.peek());

    assertEquals(expected.size(), actual.size());
    for(int i=0; i<expected.size(); i++){
      assertEquals(expected.get(i), actual.get(i));
      if(PRINT){
        System.out.println("expected " +expected.get(i)+
            "\nactual   " +actual.get(i) +"\n");
      }
    }

  }

  @Test
  public void testScannerLeak() throws IOException {
    // Test for unclosed scanners (HBASE-1927)

    List<Cell> l1 = new ArrayList<Cell>();
    l1.add(new KeyValue(row1, fam1, col5, data));
    l1.add(new KeyValue(row2, fam1, col1, data));
    l1.add(new KeyValue(row2, fam1, col2, data));
    Scanner s1 = new Scanner(l1);
    scanners.add(s1);

    List<Cell> l2 = new ArrayList<Cell>();
    l2.add(new KeyValue(row1, fam1, col1, data));
    l2.add(new KeyValue(row1, fam1, col2, data));
    Scanner s2 = new Scanner(l2);
    scanners.add(s2);

    List<Cell> l3 = new ArrayList<Cell>();
    l3.add(new KeyValue(row1, fam1, col3, data));
    l3.add(new KeyValue(row1, fam1, col4, data));
    l3.add(new KeyValue(row1, fam2, col1, data));
    l3.add(new KeyValue(row1, fam2, col2, data));
    l3.add(new KeyValue(row2, fam1, col3, data));
    Scanner s3 = new Scanner(l3);
    scanners.add(s3);

    List<Cell> l4 = new ArrayList<Cell>();
    Scanner s4 = new Scanner(l4);
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
      assertTrue(((Scanner)scanner).isClosed());
    }
  }

  @Test
  public void testScannerException() throws IOException {
    // Test for NPE issue when exception happens in scanners (HBASE-13835)

    List<Cell> l1 = new ArrayList<Cell>();
    l1.add(new KeyValue(row1, fam1, col5, data));
    l1.add(new KeyValue(row2, fam1, col1, data));
    l1.add(new KeyValue(row2, fam1, col2, data));
    SeekScanner s1 = new SeekScanner(l1);
    scanners.add(s1);

    List<Cell> l2 = new ArrayList<Cell>();
    l2.add(new KeyValue(row1, fam1, col1, data));
    l2.add(new KeyValue(row1, fam1, col2, data));
    SeekScanner s2 = new SeekScanner(l2);
    scanners.add(s2);

    List<Cell> l3 = new ArrayList<Cell>();
    l3.add(new KeyValue(row1, fam1, col3, data));
    l3.add(new KeyValue(row1, fam1, col4, data));
    l3.add(new KeyValue(row1, fam2, col1, data));
    l3.add(new KeyValue(row1, fam2, col2, data));
    l3.add(new KeyValue(row2, fam1, col3, data));
    SeekScanner s3 = new SeekScanner(l3);
    scanners.add(s3);

    List<Cell> l4 = new ArrayList<Cell>();
    SeekScanner s4 = new SeekScanner(l4);
    scanners.add(s4);

    // Creating KeyValueHeap
    KeyValueHeap kvh = new KeyValueHeap(scanners, CellComparator.COMPARATOR);

    try {
      for (KeyValueScanner scanner : scanners) {
        ((SeekScanner) scanner).setRealSeekDone(false);
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
      assertTrue(((SeekScanner) scanner).isClosed());
      assertEquals(((SeekScanner) scanner).getClosedNum(), 1);
    }
  }

  private static class Scanner extends CollectionBackedScanner {
    private Iterator<Cell> iter;
    private Cell current;
    private boolean closed = false;

    public Scanner(List<Cell> list) {
      super(list);
    }

    @Override
    public void close(){
      closed = true;
    }

    public boolean isClosed() {
      return closed;
    }
  }

  private static class SeekScanner extends Scanner {
    private int closedNum = 0;
    private boolean realSeekDone = true;

    public SeekScanner(List<Cell> list) {
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
