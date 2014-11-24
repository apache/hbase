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
import org.apache.hadoop.hbase.HBaseTestCase;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CollectionBackedScanner;
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
      new KeyValueHeap(scanners, KeyValue.COMPARATOR);

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
      int ret = KeyValue.COMPARATOR.compare(actual.get(i), actual.get(i+1));
      assertTrue(ret < 0);
    }

  }

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
      new KeyValueHeap(scanners, KeyValue.COMPARATOR);

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

  public void testScannerLeak() throws IOException {
    // Test for unclosed scanners (HBASE-1927)

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

    List<Cell> l4 = new ArrayList<Cell>();
    scanners.add(new Scanner(l4));

    //Creating KeyValueHeap
    KeyValueHeap kvh = new KeyValueHeap(scanners, KeyValue.COMPARATOR);

    while(kvh.next() != null);

    for(KeyValueScanner scanner : scanners) {
      assertTrue(((Scanner)scanner).isClosed());
    }
  }

  private static class Scanner extends CollectionBackedScanner {
    private Iterator<Cell> iter;
    private Cell current;
    private boolean closed = false;

    public Scanner(List<Cell> list) {
      super(list);
    }

    public void close(){
      closed = true;
    }

    public boolean isClosed() {
      return closed;
    }
  }


}

