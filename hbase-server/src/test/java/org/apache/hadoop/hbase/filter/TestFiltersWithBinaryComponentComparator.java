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
package org.apache.hadoop.hbase.filter;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category(MediumTests.class)
public class TestFiltersWithBinaryComponentComparator {

  /**
   * See https://issues.apache.org/jira/browse/HBASE-22969 - for need of BinaryComponentComparator
   * The descrption on jira should also help you in understanding tests implemented in this class
   */

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestFiltersWithBinaryComponentComparator.class);

  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final Logger LOG =
      LoggerFactory.getLogger(TestFiltersWithBinaryComponentComparator.class);
  private byte[] family = Bytes.toBytes("family");
  private byte[] qf = Bytes.toBytes("qf");
  private TableName tableName;
  private int aOffset = 0;
  private int bOffset = 4;
  private int cOffset = 8;
  private int dOffset = 12;

  @Rule
  public TestName name = new TestName();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniCluster();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testRowFilterWithBinaryComponentComparator() throws IOException {
    //SELECT * from table where a=1 and b > 10 and b < 20 and c > 90 and c < 100 and d=1
    tableName = TableName.valueOf(name.getMethodName());
    Table ht = TEST_UTIL.createTable(tableName, family, Integer.MAX_VALUE);
    generateRows(ht, family, qf);
    FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
    setRowFilters(filterList);
    Scan scan = createScan(filterList);
    List<Cell> result = getResults(ht,scan);
    for(Cell cell: result){
      byte[] key = CellUtil.cloneRow(cell);
      int a = Bytes.readAsInt(key,aOffset,4);
      int b = Bytes.readAsInt(key,bOffset,4);
      int c = Bytes.readAsInt(key,cOffset,4);
      int d = Bytes.readAsInt(key,dOffset,4);
      assertTrue(a == 1 &&
                 b > 10 &&
                 b < 20 &&
                 c > 90 &&
                 c < 100 &&
                 d == 1);
    }
    ht.close();
  }

  @Test
  public void testValueFilterWithBinaryComponentComparator() throws IOException {
    //SELECT * from table where value has 'y' at position 1
    tableName = TableName.valueOf(name.getMethodName());
    Table ht = TEST_UTIL.createTable(tableName, family, Integer.MAX_VALUE);
    generateRows(ht, family, qf);
    FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
    setValueFilters(filterList);
    Scan scan = new Scan();
    scan.setFilter(filterList);
    List<Cell> result = getResults(ht,scan);
    for(Cell cell: result){
      byte[] value = CellUtil.cloneValue(cell);
      assertTrue(Bytes.toString(value).charAt(1) == 'y');
    }
    ht.close();
  }

  @Test
  public void testRowAndValueFilterWithBinaryComponentComparator() throws IOException {
    //SELECT * from table where a=1 and b > 10 and b < 20 and c > 90 and c < 100 and d=1
    //and value has 'y' at position 1"
    tableName = TableName.valueOf(name.getMethodName());
    Table ht = TEST_UTIL.createTable(tableName, family, Integer.MAX_VALUE);
    generateRows(ht, family, qf);
    FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
    setRowFilters(filterList);
    setValueFilters(filterList);
    Scan scan = new Scan();
    scan.setFilter(filterList);
    List<Cell> result = getResults(ht,scan);
    for(Cell cell: result){
      byte[] key = CellUtil.cloneRow(cell);
      int a = Bytes.readAsInt(key,aOffset,4);
      int b = Bytes.readAsInt(key,bOffset,4);
      int c = Bytes.readAsInt(key,cOffset,4);
      int d = Bytes.readAsInt(key,dOffset,4);
      assertTrue(a == 1 &&
                 b > 10 &&
                 b < 20 &&
                 c > 90 &&
                 c < 100 &&
                 d == 1);
      byte[] value = CellUtil.cloneValue(cell);
      assertTrue(Bytes.toString(value).charAt(1) == 'y');
    }
    ht.close();
  }

  /**
   * Since we are trying to emulate
   * SQL: SELECT * from table where a = 1 and b > 10 and b < 20 and
   * c > 90 and c < 100 and d = 1
   * We are generating rows with:
   * a = 1, b >=9 and b < 22, c >= 89 and c < 102, and d = 1
   * At the end the table will look something like this:
   * ------------
   *  a| b|  c|d|
   * ------------
   *  1| 9| 89|1|family:qf|xyz|
   *  -----------
   *  1| 9| 90|1|family:qf|abc|
   *  -----------
   *  1| 9| 91|1|family:qf|xyz|
   *  -------------------------
   *  .
   *  -------------------------
   *  .
   *  -------------------------
   *  1|21|101|1|family:qf|xyz|
   */
  private void generateRows(Table ht, byte[] family, byte[] qf)
      throws IOException {
    for(int a = 1; a < 2; ++a) {
      for(int b = 9; b < 22; ++b) {
        for(int c = 89; c < 102; ++c) {
          for(int d = 1; d < 2 ; ++d) {
            byte[] key = new byte[16];
            Bytes.putInt(key,0,a);
            Bytes.putInt(key,4,b);
            Bytes.putInt(key,8,c);
            Bytes.putInt(key,12,d);
            Put row = new Put(key);
            if (c%2==0) {
              row.addColumn(family, qf, Bytes.toBytes("abc"));
              if (LOG.isInfoEnabled()) {
                LOG.info("added row: {} with value 'abc'", Arrays.toString(Hex.encodeHex(key)));
              }
            } else {
              row.addColumn(family, qf, Bytes.toBytes("xyz"));
              if (LOG.isInfoEnabled()) {
                LOG.info("added row: {} with value 'xyz'", Arrays.toString(Hex.encodeHex(key)));
              }
            }
          }
        }
      }
    }
    TEST_UTIL.flush();
  }

  private void setRowFilters(FilterList filterList) {
    //offset for b as it is second component of "a+b+c+d"
    //'a' is at offset 0
    int bOffset = 4;
    byte[] b10 = Bytes.toBytes(10); //tests b > 10
    Filter b10Filter = new RowFilter(CompareOperator.GREATER,
            new BinaryComponentComparator(b10,bOffset));
    filterList.addFilter(b10Filter);

    byte[] b20  = Bytes.toBytes(20); //tests b < 20
    Filter b20Filter = new RowFilter(CompareOperator.LESS,
            new BinaryComponentComparator(b20,bOffset));
    filterList.addFilter(b20Filter);

    //offset for c as it is third component of "a+b+c+d"
    int cOffset = 8;
    byte[] c90  = Bytes.toBytes(90); //tests c > 90
    Filter c90Filter = new RowFilter(CompareOperator.GREATER,
            new BinaryComponentComparator(c90,cOffset));
    filterList.addFilter(c90Filter);

    byte[] c100  = Bytes.toBytes(100); //tests c < 100
    Filter c100Filter = new RowFilter(CompareOperator.LESS,
            new BinaryComponentComparator(c100,cOffset));
    filterList.addFilter(c100Filter);

    //offset for d as it is fourth component of "a+b+c+d"
    int dOffset = 12;
    byte[] d1   = Bytes.toBytes(1); //tests d == 1
    Filter dFilter  = new RowFilter(CompareOperator.EQUAL,
            new BinaryComponentComparator(d1,dOffset));

    filterList.addFilter(dFilter);

  }

  /**
   * We have rows with either "abc" or "xyz".
   * We want values which have 'y' at second position
   * of the string.
   * As a result only values with "xyz" shall be returned
  */
  private void setValueFilters(FilterList filterList) {
    int offset = 1;
    byte[] y = Bytes.toBytes("y");
    Filter yFilter  = new ValueFilter(CompareOperator.EQUAL,
            new BinaryComponentComparator(y,offset));
    filterList.addFilter(yFilter);
  }

  private Scan createScan(FilterList list) {
    //build start and end key for scan
    byte[] startKey = new byte[16]; //key size with four ints
    Bytes.putInt(startKey,aOffset,1); //a=1, takes care of a = 1
    Bytes.putInt(startKey,bOffset,11); //b=11, takes care of b > 10
    Bytes.putInt(startKey,cOffset,91); //c=91,
    Bytes.putInt(startKey,dOffset,1); //d=1,

    byte[] endKey = new byte[16];
    Bytes.putInt(endKey,aOffset,1); //a=1, takes care of a = 1
    Bytes.putInt(endKey,bOffset,20); //b=20, takes care of b < 20
    Bytes.putInt(endKey,cOffset,100); //c=100,
    Bytes.putInt(endKey,dOffset,1); //d=1,

    //setup scan
    Scan scan = new Scan().withStartRow(startKey).withStopRow(endKey);
    scan.setFilter(list);
    return scan;
  }

  private List<Cell> getResults(Table ht, Scan scan) throws IOException {
    ResultScanner scanner = ht.getScanner(scan);
    List<Cell> results = new ArrayList<>();
    Result r;
    while ((r = scanner.next()) != null) {
      for (Cell kv : r.listCells()) {
        results.add(kv);
      }
    }
    scanner.close();
    return results;
  }

}
