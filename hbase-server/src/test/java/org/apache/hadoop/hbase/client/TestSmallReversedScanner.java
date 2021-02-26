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
package org.apache.hadoop.hbase.client;

import java.io.IOException;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category(MediumTests.class)
public class TestSmallReversedScanner {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestSmallReversedScanner.class);

  public static final Logger LOG = LoggerFactory.getLogger(TestSmallReversedScanner.class);
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private static final TableName TABLE_NAME = TableName.valueOf("testReversedSmall");
  private static final byte[] COLUMN_FAMILY = Bytes.toBytes("columnFamily");

  private static Table htable = null;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniCluster(1);

    // create a table with 4 region: (-oo, b),[b,c),[c,d),[d,+oo)
    byte[] bytes = Bytes.toBytes("bcd");
    byte[][] splitKeys = new byte[bytes.length][];

    for (int i = 0; i < bytes.length; i++) {
      splitKeys[i] = new byte[] { bytes[i] };
    }
    htable = TEST_UTIL.createTable(TABLE_NAME, COLUMN_FAMILY, splitKeys);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @After
  public void tearDown() throws IOException {
    TEST_UTIL.truncateTable(TABLE_NAME);
  }

  /**
   * all rowKeys are fit in the last region.
   * @throws IOException
   */
  @Test
  public void testSmallReversedScan01() throws IOException {
    String[][] keysCases = new String[][] {
      { "d0", "d1", "d2", "d3" }, // all rowKeys fit in the last region.
      { "a0", "a1", "a2", "a3" }, // all rowKeys fit in the first region.
      { "a0", "b1", "c2", "d3" }, // each region with a rowKey
    };

    for (int caseIndex = 0; caseIndex < keysCases.length; caseIndex++) {
      testSmallReversedScanInternal(keysCases[caseIndex]);
      TEST_UTIL.truncateTable(TABLE_NAME);
    }
  }

  private void testSmallReversedScanInternal(String[] inputRowKeys) throws IOException {
    int rowCount = inputRowKeys.length;

    for (int i = 0; i < rowCount; i++) {
      Put put = new Put(Bytes.toBytes(inputRowKeys[i]));
      put.addColumn(COLUMN_FAMILY, null, Bytes.toBytes(i));
      htable.put(put);
    }

    Scan scan = new Scan();
    scan.setReversed(true);
    scan.setSmall(true);

    ResultScanner scanner = htable.getScanner(scan);
    Result r;
    int value = rowCount;
    while ((r = scanner.next()) != null) {
      Assert.assertArrayEquals(r.getValue(COLUMN_FAMILY, null), Bytes.toBytes(--value));
      Assert.assertArrayEquals(r.getRow(), Bytes.toBytes(inputRowKeys[value]));
    }

    Assert.assertEquals(0, value);
  }

  /**
   * Corner case:
   *  HBase has 4 regions, (-oo,b),[b,c),[c,d),[d,+oo), and only rowKey with byte[]={0x00} locate in region (-oo,b) .
   *  test whether reversed small scanner will return infinity results with RowKey={0x00}.
   * @throws IOException
   */
  @Test
  public void testSmallReversedScan02() throws IOException {
    Put put = new Put(new byte[] { (char) 0x00 });
    put.addColumn(COLUMN_FAMILY, null, Bytes.toBytes(0));
    htable.put(put);

    Scan scan = new Scan();
    scan.setCaching(1);
    scan.setReversed(true);
    scan.setSmall(true);

    ResultScanner scanner = htable.getScanner(scan);
    Result r;
    int count = 1;
    while ((r = scanner.next()) != null) {
      Assert.assertArrayEquals(r.getValue(COLUMN_FAMILY, null), Bytes.toBytes(0));
      Assert.assertArrayEquals(r.getRow(), new byte[] { (char) 0x00 });
      Assert.assertTrue(--count >= 0);
    }
    Assert.assertEquals(0, count);
  }
}
