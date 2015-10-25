/**
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

package org.apache.hadoop.hbase.filter;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.testclassification.FilterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WAL;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test the invocation logic of the filters. A filter must be invoked only for
 * the columns that are requested for.
 */
@Category({FilterTests.class, SmallTests.class})
public class TestInvocationRecordFilter {

  private static final byte[] TABLE_NAME_BYTES = Bytes
      .toBytes("invocationrecord");
  private static final byte[] FAMILY_NAME_BYTES = Bytes.toBytes("mycf");

  private static final byte[] ROW_BYTES = Bytes.toBytes("row");
  private static final String QUALIFIER_PREFIX = "qualifier";
  private static final String VALUE_PREFIX = "value";

  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private Region region;

  @Before
  public void setUp() throws Exception {
    HTableDescriptor htd = new HTableDescriptor(
        TableName.valueOf(TABLE_NAME_BYTES));
    htd.addFamily(new HColumnDescriptor(FAMILY_NAME_BYTES));
    HRegionInfo info = new HRegionInfo(htd.getTableName(), null, null, false);
    this.region = HBaseTestingUtility.createRegionAndWAL(info, TEST_UTIL.getDataTestDir(),
        TEST_UTIL.getConfiguration(), htd);

    Put put = new Put(ROW_BYTES);
    for (int i = 0; i < 10; i += 2) {
      // puts 0, 2, 4, 6 and 8
      put.addColumn(FAMILY_NAME_BYTES, Bytes.toBytes(QUALIFIER_PREFIX + i), (long) i,
              Bytes.toBytes(VALUE_PREFIX + i));
    }
    this.region.put(put);
    this.region.flush(true);
  }

  @Test
  public void testFilterInvocation() throws Exception {
    List<Integer> selectQualifiers = new ArrayList<Integer>();
    List<Integer> expectedQualifiers = new ArrayList<Integer>();

    selectQualifiers.add(-1);
    verifyInvocationResults(selectQualifiers.toArray(new Integer[selectQualifiers.size()]),
        expectedQualifiers.toArray(new Integer[expectedQualifiers.size()]));

    selectQualifiers.clear();

    selectQualifiers.add(0);
    expectedQualifiers.add(0);
    verifyInvocationResults(selectQualifiers.toArray(new Integer[selectQualifiers.size()]),
        expectedQualifiers.toArray(new Integer[expectedQualifiers.size()]));

    selectQualifiers.add(3);
    verifyInvocationResults(selectQualifiers.toArray(new Integer[selectQualifiers.size()]),
        expectedQualifiers.toArray(new Integer[expectedQualifiers.size()]));

    selectQualifiers.add(4);
    expectedQualifiers.add(4);
    verifyInvocationResults(selectQualifiers.toArray(new Integer[selectQualifiers.size()]),
        expectedQualifiers.toArray(new Integer[expectedQualifiers.size()]));

    selectQualifiers.add(5);
    verifyInvocationResults(selectQualifiers.toArray(new Integer[selectQualifiers.size()]),
        expectedQualifiers.toArray(new Integer[expectedQualifiers.size()]));

    selectQualifiers.add(8);
    expectedQualifiers.add(8);
    verifyInvocationResults(selectQualifiers.toArray(new Integer[selectQualifiers.size()]),
        expectedQualifiers.toArray(new Integer[expectedQualifiers.size()]));
  }

  public void verifyInvocationResults(Integer[] selectQualifiers,
      Integer[] expectedQualifiers) throws Exception {
    Get get = new Get(ROW_BYTES);
    for (int i = 0; i < selectQualifiers.length; i++) {
      get.addColumn(FAMILY_NAME_BYTES,
          Bytes.toBytes(QUALIFIER_PREFIX + selectQualifiers[i]));
    }

    get.setFilter(new InvocationRecordFilter());

    List<KeyValue> expectedValues = new ArrayList<KeyValue>();
    for (int i = 0; i < expectedQualifiers.length; i++) {
      expectedValues.add(new KeyValue(ROW_BYTES, FAMILY_NAME_BYTES, Bytes
          .toBytes(QUALIFIER_PREFIX + expectedQualifiers[i]),
          expectedQualifiers[i], Bytes.toBytes(VALUE_PREFIX
              + expectedQualifiers[i])));
    }

    Scan scan = new Scan(get);
    List<Cell> actualValues = new ArrayList<Cell>();
    List<Cell> temp = new ArrayList<Cell>();
    InternalScanner scanner = this.region.getScanner(scan);
    while (scanner.next(temp)) {
      actualValues.addAll(temp);
      temp.clear();
    }
    actualValues.addAll(temp);
    Assert.assertTrue("Actual values " + actualValues
        + " differ from the expected values:" + expectedValues,
        expectedValues.equals(actualValues));
  }

  @After
  public void tearDown() throws Exception {
    WAL wal = ((HRegion)region).getWAL();
    ((HRegion)region).close();
    wal.close();
  }

  /**
   * Filter which gives the list of keyvalues for which the filter is invoked.
   */
  private static class InvocationRecordFilter extends FilterBase {

    private List<Cell> visitedKeyValues = new ArrayList<Cell>();

    public void reset() {
      visitedKeyValues.clear();
    }

    public ReturnCode filterKeyValue(Cell ignored) {
      visitedKeyValues.add(ignored);
      return ReturnCode.INCLUDE;
    }

    public void filterRowCells(List<Cell> kvs) {
      kvs.clear();
      kvs.addAll(visitedKeyValues);
    }

    public boolean hasFilterRow() {
      return true;
    }
  }
}
