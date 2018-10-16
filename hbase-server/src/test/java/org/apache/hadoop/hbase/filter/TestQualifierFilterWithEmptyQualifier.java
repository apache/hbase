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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.testclassification.FilterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test qualifierFilter with empty qualifier column
 */
@Category({FilterTests.class, SmallTests.class})
public class TestQualifierFilterWithEmptyQualifier {

  private final static Logger LOG
      = LoggerFactory.getLogger(TestQualifierFilterWithEmptyQualifier.class);
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestQualifierFilterWithEmptyQualifier.class);
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private HRegion region;

  @Rule
  public TestName name = new TestName();

  private static final byte[][] ROWS =
    { Bytes.toBytes("testRowOne-0"), Bytes.toBytes("testRowOne-1"),
        Bytes.toBytes("testRowOne-2"), Bytes.toBytes("testRowOne-3") };
  private static final byte[] FAMILY = Bytes.toBytes("testFamily");
  private static final byte[][] QUALIFIERS = {HConstants.EMPTY_BYTE_ARRAY,
      Bytes.toBytes("testQualifier")};
  private static final byte[] VALUE = Bytes.toBytes("testValueOne");
  private long numRows = (long) ROWS.length;

  @Before
  public void setUp() throws Exception {
    TableDescriptor htd = TableDescriptorBuilder
        .newBuilder(TableName.valueOf("TestQualifierFilter"))
        .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(FAMILY).build()).build();
    RegionInfo info = RegionInfoBuilder.newBuilder(htd.getTableName()).build();
    this.region = HBaseTestingUtility
        .createRegionAndWAL(info, TEST_UTIL.getDataTestDir(), TEST_UTIL.getConfiguration(), htd);

    // Insert data
    for (byte[] ROW : ROWS) {
      Put p = new Put(ROW);
      p.setDurability(Durability.SKIP_WAL);
      for (byte[] QUALIFIER : QUALIFIERS) {
        p.addColumn(FAMILY, QUALIFIER, VALUE);
      }
      this.region.put(p);
    }

    // Flush
    this.region.flush(true);
  }

  @After
  public void tearDown() throws Exception {
    HBaseTestingUtility.closeRegionAndWAL(region);
  }

  @Test
  public void testQualifierFilterWithEmptyColumn() throws IOException {
    long colsPerRow = 2;
    long expectedKeys = colsPerRow / 2;
    Filter f = new QualifierFilter(CompareOperator.EQUAL,
        new BinaryComparator(QUALIFIERS[0]));
    Scan s = new Scan();
    s.setFilter(f);
    verifyScanNoEarlyOut(s, this.numRows, expectedKeys);

    expectedKeys = colsPerRow / 2;
    f = new QualifierFilter(CompareOperator.EQUAL,
        new BinaryComparator(QUALIFIERS[1]));
    s = new Scan();
    s.setFilter(f);
    verifyScanNoEarlyOut(s, this.numRows, expectedKeys);

    expectedKeys = colsPerRow / 2;
    f = new QualifierFilter(CompareOperator.GREATER,
        new BinaryComparator(QUALIFIERS[0]));
    s = new Scan();
    s.setFilter(f);
    verifyScanNoEarlyOut(s, this.numRows, expectedKeys);

    expectedKeys = colsPerRow;
    f = new QualifierFilter(CompareOperator.GREATER_OR_EQUAL,
        new BinaryComparator(QUALIFIERS[0]));
    s = new Scan();
    s.setFilter(f);
    verifyScanNoEarlyOut(s, this.numRows, expectedKeys);
  }

  private void verifyScanNoEarlyOut(Scan s, long expectedRows,
      long expectedKeys)
      throws IOException {
    InternalScanner scanner = this.region.getScanner(s);
    List<Cell> results = new ArrayList<>();
    int i = 0;
    for (boolean done = true; done; i++) {
      done = scanner.next(results);
      Arrays.sort(results.toArray(new Cell[results.size()]),
          CellComparator.getInstance());
      LOG.info("counter=" + i + ", " + results);
      if(results.isEmpty()) {
        break;
      }
      assertTrue("Scanned too many rows! Only expected " + expectedRows +
          " total but already scanned " + (i+1), expectedRows > i);
      assertEquals("Expected " + expectedKeys + " keys per row but " +
          "returned " + results.size(), expectedKeys, results.size());
      results.clear();
    }
    assertEquals("Expected " + expectedRows + " rows but scanned " + i +
        " rows", expectedRows, i);
  }
}