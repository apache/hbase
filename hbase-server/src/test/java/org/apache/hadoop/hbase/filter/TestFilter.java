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

package org.apache.hadoop.hbase.filter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.testclassification.FilterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WAL;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.base.Throwables;

/**
 * Test filters at the HRegion doorstep.
 */
@Category({FilterTests.class, SmallTests.class})
public class TestFilter {
  private final static Log LOG = LogFactory.getLog(TestFilter.class);
  private Region region;
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  //
  // Rows, Qualifiers, and Values are in two groups, One and Two.
  //

  private static final byte [][] ROWS_ONE = {
      Bytes.toBytes("testRowOne-0"), Bytes.toBytes("testRowOne-1"),
      Bytes.toBytes("testRowOne-2"), Bytes.toBytes("testRowOne-3")
  };

  private static final byte [][] ROWS_TWO = {
      Bytes.toBytes("testRowTwo-0"), Bytes.toBytes("testRowTwo-1"),
      Bytes.toBytes("testRowTwo-2"), Bytes.toBytes("testRowTwo-3")
  };

  private static final byte [][] ROWS_THREE = {
    Bytes.toBytes("testRowThree-0"), Bytes.toBytes("testRowThree-1"),
    Bytes.toBytes("testRowThree-2"), Bytes.toBytes("testRowThree-3")
  };

  private static final byte [][] ROWS_FOUR = {
    Bytes.toBytes("testRowFour-0"), Bytes.toBytes("testRowFour-1"),
    Bytes.toBytes("testRowFour-2"), Bytes.toBytes("testRowFour-3")
  };

  private static final byte [][] FAMILIES = {
    Bytes.toBytes("testFamilyOne"), Bytes.toBytes("testFamilyTwo")
  };

  private static final byte [][] FAMILIES_1 = {
    Bytes.toBytes("testFamilyThree"), Bytes.toBytes("testFamilyFour")
  };

  private static final byte [][] QUALIFIERS_ONE = {
    Bytes.toBytes("testQualifierOne-0"), Bytes.toBytes("testQualifierOne-1"),
    Bytes.toBytes("testQualifierOne-2"), Bytes.toBytes("testQualifierOne-3")
  };

  private static final byte [][] QUALIFIERS_TWO = {
    Bytes.toBytes("testQualifierTwo-0"), Bytes.toBytes("testQualifierTwo-1"),
    Bytes.toBytes("testQualifierTwo-2"), Bytes.toBytes("testQualifierTwo-3")
  };

  private static final byte [][] QUALIFIERS_THREE = {
    Bytes.toBytes("testQualifierThree-0"), Bytes.toBytes("testQualifierThree-1"),
    Bytes.toBytes("testQualifierThree-2"), Bytes.toBytes("testQualifierThree-3")
  };

  private static final byte [][] QUALIFIERS_FOUR = {
    Bytes.toBytes("testQualifierFour-0"), Bytes.toBytes("testQualifierFour-1"),
    Bytes.toBytes("testQualifierFour-2"), Bytes.toBytes("testQualifierFour-3")
  };

  private static final byte [][] VALUES = {
    Bytes.toBytes("testValueOne"), Bytes.toBytes("testValueTwo")
  };

  byte [][] NEW_FAMILIES = {
      Bytes.toBytes("f1"), Bytes.toBytes("f2")
    };

  private long numRows = ROWS_ONE.length + ROWS_TWO.length;
  private long colsPerRow = FAMILIES.length * QUALIFIERS_ONE.length;

  @Before
  public void setUp() throws Exception {
    HTableDescriptor htd = new HTableDescriptor(TableName.valueOf("TestFilter"));
    htd.addFamily(new HColumnDescriptor(FAMILIES[0]));
    htd.addFamily(new HColumnDescriptor(FAMILIES[1]));
    htd.addFamily(new HColumnDescriptor(FAMILIES_1[0]));
    htd.addFamily(new HColumnDescriptor(FAMILIES_1[1]));
    htd.addFamily(new HColumnDescriptor(NEW_FAMILIES[0]));
    htd.addFamily(new HColumnDescriptor(NEW_FAMILIES[1]));
    HRegionInfo info = new HRegionInfo(htd.getTableName(), null, null, false);
    this.region = HBaseTestingUtility.createRegionAndWAL(info, TEST_UTIL.getDataTestDir(),
        TEST_UTIL.getConfiguration(), htd);

    // Insert first half
    for(byte [] ROW : ROWS_ONE) {
      Put p = new Put(ROW);
      p.setDurability(Durability.SKIP_WAL);
      for(byte [] QUALIFIER : QUALIFIERS_ONE) {
        p.addColumn(FAMILIES[0], QUALIFIER, VALUES[0]);
      }
      this.region.put(p);
    }
    for(byte [] ROW : ROWS_TWO) {
      Put p = new Put(ROW);
      p.setDurability(Durability.SKIP_WAL);
      for(byte [] QUALIFIER : QUALIFIERS_TWO) {
        p.addColumn(FAMILIES[1], QUALIFIER, VALUES[1]);
      }
      this.region.put(p);
    }

    // Flush
    this.region.flush(true);

    // Insert second half (reverse families)
    for(byte [] ROW : ROWS_ONE) {
      Put p = new Put(ROW);
      p.setDurability(Durability.SKIP_WAL);
      for(byte [] QUALIFIER : QUALIFIERS_ONE) {
        p.addColumn(FAMILIES[1], QUALIFIER, VALUES[0]);
      }
      this.region.put(p);
    }
    for(byte [] ROW : ROWS_TWO) {
      Put p = new Put(ROW);
      p.setDurability(Durability.SKIP_WAL);
      for(byte [] QUALIFIER : QUALIFIERS_TWO) {
        p.addColumn(FAMILIES[0], QUALIFIER, VALUES[1]);
      }
      this.region.put(p);
    }

    // Delete the second qualifier from all rows and families
    for(byte [] ROW : ROWS_ONE) {
      Delete d = new Delete(ROW);
      d.addColumns(FAMILIES[0], QUALIFIERS_ONE[1]);
      d.addColumns(FAMILIES[1], QUALIFIERS_ONE[1]);
      this.region.delete(d);
    }
    for(byte [] ROW : ROWS_TWO) {
      Delete d = new Delete(ROW);
      d.addColumns(FAMILIES[0], QUALIFIERS_TWO[1]);
      d.addColumns(FAMILIES[1], QUALIFIERS_TWO[1]);
      this.region.delete(d);
    }
    colsPerRow -= 2;

    // Delete the second rows from both groups, one column at a time
    for(byte [] QUALIFIER : QUALIFIERS_ONE) {
      Delete d = new Delete(ROWS_ONE[1]);
      d.addColumns(FAMILIES[0], QUALIFIER);
      d.addColumns(FAMILIES[1], QUALIFIER);
      this.region.delete(d);
    }
    for(byte [] QUALIFIER : QUALIFIERS_TWO) {
      Delete d = new Delete(ROWS_TWO[1]);
      d.addColumns(FAMILIES[0], QUALIFIER);
      d.addColumns(FAMILIES[1], QUALIFIER);
      this.region.delete(d);
    }
    numRows -= 2;
  }

  @After
  public void tearDown() throws Exception {
    HBaseTestingUtility.closeRegionAndWAL(region);
  }

  @Test
  public void testRegionScannerReseek() throws Exception {
    // create new rows and column family to show how reseek works..
    for (byte[] ROW : ROWS_THREE) {
      Put p = new Put(ROW);
      p.setDurability(Durability.SKIP_WAL);
      for (byte[] QUALIFIER : QUALIFIERS_THREE) {
        p.addColumn(FAMILIES[0], QUALIFIER, VALUES[0]);

      }
      this.region.put(p);
    }
    for (byte[] ROW : ROWS_FOUR) {
      Put p = new Put(ROW);
      p.setDurability(Durability.SKIP_WAL);
      for (byte[] QUALIFIER : QUALIFIERS_FOUR) {
        p.addColumn(FAMILIES[1], QUALIFIER, VALUES[1]);
      }
      this.region.put(p);
    }
    // Flush
    this.region.flush(true);

    // Insert second half (reverse families)
    for (byte[] ROW : ROWS_THREE) {
      Put p = new Put(ROW);
      p.setDurability(Durability.SKIP_WAL);
      for (byte[] QUALIFIER : QUALIFIERS_THREE) {
        p.addColumn(FAMILIES[1], QUALIFIER, VALUES[0]);
      }
      this.region.put(p);
    }
    for (byte[] ROW : ROWS_FOUR) {
      Put p = new Put(ROW);
      p.setDurability(Durability.SKIP_WAL);
      for (byte[] QUALIFIER : QUALIFIERS_FOUR) {
        p.addColumn(FAMILIES[0], QUALIFIER, VALUES[1]);
      }
      this.region.put(p);
    }

    Scan s = new Scan();
    // set a start row
    s.setStartRow(ROWS_FOUR[1]);
    RegionScanner scanner = region.getScanner(s);

    // reseek to row three.
    scanner.reseek(ROWS_THREE[1]);
    List<Cell> results = new ArrayList<Cell>();

    // the results should belong to ROWS_THREE[1]
    scanner.next(results);
    for (Cell keyValue : results) {
      assertTrue("The rows with ROWS_TWO as row key should be appearing.",
          CellUtil.matchingRow(keyValue, ROWS_THREE[1]));
    }
    // again try to reseek to a value before ROWS_THREE[1]
    scanner.reseek(ROWS_ONE[1]);
    results = new ArrayList<Cell>();
    // This time no seek would have been done to ROWS_ONE[1]
    scanner.next(results);
    for (Cell keyValue : results) {
      assertFalse("Cannot rewind back to a value less than previous reseek.",
          Bytes.toString(CellUtil.cloneRow(keyValue)).contains("testRowOne"));
    }
  }

  @Test
  public void testNoFilter() throws Exception {
    // No filter
    long expectedRows = this.numRows;
    long expectedKeys = this.colsPerRow;

    // Both families
    Scan s = new Scan();
    verifyScan(s, expectedRows, expectedKeys);

    // One family
    s = new Scan();
    s.addFamily(FAMILIES[0]);
    verifyScan(s, expectedRows, expectedKeys/2);
  }

  @Test
  public void testPrefixFilter() throws Exception {
    // Grab rows from group one (half of total)
    long expectedRows = this.numRows / 2;
    long expectedKeys = this.colsPerRow;
    Scan s = new Scan();
    s.setFilter(new PrefixFilter(Bytes.toBytes("testRowOne")));
    verifyScan(s, expectedRows, expectedKeys);
  }

  public void testPrefixFilterWithReverseScan() throws Exception {
    // Grab rows from group one (half of total)
    long expectedRows = this.numRows / 2;
    long expectedKeys = this.colsPerRow;
    Scan s = new Scan();
    s.setReversed(true);
    s.setFilter(new PrefixFilter(Bytes.toBytes("testRowOne")));
    verifyScan(s, expectedRows, expectedKeys);
  }

  @Test
  public void testPageFilter() throws Exception {

    // KVs in first 6 rows
    KeyValue [] expectedKVs = {
      // testRowOne-0
      new KeyValue(ROWS_ONE[0], FAMILIES[0], QUALIFIERS_ONE[0], VALUES[0]),
      new KeyValue(ROWS_ONE[0], FAMILIES[0], QUALIFIERS_ONE[2], VALUES[0]),
      new KeyValue(ROWS_ONE[0], FAMILIES[0], QUALIFIERS_ONE[3], VALUES[0]),
      new KeyValue(ROWS_ONE[0], FAMILIES[1], QUALIFIERS_ONE[0], VALUES[0]),
      new KeyValue(ROWS_ONE[0], FAMILIES[1], QUALIFIERS_ONE[2], VALUES[0]),
      new KeyValue(ROWS_ONE[0], FAMILIES[1], QUALIFIERS_ONE[3], VALUES[0]),
      // testRowOne-2
      new KeyValue(ROWS_ONE[2], FAMILIES[0], QUALIFIERS_ONE[0], VALUES[0]),
      new KeyValue(ROWS_ONE[2], FAMILIES[0], QUALIFIERS_ONE[2], VALUES[0]),
      new KeyValue(ROWS_ONE[2], FAMILIES[0], QUALIFIERS_ONE[3], VALUES[0]),
      new KeyValue(ROWS_ONE[2], FAMILIES[1], QUALIFIERS_ONE[0], VALUES[0]),
      new KeyValue(ROWS_ONE[2], FAMILIES[1], QUALIFIERS_ONE[2], VALUES[0]),
      new KeyValue(ROWS_ONE[2], FAMILIES[1], QUALIFIERS_ONE[3], VALUES[0]),
      // testRowOne-3
      new KeyValue(ROWS_ONE[3], FAMILIES[0], QUALIFIERS_ONE[0], VALUES[0]),
      new KeyValue(ROWS_ONE[3], FAMILIES[0], QUALIFIERS_ONE[2], VALUES[0]),
      new KeyValue(ROWS_ONE[3], FAMILIES[0], QUALIFIERS_ONE[3], VALUES[0]),
      new KeyValue(ROWS_ONE[3], FAMILIES[1], QUALIFIERS_ONE[0], VALUES[0]),
      new KeyValue(ROWS_ONE[3], FAMILIES[1], QUALIFIERS_ONE[2], VALUES[0]),
      new KeyValue(ROWS_ONE[3], FAMILIES[1], QUALIFIERS_ONE[3], VALUES[0]),
      // testRowTwo-0
      new KeyValue(ROWS_TWO[0], FAMILIES[0], QUALIFIERS_TWO[0], VALUES[1]),
      new KeyValue(ROWS_TWO[0], FAMILIES[0], QUALIFIERS_TWO[2], VALUES[1]),
      new KeyValue(ROWS_TWO[0], FAMILIES[0], QUALIFIERS_TWO[3], VALUES[1]),
      new KeyValue(ROWS_TWO[0], FAMILIES[1], QUALIFIERS_TWO[0], VALUES[1]),
      new KeyValue(ROWS_TWO[0], FAMILIES[1], QUALIFIERS_TWO[2], VALUES[1]),
      new KeyValue(ROWS_TWO[0], FAMILIES[1], QUALIFIERS_TWO[3], VALUES[1]),
      // testRowTwo-2
      new KeyValue(ROWS_TWO[2], FAMILIES[0], QUALIFIERS_TWO[0], VALUES[1]),
      new KeyValue(ROWS_TWO[2], FAMILIES[0], QUALIFIERS_TWO[2], VALUES[1]),
      new KeyValue(ROWS_TWO[2], FAMILIES[0], QUALIFIERS_TWO[3], VALUES[1]),
      new KeyValue(ROWS_TWO[2], FAMILIES[1], QUALIFIERS_TWO[0], VALUES[1]),
      new KeyValue(ROWS_TWO[2], FAMILIES[1], QUALIFIERS_TWO[2], VALUES[1]),
      new KeyValue(ROWS_TWO[2], FAMILIES[1], QUALIFIERS_TWO[3], VALUES[1]),
      // testRowTwo-3
      new KeyValue(ROWS_TWO[3], FAMILIES[0], QUALIFIERS_TWO[0], VALUES[1]),
      new KeyValue(ROWS_TWO[3], FAMILIES[0], QUALIFIERS_TWO[2], VALUES[1]),
      new KeyValue(ROWS_TWO[3], FAMILIES[0], QUALIFIERS_TWO[3], VALUES[1]),
      new KeyValue(ROWS_TWO[3], FAMILIES[1], QUALIFIERS_TWO[0], VALUES[1]),
      new KeyValue(ROWS_TWO[3], FAMILIES[1], QUALIFIERS_TWO[2], VALUES[1]),
      new KeyValue(ROWS_TWO[3], FAMILIES[1], QUALIFIERS_TWO[3], VALUES[1])
    };

    // Grab all 6 rows
    long expectedRows = 6;
    long expectedKeys = this.colsPerRow;
    Scan s = new Scan();
    s.setFilter(new PageFilter(expectedRows));
    verifyScan(s, expectedRows, expectedKeys);
    s.setFilter(new PageFilter(expectedRows));
    verifyScanFull(s, expectedKVs);

    // Grab first 4 rows (6 cols per row)
    expectedRows = 4;
    expectedKeys = this.colsPerRow;
    s = new Scan();
    s.setFilter(new PageFilter(expectedRows));
    verifyScan(s, expectedRows, expectedKeys);
    s.setFilter(new PageFilter(expectedRows));
    verifyScanFull(s, Arrays.copyOf(expectedKVs, 24));

    // Grab first 2 rows
    expectedRows = 2;
    expectedKeys = this.colsPerRow;
    s = new Scan();
    s.setFilter(new PageFilter(expectedRows));
    verifyScan(s, expectedRows, expectedKeys);
    s.setFilter(new PageFilter(expectedRows));
    verifyScanFull(s, Arrays.copyOf(expectedKVs, 12));

    // Grab first row
    expectedRows = 1;
    expectedKeys = this.colsPerRow;
    s = new Scan();
    s.setFilter(new PageFilter(expectedRows));
    verifyScan(s, expectedRows, expectedKeys);
    s.setFilter(new PageFilter(expectedRows));
    verifyScanFull(s, Arrays.copyOf(expectedKVs, 6));

  }

  public void testPageFilterWithReverseScan() throws Exception {
    // KVs in first 6 rows
    KeyValue[] expectedKVs = {
        // testRowOne-0
        new KeyValue(ROWS_ONE[0], FAMILIES[0], QUALIFIERS_ONE[0], VALUES[0]),
        new KeyValue(ROWS_ONE[0], FAMILIES[0], QUALIFIERS_ONE[2], VALUES[0]),
        new KeyValue(ROWS_ONE[0], FAMILIES[0], QUALIFIERS_ONE[3], VALUES[0]),
        new KeyValue(ROWS_ONE[0], FAMILIES[1], QUALIFIERS_ONE[0], VALUES[0]),
        new KeyValue(ROWS_ONE[0], FAMILIES[1], QUALIFIERS_ONE[2], VALUES[0]),
        new KeyValue(ROWS_ONE[0], FAMILIES[1], QUALIFIERS_ONE[3], VALUES[0]),
        // testRowOne-2
        new KeyValue(ROWS_ONE[2], FAMILIES[0], QUALIFIERS_ONE[0], VALUES[0]),
        new KeyValue(ROWS_ONE[2], FAMILIES[0], QUALIFIERS_ONE[2], VALUES[0]),
        new KeyValue(ROWS_ONE[2], FAMILIES[0], QUALIFIERS_ONE[3], VALUES[0]),
        new KeyValue(ROWS_ONE[2], FAMILIES[1], QUALIFIERS_ONE[0], VALUES[0]),
        new KeyValue(ROWS_ONE[2], FAMILIES[1], QUALIFIERS_ONE[2], VALUES[0]),
        new KeyValue(ROWS_ONE[2], FAMILIES[1], QUALIFIERS_ONE[3], VALUES[0]),
        // testRowOne-3
        new KeyValue(ROWS_ONE[3], FAMILIES[0], QUALIFIERS_ONE[0], VALUES[0]),
        new KeyValue(ROWS_ONE[3], FAMILIES[0], QUALIFIERS_ONE[2], VALUES[0]),
        new KeyValue(ROWS_ONE[3], FAMILIES[0], QUALIFIERS_ONE[3], VALUES[0]),
        new KeyValue(ROWS_ONE[3], FAMILIES[1], QUALIFIERS_ONE[0], VALUES[0]),
        new KeyValue(ROWS_ONE[3], FAMILIES[1], QUALIFIERS_ONE[2], VALUES[0]),
        new KeyValue(ROWS_ONE[3], FAMILIES[1], QUALIFIERS_ONE[3], VALUES[0]),
        // testRowTwo-0
        new KeyValue(ROWS_TWO[0], FAMILIES[0], QUALIFIERS_TWO[0], VALUES[1]),
        new KeyValue(ROWS_TWO[0], FAMILIES[0], QUALIFIERS_TWO[2], VALUES[1]),
        new KeyValue(ROWS_TWO[0], FAMILIES[0], QUALIFIERS_TWO[3], VALUES[1]),
        new KeyValue(ROWS_TWO[0], FAMILIES[1], QUALIFIERS_TWO[0], VALUES[1]),
        new KeyValue(ROWS_TWO[0], FAMILIES[1], QUALIFIERS_TWO[2], VALUES[1]),
        new KeyValue(ROWS_TWO[0], FAMILIES[1], QUALIFIERS_TWO[3], VALUES[1]),
        // testRowTwo-2
        new KeyValue(ROWS_TWO[2], FAMILIES[0], QUALIFIERS_TWO[0], VALUES[1]),
        new KeyValue(ROWS_TWO[2], FAMILIES[0], QUALIFIERS_TWO[2], VALUES[1]),
        new KeyValue(ROWS_TWO[2], FAMILIES[0], QUALIFIERS_TWO[3], VALUES[1]),
        new KeyValue(ROWS_TWO[2], FAMILIES[1], QUALIFIERS_TWO[0], VALUES[1]),
        new KeyValue(ROWS_TWO[2], FAMILIES[1], QUALIFIERS_TWO[2], VALUES[1]),
        new KeyValue(ROWS_TWO[2], FAMILIES[1], QUALIFIERS_TWO[3], VALUES[1]),
        // testRowTwo-3
        new KeyValue(ROWS_TWO[3], FAMILIES[0], QUALIFIERS_TWO[0], VALUES[1]),
        new KeyValue(ROWS_TWO[3], FAMILIES[0], QUALIFIERS_TWO[2], VALUES[1]),
        new KeyValue(ROWS_TWO[3], FAMILIES[0], QUALIFIERS_TWO[3], VALUES[1]),
        new KeyValue(ROWS_TWO[3], FAMILIES[1], QUALIFIERS_TWO[0], VALUES[1]),
        new KeyValue(ROWS_TWO[3], FAMILIES[1], QUALIFIERS_TWO[2], VALUES[1]),
        new KeyValue(ROWS_TWO[3], FAMILIES[1], QUALIFIERS_TWO[3], VALUES[1]) };

    // Grab all 6 rows
    long expectedRows = 6;
    long expectedKeys = this.colsPerRow;
    Scan s = new Scan();
    s.setReversed(true);
    s.setFilter(new PageFilter(expectedRows));
    verifyScan(s, expectedRows, expectedKeys);

    // Grab first 4 rows (6 cols per row)
    expectedRows = 4;
    expectedKeys = this.colsPerRow;
    s = new Scan();
    s.setReversed(true);
    s.setFilter(new PageFilter(expectedRows));
    verifyScan(s, expectedRows, expectedKeys);

    // Grab first 2 rows
    expectedRows = 2;
    expectedKeys = this.colsPerRow;
    s = new Scan();
    s.setReversed(true);
    s.setFilter(new PageFilter(expectedRows));
    verifyScan(s, expectedRows, expectedKeys);

    // Grab first row
    expectedRows = 1;
    expectedKeys = this.colsPerRow;
    s = new Scan();
    s.setReversed(true);
    s.setFilter(new PageFilter(expectedRows));
    verifyScan(s, expectedRows, expectedKeys);
  }

  public void testWhileMatchFilterWithFilterRowWithReverseScan()
      throws Exception {
    final int pageSize = 4;

    Scan s = new Scan();
    s.setReversed(true);
    WhileMatchFilter filter = new WhileMatchFilter(new PageFilter(pageSize));
    s.setFilter(filter);

    InternalScanner scanner = this.region.getScanner(s);
    int scannerCounter = 0;
    while (true) {
      boolean isMoreResults = scanner.next(new ArrayList<Cell>());
      scannerCounter++;

      if (scannerCounter >= pageSize) {
        Assert.assertTrue(
            "The WhileMatchFilter should now filter all remaining",
            filter.filterAllRemaining());
      }
      if (!isMoreResults) {
        break;
      }
    }
    scanner.close();
    Assert.assertEquals("The page filter returned more rows than expected",
        pageSize, scannerCounter);
  }

  public void testWhileMatchFilterWithFilterRowKeyWithReverseScan()
      throws Exception {
    Scan s = new Scan();
    String prefix = "testRowOne";
    WhileMatchFilter filter = new WhileMatchFilter(new PrefixFilter(
        Bytes.toBytes(prefix)));
    s.setFilter(filter);
    s.setReversed(true);

    InternalScanner scanner = this.region.getScanner(s);
    while (true) {
      ArrayList<Cell> values = new ArrayList<Cell>();
      boolean isMoreResults = scanner.next(values);
      if (!isMoreResults
          || !Bytes.toString(CellUtil.cloneRow(values.get(0))).startsWith(prefix)) {
        Assert.assertTrue(
            "The WhileMatchFilter should now filter all remaining",
            filter.filterAllRemaining());
      }
      if (!isMoreResults) {
        break;
      }
    }
    scanner.close();
  }

  /**
   * Tests the the {@link WhileMatchFilter} works in combination with a
   * {@link Filter} that uses the
   * {@link Filter#filterRow()} method.
   *
   * See HBASE-2258.
   *
   * @throws Exception
   */
  @Test
  public void testWhileMatchFilterWithFilterRow() throws Exception {
    final int pageSize = 4;

    Scan s = new Scan();
    WhileMatchFilter filter = new WhileMatchFilter(new PageFilter(pageSize));
    s.setFilter(filter);

    InternalScanner scanner = this.region.getScanner(s);
    int scannerCounter = 0;
    while (true) {
      boolean isMoreResults = scanner.next(new ArrayList<Cell>());
      scannerCounter++;

      if (scannerCounter >= pageSize) {
        assertTrue("The WhileMatchFilter should now filter all remaining", filter.filterAllRemaining());
      }
      if (!isMoreResults) {
        break;
      }
    }
    assertEquals("The page filter returned more rows than expected", pageSize, scannerCounter);
  }


  /**
   * The following filter simulates a pre-0.96 filter where filterRow() is defined while
   * hasFilterRow() returns false
   */
  static class OldTestFilter extends FilterBase {
    @Override
    public byte [] toByteArray() {return null;}

    @Override
    public boolean hasFilterRow() {
      return false;
    }

    @Override
    public boolean filterRow() {
      // always filter out rows
      return true;
    }

    @Override
    public ReturnCode filterKeyValue(Cell ignored) throws IOException {
      return ReturnCode.INCLUDE;
    }
  }

  /**
   * The following test is to ensure old(such as hbase0.94) filterRow() can be correctly fired in
   * 0.96+ code base.
   *
   * See HBASE-10366
   *
   * @throws Exception
   */
  @Test
  public void tes94FilterRowCompatibility() throws Exception {
    Scan s = new Scan();
    OldTestFilter filter = new OldTestFilter();
    s.setFilter(filter);

    InternalScanner scanner = this.region.getScanner(s);
    ArrayList<Cell> values = new ArrayList<Cell>();
    scanner.next(values);
    assertTrue("All rows should be filtered out", values.isEmpty());
  }

  /**
   * Tests the the {@link WhileMatchFilter} works in combination with a
   * {@link Filter} that uses the
   * {@link Filter#filterRowKey(Cell)} method.
   *
   * See HBASE-2258.
   *
   * @throws Exception
   */
  @Test
  public void testWhileMatchFilterWithFilterRowKey() throws Exception {
    Scan s = new Scan();
    String prefix = "testRowOne";
    WhileMatchFilter filter = new WhileMatchFilter(new PrefixFilter(Bytes.toBytes(prefix)));
    s.setFilter(filter);

    InternalScanner scanner = this.region.getScanner(s);
    while (true) {
      ArrayList<Cell> values = new ArrayList<Cell>();
      boolean isMoreResults = scanner.next(values);
      if (!isMoreResults || !Bytes.toString(CellUtil.cloneRow(values.get(0))).startsWith(prefix)) {
        assertTrue("The WhileMatchFilter should now filter all remaining", filter.filterAllRemaining());
      }
      if (!isMoreResults) {
        break;
      }
    }
  }

  /**
   * Tests the the {@link WhileMatchFilter} works in combination with a
   * {@link Filter} that uses the {@link Filter#filterKeyValue(Cell)} method.
   *
   * See HBASE-2258.
   *
   * @throws Exception
   */
  @Test
  public void testWhileMatchFilterWithFilterKeyValue() throws Exception {
    Scan s = new Scan();
    WhileMatchFilter filter = new WhileMatchFilter(
        new SingleColumnValueFilter(FAMILIES[0], QUALIFIERS_ONE[0], CompareOp.EQUAL, Bytes.toBytes("foo"))
    );
    s.setFilter(filter);

    InternalScanner scanner = this.region.getScanner(s);
    while (true) {
      ArrayList<Cell> values = new ArrayList<Cell>();
      boolean isMoreResults = scanner.next(values);
      assertTrue("The WhileMatchFilter should now filter all remaining", filter.filterAllRemaining());
      if (!isMoreResults) {
        break;
      }
    }
  }

  @Test
  public void testInclusiveStopFilter() throws IOException {

    // Grab rows from group one

    // If we just use start/stop row, we get total/2 - 1 rows
    long expectedRows = (this.numRows / 2) - 1;
    long expectedKeys = this.colsPerRow;
    Scan s = new Scan(Bytes.toBytes("testRowOne-0"),
        Bytes.toBytes("testRowOne-3"));
    verifyScan(s, expectedRows, expectedKeys);

    // Now use start row with inclusive stop filter
    expectedRows = this.numRows / 2;
    s = new Scan(Bytes.toBytes("testRowOne-0"));
    s.setFilter(new InclusiveStopFilter(Bytes.toBytes("testRowOne-3")));
    verifyScan(s, expectedRows, expectedKeys);

    // Grab rows from group two

    // If we just use start/stop row, we get total/2 - 1 rows
    expectedRows = (this.numRows / 2) - 1;
    expectedKeys = this.colsPerRow;
    s = new Scan(Bytes.toBytes("testRowTwo-0"),
        Bytes.toBytes("testRowTwo-3"));
    verifyScan(s, expectedRows, expectedKeys);

    // Now use start row with inclusive stop filter
    expectedRows = this.numRows / 2;
    s = new Scan(Bytes.toBytes("testRowTwo-0"));
    s.setFilter(new InclusiveStopFilter(Bytes.toBytes("testRowTwo-3")));
    verifyScan(s, expectedRows, expectedKeys);

  }

  @Test
  public void testInclusiveStopFilterWithReverseScan() throws IOException {

    // Grab rows from group one

    // If we just use start/stop row, we get total/2 - 1 rows
    long expectedRows = (this.numRows / 2) - 1;
    long expectedKeys = this.colsPerRow;
    Scan s = new Scan(Bytes.toBytes("testRowOne-3"), Bytes.toBytes("testRowOne-0"));
    s.setReversed(true);
    verifyScan(s, expectedRows, expectedKeys);

    // Now use start row with inclusive stop filter
    expectedRows = this.numRows / 2;
    s = new Scan(Bytes.toBytes("testRowOne-3"));
    s.setReversed(true);
    s.setFilter(new InclusiveStopFilter(Bytes.toBytes("testRowOne-0")));
    verifyScan(s, expectedRows, expectedKeys);

    // Grab rows from group two

    // If we just use start/stop row, we get total/2 - 1 rows
    expectedRows = (this.numRows / 2) - 1;
    expectedKeys = this.colsPerRow;
    s = new Scan(Bytes.toBytes("testRowTwo-3"), Bytes.toBytes("testRowTwo-0"));
    s.setReversed(true);
    verifyScan(s, expectedRows, expectedKeys);

    // Now use start row with inclusive stop filter
    expectedRows = this.numRows / 2;
    s = new Scan(Bytes.toBytes("testRowTwo-3"));
    s.setReversed(true);
    s.setFilter(new InclusiveStopFilter(Bytes.toBytes("testRowTwo-0")));
    verifyScan(s, expectedRows, expectedKeys);

  }

  @Test
  public void testQualifierFilter() throws IOException {

    // Match two keys (one from each family) in half the rows
    long expectedRows = this.numRows / 2;
    long expectedKeys = 2;
    Filter f = new QualifierFilter(CompareOp.EQUAL,
        new BinaryComparator(Bytes.toBytes("testQualifierOne-2")));
    Scan s = new Scan();
    s.setFilter(f);
    verifyScanNoEarlyOut(s, expectedRows, expectedKeys);

    // Match keys less than same qualifier
    // Expect only two keys (one from each family) in half the rows
    expectedRows = this.numRows / 2;
    expectedKeys = 2;
    f = new QualifierFilter(CompareOp.LESS,
        new BinaryComparator(Bytes.toBytes("testQualifierOne-2")));
    s = new Scan();
    s.setFilter(f);
    verifyScanNoEarlyOut(s, expectedRows, expectedKeys);

    // Match keys less than or equal
    // Expect four keys (two from each family) in half the rows
    expectedRows = this.numRows / 2;
    expectedKeys = 4;
    f = new QualifierFilter(CompareOp.LESS_OR_EQUAL,
        new BinaryComparator(Bytes.toBytes("testQualifierOne-2")));
    s = new Scan();
    s.setFilter(f);
    verifyScanNoEarlyOut(s, expectedRows, expectedKeys);

    // Match keys not equal
    // Expect four keys (two from each family)
    // Only look in first group of rows
    expectedRows = this.numRows / 2;
    expectedKeys = 4;
    f = new QualifierFilter(CompareOp.NOT_EQUAL,
        new BinaryComparator(Bytes.toBytes("testQualifierOne-2")));
    s = new Scan(HConstants.EMPTY_START_ROW, Bytes.toBytes("testRowTwo"));
    s.setFilter(f);
    verifyScanNoEarlyOut(s, expectedRows, expectedKeys);

    // Match keys greater or equal
    // Expect four keys (two from each family)
    // Only look in first group of rows
    expectedRows = this.numRows / 2;
    expectedKeys = 4;
    f = new QualifierFilter(CompareOp.GREATER_OR_EQUAL,
        new BinaryComparator(Bytes.toBytes("testQualifierOne-2")));
    s = new Scan(HConstants.EMPTY_START_ROW, Bytes.toBytes("testRowTwo"));
    s.setFilter(f);
    verifyScanNoEarlyOut(s, expectedRows, expectedKeys);

    // Match keys greater
    // Expect two keys (one from each family)
    // Only look in first group of rows
    expectedRows = this.numRows / 2;
    expectedKeys = 2;
    f = new QualifierFilter(CompareOp.GREATER,
        new BinaryComparator(Bytes.toBytes("testQualifierOne-2")));
    s = new Scan(HConstants.EMPTY_START_ROW, Bytes.toBytes("testRowTwo"));
    s.setFilter(f);
    verifyScanNoEarlyOut(s, expectedRows, expectedKeys);

    // Match keys not equal to
    // Look across rows and fully validate the keys and ordering
    // Expect varied numbers of keys, 4 per row in group one, 6 per row in group two
    f = new QualifierFilter(CompareOp.NOT_EQUAL,
        new BinaryComparator(QUALIFIERS_ONE[2]));
    s = new Scan();
    s.setFilter(f);

    KeyValue [] kvs = {
        // testRowOne-0
        new KeyValue(ROWS_ONE[0], FAMILIES[0], QUALIFIERS_ONE[0], VALUES[0]),
        new KeyValue(ROWS_ONE[0], FAMILIES[0], QUALIFIERS_ONE[3], VALUES[0]),
        new KeyValue(ROWS_ONE[0], FAMILIES[1], QUALIFIERS_ONE[0], VALUES[0]),
        new KeyValue(ROWS_ONE[0], FAMILIES[1], QUALIFIERS_ONE[3], VALUES[0]),
        // testRowOne-2
        new KeyValue(ROWS_ONE[2], FAMILIES[0], QUALIFIERS_ONE[0], VALUES[0]),
        new KeyValue(ROWS_ONE[2], FAMILIES[0], QUALIFIERS_ONE[3], VALUES[0]),
        new KeyValue(ROWS_ONE[2], FAMILIES[1], QUALIFIERS_ONE[0], VALUES[0]),
        new KeyValue(ROWS_ONE[2], FAMILIES[1], QUALIFIERS_ONE[3], VALUES[0]),
        // testRowOne-3
        new KeyValue(ROWS_ONE[3], FAMILIES[0], QUALIFIERS_ONE[0], VALUES[0]),
        new KeyValue(ROWS_ONE[3], FAMILIES[0], QUALIFIERS_ONE[3], VALUES[0]),
        new KeyValue(ROWS_ONE[3], FAMILIES[1], QUALIFIERS_ONE[0], VALUES[0]),
        new KeyValue(ROWS_ONE[3], FAMILIES[1], QUALIFIERS_ONE[3], VALUES[0]),
        // testRowTwo-0
        new KeyValue(ROWS_TWO[0], FAMILIES[0], QUALIFIERS_TWO[0], VALUES[1]),
        new KeyValue(ROWS_TWO[0], FAMILIES[0], QUALIFIERS_TWO[2], VALUES[1]),
        new KeyValue(ROWS_TWO[0], FAMILIES[0], QUALIFIERS_TWO[3], VALUES[1]),
        new KeyValue(ROWS_TWO[0], FAMILIES[1], QUALIFIERS_TWO[0], VALUES[1]),
        new KeyValue(ROWS_TWO[0], FAMILIES[1], QUALIFIERS_TWO[2], VALUES[1]),
        new KeyValue(ROWS_TWO[0], FAMILIES[1], QUALIFIERS_TWO[3], VALUES[1]),
        // testRowTwo-2
        new KeyValue(ROWS_TWO[2], FAMILIES[0], QUALIFIERS_TWO[0], VALUES[1]),
        new KeyValue(ROWS_TWO[2], FAMILIES[0], QUALIFIERS_TWO[2], VALUES[1]),
        new KeyValue(ROWS_TWO[2], FAMILIES[0], QUALIFIERS_TWO[3], VALUES[1]),
        new KeyValue(ROWS_TWO[2], FAMILIES[1], QUALIFIERS_TWO[0], VALUES[1]),
        new KeyValue(ROWS_TWO[2], FAMILIES[1], QUALIFIERS_TWO[2], VALUES[1]),
        new KeyValue(ROWS_TWO[2], FAMILIES[1], QUALIFIERS_TWO[3], VALUES[1]),
        // testRowTwo-3
        new KeyValue(ROWS_TWO[3], FAMILIES[0], QUALIFIERS_TWO[0], VALUES[1]),
        new KeyValue(ROWS_TWO[3], FAMILIES[0], QUALIFIERS_TWO[2], VALUES[1]),
        new KeyValue(ROWS_TWO[3], FAMILIES[0], QUALIFIERS_TWO[3], VALUES[1]),
        new KeyValue(ROWS_TWO[3], FAMILIES[1], QUALIFIERS_TWO[0], VALUES[1]),
        new KeyValue(ROWS_TWO[3], FAMILIES[1], QUALIFIERS_TWO[2], VALUES[1]),
        new KeyValue(ROWS_TWO[3], FAMILIES[1], QUALIFIERS_TWO[3], VALUES[1]),
    };
    verifyScanFull(s, kvs);


    // Test across rows and groups with a regex
    // Filter out "test*-2"
    // Expect 4 keys per row across both groups
    f = new QualifierFilter(CompareOp.NOT_EQUAL,
        new RegexStringComparator("test.+-2"));
    s = new Scan();
    s.setFilter(f);

    kvs = new KeyValue [] {
        // testRowOne-0
        new KeyValue(ROWS_ONE[0], FAMILIES[0], QUALIFIERS_ONE[0], VALUES[0]),
        new KeyValue(ROWS_ONE[0], FAMILIES[0], QUALIFIERS_ONE[3], VALUES[0]),
        new KeyValue(ROWS_ONE[0], FAMILIES[1], QUALIFIERS_ONE[0], VALUES[0]),
        new KeyValue(ROWS_ONE[0], FAMILIES[1], QUALIFIERS_ONE[3], VALUES[0]),
        // testRowOne-2
        new KeyValue(ROWS_ONE[2], FAMILIES[0], QUALIFIERS_ONE[0], VALUES[0]),
        new KeyValue(ROWS_ONE[2], FAMILIES[0], QUALIFIERS_ONE[3], VALUES[0]),
        new KeyValue(ROWS_ONE[2], FAMILIES[1], QUALIFIERS_ONE[0], VALUES[0]),
        new KeyValue(ROWS_ONE[2], FAMILIES[1], QUALIFIERS_ONE[3], VALUES[0]),
        // testRowOne-3
        new KeyValue(ROWS_ONE[3], FAMILIES[0], QUALIFIERS_ONE[0], VALUES[0]),
        new KeyValue(ROWS_ONE[3], FAMILIES[0], QUALIFIERS_ONE[3], VALUES[0]),
        new KeyValue(ROWS_ONE[3], FAMILIES[1], QUALIFIERS_ONE[0], VALUES[0]),
        new KeyValue(ROWS_ONE[3], FAMILIES[1], QUALIFIERS_ONE[3], VALUES[0]),
        // testRowTwo-0
        new KeyValue(ROWS_TWO[0], FAMILIES[0], QUALIFIERS_TWO[0], VALUES[1]),
        new KeyValue(ROWS_TWO[0], FAMILIES[0], QUALIFIERS_TWO[3], VALUES[1]),
        new KeyValue(ROWS_TWO[0], FAMILIES[1], QUALIFIERS_TWO[0], VALUES[1]),
        new KeyValue(ROWS_TWO[0], FAMILIES[1], QUALIFIERS_TWO[3], VALUES[1]),
        // testRowTwo-2
        new KeyValue(ROWS_TWO[2], FAMILIES[0], QUALIFIERS_TWO[0], VALUES[1]),
        new KeyValue(ROWS_TWO[2], FAMILIES[0], QUALIFIERS_TWO[3], VALUES[1]),
        new KeyValue(ROWS_TWO[2], FAMILIES[1], QUALIFIERS_TWO[0], VALUES[1]),
        new KeyValue(ROWS_TWO[2], FAMILIES[1], QUALIFIERS_TWO[3], VALUES[1]),
        // testRowTwo-3
        new KeyValue(ROWS_TWO[3], FAMILIES[0], QUALIFIERS_TWO[0], VALUES[1]),
        new KeyValue(ROWS_TWO[3], FAMILIES[0], QUALIFIERS_TWO[3], VALUES[1]),
        new KeyValue(ROWS_TWO[3], FAMILIES[1], QUALIFIERS_TWO[0], VALUES[1]),
        new KeyValue(ROWS_TWO[3], FAMILIES[1], QUALIFIERS_TWO[3], VALUES[1]),
    };
    verifyScanFull(s, kvs);

  }

  @Test
  public void testFamilyFilter() throws IOException {

      // Match family, only half of columns returned.
      long expectedRows = this.numRows;
      long expectedKeys = this.colsPerRow / 2;
      Filter f = new FamilyFilter(CompareOp.EQUAL,
          new BinaryComparator(Bytes.toBytes("testFamilyOne")));
      Scan s = new Scan();
      s.setFilter(f);
      verifyScanNoEarlyOut(s, expectedRows, expectedKeys);

      // Match keys less than given family, should return nothing
      expectedRows = 0;
      expectedKeys = 0;
      f = new FamilyFilter(CompareOp.LESS,
          new BinaryComparator(Bytes.toBytes("testFamily")));
      s = new Scan();
      s.setFilter(f);
      verifyScanNoEarlyOut(s, expectedRows, expectedKeys);

      // Match keys less than or equal, should return half of columns
      expectedRows = this.numRows;
      expectedKeys = this.colsPerRow / 2;
      f = new FamilyFilter(CompareOp.LESS_OR_EQUAL,
          new BinaryComparator(Bytes.toBytes("testFamilyOne")));
      s = new Scan();
      s.setFilter(f);
      verifyScanNoEarlyOut(s, expectedRows, expectedKeys);

      // Match keys from second family
      // look only in second group of rows
      expectedRows = this.numRows / 2;
      expectedKeys = this.colsPerRow / 2;
      f = new FamilyFilter(CompareOp.NOT_EQUAL,
          new BinaryComparator(Bytes.toBytes("testFamilyOne")));
      s = new Scan(HConstants.EMPTY_START_ROW, Bytes.toBytes("testRowTwo"));
      s.setFilter(f);
      verifyScanNoEarlyOut(s, expectedRows, expectedKeys);

      // Match all columns
      // look only in second group of rows
      expectedRows = this.numRows / 2;
      expectedKeys = this.colsPerRow;
      f = new FamilyFilter(CompareOp.GREATER_OR_EQUAL,
          new BinaryComparator(Bytes.toBytes("testFamilyOne")));
      s = new Scan(HConstants.EMPTY_START_ROW, Bytes.toBytes("testRowTwo"));
      s.setFilter(f);
      verifyScanNoEarlyOut(s, expectedRows, expectedKeys);

      // Match all columns in second family
      // look only in second group of rows
      expectedRows = this.numRows / 2;
      expectedKeys = this.colsPerRow / 2;
      f = new FamilyFilter(CompareOp.GREATER,
          new BinaryComparator(Bytes.toBytes("testFamilyOne")));
      s = new Scan(HConstants.EMPTY_START_ROW, Bytes.toBytes("testRowTwo"));
      s.setFilter(f);
      verifyScanNoEarlyOut(s, expectedRows, expectedKeys);

      // Match keys not equal to given family
      // Look across rows and fully validate the keys and ordering
      f = new FamilyFilter(CompareOp.NOT_EQUAL,
          new BinaryComparator(FAMILIES[1]));
      s = new Scan();
      s.setFilter(f);

      KeyValue [] kvs = {
          // testRowOne-0
          new KeyValue(ROWS_ONE[0], FAMILIES[0], QUALIFIERS_ONE[0], VALUES[0]),
          new KeyValue(ROWS_ONE[0], FAMILIES[0], QUALIFIERS_ONE[2], VALUES[0]),
          new KeyValue(ROWS_ONE[0], FAMILIES[0], QUALIFIERS_ONE[3], VALUES[0]),
          // testRowOne-2
          new KeyValue(ROWS_ONE[2], FAMILIES[0], QUALIFIERS_ONE[0], VALUES[0]),
          new KeyValue(ROWS_ONE[2], FAMILIES[0], QUALIFIERS_ONE[2], VALUES[0]),
          new KeyValue(ROWS_ONE[2], FAMILIES[0], QUALIFIERS_ONE[3], VALUES[0]),
          // testRowOne-3
          new KeyValue(ROWS_ONE[3], FAMILIES[0], QUALIFIERS_ONE[0], VALUES[0]),
          new KeyValue(ROWS_ONE[3], FAMILIES[0], QUALIFIERS_ONE[2], VALUES[0]),
          new KeyValue(ROWS_ONE[3], FAMILIES[0], QUALIFIERS_ONE[3], VALUES[0]),
          // testRowTwo-0
          new KeyValue(ROWS_TWO[0], FAMILIES[0], QUALIFIERS_TWO[0], VALUES[1]),
          new KeyValue(ROWS_TWO[0], FAMILIES[0], QUALIFIERS_TWO[2], VALUES[1]),
          new KeyValue(ROWS_TWO[0], FAMILIES[0], QUALIFIERS_TWO[3], VALUES[1]),
          // testRowTwo-2
          new KeyValue(ROWS_TWO[2], FAMILIES[0], QUALIFIERS_TWO[0], VALUES[1]),
          new KeyValue(ROWS_TWO[2], FAMILIES[0], QUALIFIERS_TWO[2], VALUES[1]),
          new KeyValue(ROWS_TWO[2], FAMILIES[0], QUALIFIERS_TWO[3], VALUES[1]),
          // testRowTwo-3
          new KeyValue(ROWS_TWO[3], FAMILIES[0], QUALIFIERS_TWO[0], VALUES[1]),
          new KeyValue(ROWS_TWO[3], FAMILIES[0], QUALIFIERS_TWO[2], VALUES[1]),
          new KeyValue(ROWS_TWO[3], FAMILIES[0], QUALIFIERS_TWO[3], VALUES[1]),
      };
      verifyScanFull(s, kvs);


      // Test across rows and groups with a regex
      // Filter out "test*-2"
      // Expect 4 keys per row across both groups
      f = new FamilyFilter(CompareOp.NOT_EQUAL,
          new RegexStringComparator("test.*One"));
      s = new Scan();
      s.setFilter(f);

      kvs = new KeyValue [] {
          // testRowOne-0
          new KeyValue(ROWS_ONE[0], FAMILIES[1], QUALIFIERS_ONE[0], VALUES[0]),
          new KeyValue(ROWS_ONE[0], FAMILIES[1], QUALIFIERS_ONE[2], VALUES[0]),
          new KeyValue(ROWS_ONE[0], FAMILIES[1], QUALIFIERS_ONE[3], VALUES[0]),
          // testRowOne-2
          new KeyValue(ROWS_ONE[2], FAMILIES[1], QUALIFIERS_ONE[0], VALUES[0]),
          new KeyValue(ROWS_ONE[2], FAMILIES[1], QUALIFIERS_ONE[2], VALUES[0]),
          new KeyValue(ROWS_ONE[2], FAMILIES[1], QUALIFIERS_ONE[3], VALUES[0]),
          // testRowOne-3
          new KeyValue(ROWS_ONE[3], FAMILIES[1], QUALIFIERS_ONE[0], VALUES[0]),
          new KeyValue(ROWS_ONE[3], FAMILIES[1], QUALIFIERS_ONE[2], VALUES[0]),
          new KeyValue(ROWS_ONE[3], FAMILIES[1], QUALIFIERS_ONE[3], VALUES[0]),
          // testRowTwo-0
          new KeyValue(ROWS_TWO[0], FAMILIES[1], QUALIFIERS_TWO[0], VALUES[1]),
          new KeyValue(ROWS_TWO[0], FAMILIES[1], QUALIFIERS_TWO[2], VALUES[1]),
          new KeyValue(ROWS_TWO[0], FAMILIES[1], QUALIFIERS_TWO[3], VALUES[1]),
          // testRowTwo-2
          new KeyValue(ROWS_TWO[2], FAMILIES[1], QUALIFIERS_TWO[0], VALUES[1]),
          new KeyValue(ROWS_TWO[2], FAMILIES[1], QUALIFIERS_TWO[2], VALUES[1]),
          new KeyValue(ROWS_TWO[2], FAMILIES[1], QUALIFIERS_TWO[3], VALUES[1]),
          // testRowTwo-3
          new KeyValue(ROWS_TWO[3], FAMILIES[1], QUALIFIERS_TWO[0], VALUES[1]),
          new KeyValue(ROWS_TWO[3], FAMILIES[1], QUALIFIERS_TWO[2], VALUES[1]),
          new KeyValue(ROWS_TWO[3], FAMILIES[1], QUALIFIERS_TWO[3], VALUES[1]),
      };
      verifyScanFull(s, kvs);

    }


  @Test
  public void testRowFilter() throws IOException {

    // Match a single row, all keys
    long expectedRows = 1;
    long expectedKeys = this.colsPerRow;
    Filter f = new RowFilter(CompareOp.EQUAL,
        new BinaryComparator(Bytes.toBytes("testRowOne-2")));
    Scan s = new Scan();
    s.setFilter(f);
    verifyScanNoEarlyOut(s, expectedRows, expectedKeys);

    // Match a two rows, one from each group, using regex
    expectedRows = 2;
    expectedKeys = this.colsPerRow;
    f = new RowFilter(CompareOp.EQUAL,
        new RegexStringComparator("testRow.+-2"));
    s = new Scan();
    s.setFilter(f);
    verifyScanNoEarlyOut(s, expectedRows, expectedKeys);

    // Match rows less than
    // Expect all keys in one row
    expectedRows = 1;
    expectedKeys = this.colsPerRow;
    f = new RowFilter(CompareOp.LESS,
        new BinaryComparator(Bytes.toBytes("testRowOne-2")));
    s = new Scan();
    s.setFilter(f);
    verifyScanNoEarlyOut(s, expectedRows, expectedKeys);

    // Match rows less than or equal
    // Expect all keys in two rows
    expectedRows = 2;
    expectedKeys = this.colsPerRow;
    f = new RowFilter(CompareOp.LESS_OR_EQUAL,
        new BinaryComparator(Bytes.toBytes("testRowOne-2")));
    s = new Scan();
    s.setFilter(f);
    verifyScanNoEarlyOut(s, expectedRows, expectedKeys);

    // Match rows not equal
    // Expect all keys in all but one row
    expectedRows = this.numRows - 1;
    expectedKeys = this.colsPerRow;
    f = new RowFilter(CompareOp.NOT_EQUAL,
        new BinaryComparator(Bytes.toBytes("testRowOne-2")));
    s = new Scan();
    s.setFilter(f);
    verifyScanNoEarlyOut(s, expectedRows, expectedKeys);

    // Match keys greater or equal
    // Expect all keys in all but one row
    expectedRows = this.numRows - 1;
    expectedKeys = this.colsPerRow;
    f = new RowFilter(CompareOp.GREATER_OR_EQUAL,
        new BinaryComparator(Bytes.toBytes("testRowOne-2")));
    s = new Scan();
    s.setFilter(f);
    verifyScanNoEarlyOut(s, expectedRows, expectedKeys);

    // Match keys greater
    // Expect all keys in all but two rows
    expectedRows = this.numRows - 2;
    expectedKeys = this.colsPerRow;
    f = new RowFilter(CompareOp.GREATER,
        new BinaryComparator(Bytes.toBytes("testRowOne-2")));
    s = new Scan();
    s.setFilter(f);
    verifyScanNoEarlyOut(s, expectedRows, expectedKeys);

    // Match rows not equal to testRowTwo-2
    // Look across rows and fully validate the keys and ordering
    // Should see all keys in all rows but testRowTwo-2
    f = new RowFilter(CompareOp.NOT_EQUAL,
        new BinaryComparator(Bytes.toBytes("testRowOne-2")));
    s = new Scan();
    s.setFilter(f);

    KeyValue [] kvs = {
        // testRowOne-0
        new KeyValue(ROWS_ONE[0], FAMILIES[0], QUALIFIERS_ONE[0], VALUES[0]),
        new KeyValue(ROWS_ONE[0], FAMILIES[0], QUALIFIERS_ONE[2], VALUES[0]),
        new KeyValue(ROWS_ONE[0], FAMILIES[0], QUALIFIERS_ONE[3], VALUES[0]),
        new KeyValue(ROWS_ONE[0], FAMILIES[1], QUALIFIERS_ONE[0], VALUES[0]),
        new KeyValue(ROWS_ONE[0], FAMILIES[1], QUALIFIERS_ONE[2], VALUES[0]),
        new KeyValue(ROWS_ONE[0], FAMILIES[1], QUALIFIERS_ONE[3], VALUES[0]),
        // testRowOne-3
        new KeyValue(ROWS_ONE[3], FAMILIES[0], QUALIFIERS_ONE[0], VALUES[0]),
        new KeyValue(ROWS_ONE[3], FAMILIES[0], QUALIFIERS_ONE[2], VALUES[0]),
        new KeyValue(ROWS_ONE[3], FAMILIES[0], QUALIFIERS_ONE[3], VALUES[0]),
        new KeyValue(ROWS_ONE[3], FAMILIES[1], QUALIFIERS_ONE[0], VALUES[0]),
        new KeyValue(ROWS_ONE[3], FAMILIES[1], QUALIFIERS_ONE[2], VALUES[0]),
        new KeyValue(ROWS_ONE[3], FAMILIES[1], QUALIFIERS_ONE[3], VALUES[0]),
        // testRowTwo-0
        new KeyValue(ROWS_TWO[0], FAMILIES[0], QUALIFIERS_TWO[0], VALUES[1]),
        new KeyValue(ROWS_TWO[0], FAMILIES[0], QUALIFIERS_TWO[2], VALUES[1]),
        new KeyValue(ROWS_TWO[0], FAMILIES[0], QUALIFIERS_TWO[3], VALUES[1]),
        new KeyValue(ROWS_TWO[0], FAMILIES[1], QUALIFIERS_TWO[0], VALUES[1]),
        new KeyValue(ROWS_TWO[0], FAMILIES[1], QUALIFIERS_TWO[2], VALUES[1]),
        new KeyValue(ROWS_TWO[0], FAMILIES[1], QUALIFIERS_TWO[3], VALUES[1]),
        // testRowTwo-2
        new KeyValue(ROWS_TWO[2], FAMILIES[0], QUALIFIERS_TWO[0], VALUES[1]),
        new KeyValue(ROWS_TWO[2], FAMILIES[0], QUALIFIERS_TWO[2], VALUES[1]),
        new KeyValue(ROWS_TWO[2], FAMILIES[0], QUALIFIERS_TWO[3], VALUES[1]),
        new KeyValue(ROWS_TWO[2], FAMILIES[1], QUALIFIERS_TWO[0], VALUES[1]),
        new KeyValue(ROWS_TWO[2], FAMILIES[1], QUALIFIERS_TWO[2], VALUES[1]),
        new KeyValue(ROWS_TWO[2], FAMILIES[1], QUALIFIERS_TWO[3], VALUES[1]),
        // testRowTwo-3
        new KeyValue(ROWS_TWO[3], FAMILIES[0], QUALIFIERS_TWO[0], VALUES[1]),
        new KeyValue(ROWS_TWO[3], FAMILIES[0], QUALIFIERS_TWO[2], VALUES[1]),
        new KeyValue(ROWS_TWO[3], FAMILIES[0], QUALIFIERS_TWO[3], VALUES[1]),
        new KeyValue(ROWS_TWO[3], FAMILIES[1], QUALIFIERS_TWO[0], VALUES[1]),
        new KeyValue(ROWS_TWO[3], FAMILIES[1], QUALIFIERS_TWO[2], VALUES[1]),
        new KeyValue(ROWS_TWO[3], FAMILIES[1], QUALIFIERS_TWO[3], VALUES[1]),
    };
    verifyScanFull(s, kvs);


    // Test across rows and groups with a regex
    // Filter out everything that doesn't match "*-2"
    // Expect all keys in two rows
    f = new RowFilter(CompareOp.EQUAL,
        new RegexStringComparator(".+-2"));
    s = new Scan();
    s.setFilter(f);

    kvs = new KeyValue [] {
        // testRowOne-2
        new KeyValue(ROWS_ONE[2], FAMILIES[0], QUALIFIERS_ONE[0], VALUES[0]),
        new KeyValue(ROWS_ONE[2], FAMILIES[0], QUALIFIERS_ONE[2], VALUES[0]),
        new KeyValue(ROWS_ONE[2], FAMILIES[0], QUALIFIERS_ONE[3], VALUES[0]),
        new KeyValue(ROWS_ONE[2], FAMILIES[1], QUALIFIERS_ONE[0], VALUES[0]),
        new KeyValue(ROWS_ONE[2], FAMILIES[1], QUALIFIERS_ONE[2], VALUES[0]),
        new KeyValue(ROWS_ONE[2], FAMILIES[1], QUALIFIERS_ONE[3], VALUES[0]),
        // testRowTwo-2
        new KeyValue(ROWS_TWO[2], FAMILIES[0], QUALIFIERS_TWO[0], VALUES[1]),
        new KeyValue(ROWS_TWO[2], FAMILIES[0], QUALIFIERS_TWO[2], VALUES[1]),
        new KeyValue(ROWS_TWO[2], FAMILIES[0], QUALIFIERS_TWO[3], VALUES[1]),
        new KeyValue(ROWS_TWO[2], FAMILIES[1], QUALIFIERS_TWO[0], VALUES[1]),
        new KeyValue(ROWS_TWO[2], FAMILIES[1], QUALIFIERS_TWO[2], VALUES[1]),
        new KeyValue(ROWS_TWO[2], FAMILIES[1], QUALIFIERS_TWO[3], VALUES[1])
    };
    verifyScanFull(s, kvs);

  }

  @Test
  public void testValueFilter() throws IOException {

    // Match group one rows
    long expectedRows = this.numRows / 2;
    long expectedKeys = this.colsPerRow;
    Filter f = new ValueFilter(CompareOp.EQUAL,
        new BinaryComparator(Bytes.toBytes("testValueOne")));
    Scan s = new Scan();
    s.setFilter(f);
    verifyScanNoEarlyOut(s, expectedRows, expectedKeys);

    // Match group two rows
    expectedRows = this.numRows / 2;
    expectedKeys = this.colsPerRow;
    f = new ValueFilter(CompareOp.EQUAL,
        new BinaryComparator(Bytes.toBytes("testValueTwo")));
    s = new Scan();
    s.setFilter(f);
    verifyScanNoEarlyOut(s, expectedRows, expectedKeys);

    // Match all values using regex
    expectedRows = this.numRows;
    expectedKeys = this.colsPerRow;
    f = new ValueFilter(CompareOp.EQUAL,
        new RegexStringComparator("testValue((One)|(Two))"));
    s = new Scan();
    s.setFilter(f);
    verifyScanNoEarlyOut(s, expectedRows, expectedKeys);

    // Match values less than
    // Expect group one rows
    expectedRows = this.numRows / 2;
    expectedKeys = this.colsPerRow;
    f = new ValueFilter(CompareOp.LESS,
        new BinaryComparator(Bytes.toBytes("testValueTwo")));
    s = new Scan();
    s.setFilter(f);
    verifyScanNoEarlyOut(s, expectedRows, expectedKeys);

    // Match values less than or equal
    // Expect all rows
    expectedRows = this.numRows;
    expectedKeys = this.colsPerRow;
    f = new ValueFilter(CompareOp.LESS_OR_EQUAL,
        new BinaryComparator(Bytes.toBytes("testValueTwo")));
    s = new Scan();
    s.setFilter(f);
    verifyScanNoEarlyOut(s, expectedRows, expectedKeys);

    // Match values less than or equal
    // Expect group one rows
    expectedRows = this.numRows / 2;
    expectedKeys = this.colsPerRow;
    f = new ValueFilter(CompareOp.LESS_OR_EQUAL,
        new BinaryComparator(Bytes.toBytes("testValueOne")));
    s = new Scan();
    s.setFilter(f);
    verifyScanNoEarlyOut(s, expectedRows, expectedKeys);

    // Match values not equal
    // Expect half the rows
    expectedRows = this.numRows / 2;
    expectedKeys = this.colsPerRow;
    f = new ValueFilter(CompareOp.NOT_EQUAL,
        new BinaryComparator(Bytes.toBytes("testValueOne")));
    s = new Scan();
    s.setFilter(f);
    verifyScanNoEarlyOut(s, expectedRows, expectedKeys);

    // Match values greater or equal
    // Expect all rows
    expectedRows = this.numRows;
    expectedKeys = this.colsPerRow;
    f = new ValueFilter(CompareOp.GREATER_OR_EQUAL,
        new BinaryComparator(Bytes.toBytes("testValueOne")));
    s = new Scan();
    s.setFilter(f);
    verifyScanNoEarlyOut(s, expectedRows, expectedKeys);

    // Match values greater
    // Expect half rows
    expectedRows = this.numRows / 2;
    expectedKeys = this.colsPerRow;
    f = new ValueFilter(CompareOp.GREATER,
        new BinaryComparator(Bytes.toBytes("testValueOne")));
    s = new Scan();
    s.setFilter(f);
    verifyScanNoEarlyOut(s, expectedRows, expectedKeys);

    // Match values not equal to testValueOne
    // Look across rows and fully validate the keys and ordering
    // Should see all keys in all group two rows
    f = new ValueFilter(CompareOp.NOT_EQUAL,
        new BinaryComparator(Bytes.toBytes("testValueOne")));
    s = new Scan();
    s.setFilter(f);

    KeyValue [] kvs = {
        // testRowTwo-0
        new KeyValue(ROWS_TWO[0], FAMILIES[0], QUALIFIERS_TWO[0], VALUES[1]),
        new KeyValue(ROWS_TWO[0], FAMILIES[0], QUALIFIERS_TWO[2], VALUES[1]),
        new KeyValue(ROWS_TWO[0], FAMILIES[0], QUALIFIERS_TWO[3], VALUES[1]),
        new KeyValue(ROWS_TWO[0], FAMILIES[1], QUALIFIERS_TWO[0], VALUES[1]),
        new KeyValue(ROWS_TWO[0], FAMILIES[1], QUALIFIERS_TWO[2], VALUES[1]),
        new KeyValue(ROWS_TWO[0], FAMILIES[1], QUALIFIERS_TWO[3], VALUES[1]),
        // testRowTwo-2
        new KeyValue(ROWS_TWO[2], FAMILIES[0], QUALIFIERS_TWO[0], VALUES[1]),
        new KeyValue(ROWS_TWO[2], FAMILIES[0], QUALIFIERS_TWO[2], VALUES[1]),
        new KeyValue(ROWS_TWO[2], FAMILIES[0], QUALIFIERS_TWO[3], VALUES[1]),
        new KeyValue(ROWS_TWO[2], FAMILIES[1], QUALIFIERS_TWO[0], VALUES[1]),
        new KeyValue(ROWS_TWO[2], FAMILIES[1], QUALIFIERS_TWO[2], VALUES[1]),
        new KeyValue(ROWS_TWO[2], FAMILIES[1], QUALIFIERS_TWO[3], VALUES[1]),
        // testRowTwo-3
        new KeyValue(ROWS_TWO[3], FAMILIES[0], QUALIFIERS_TWO[0], VALUES[1]),
        new KeyValue(ROWS_TWO[3], FAMILIES[0], QUALIFIERS_TWO[2], VALUES[1]),
        new KeyValue(ROWS_TWO[3], FAMILIES[0], QUALIFIERS_TWO[3], VALUES[1]),
        new KeyValue(ROWS_TWO[3], FAMILIES[1], QUALIFIERS_TWO[0], VALUES[1]),
        new KeyValue(ROWS_TWO[3], FAMILIES[1], QUALIFIERS_TWO[2], VALUES[1]),
        new KeyValue(ROWS_TWO[3], FAMILIES[1], QUALIFIERS_TWO[3], VALUES[1]),
    };
    verifyScanFull(s, kvs);
  }

  @Test
  public void testSkipFilter() throws IOException {

    // Test for qualifier regex: "testQualifierOne-2"
    // Should only get rows from second group, and all keys
    Filter f = new SkipFilter(new QualifierFilter(CompareOp.NOT_EQUAL,
        new BinaryComparator(Bytes.toBytes("testQualifierOne-2"))));
    Scan s = new Scan();
    s.setFilter(f);

    KeyValue [] kvs = {
        // testRowTwo-0
        new KeyValue(ROWS_TWO[0], FAMILIES[0], QUALIFIERS_TWO[0], VALUES[1]),
        new KeyValue(ROWS_TWO[0], FAMILIES[0], QUALIFIERS_TWO[2], VALUES[1]),
        new KeyValue(ROWS_TWO[0], FAMILIES[0], QUALIFIERS_TWO[3], VALUES[1]),
        new KeyValue(ROWS_TWO[0], FAMILIES[1], QUALIFIERS_TWO[0], VALUES[1]),
        new KeyValue(ROWS_TWO[0], FAMILIES[1], QUALIFIERS_TWO[2], VALUES[1]),
        new KeyValue(ROWS_TWO[0], FAMILIES[1], QUALIFIERS_TWO[3], VALUES[1]),
        // testRowTwo-2
        new KeyValue(ROWS_TWO[2], FAMILIES[0], QUALIFIERS_TWO[0], VALUES[1]),
        new KeyValue(ROWS_TWO[2], FAMILIES[0], QUALIFIERS_TWO[2], VALUES[1]),
        new KeyValue(ROWS_TWO[2], FAMILIES[0], QUALIFIERS_TWO[3], VALUES[1]),
        new KeyValue(ROWS_TWO[2], FAMILIES[1], QUALIFIERS_TWO[0], VALUES[1]),
        new KeyValue(ROWS_TWO[2], FAMILIES[1], QUALIFIERS_TWO[2], VALUES[1]),
        new KeyValue(ROWS_TWO[2], FAMILIES[1], QUALIFIERS_TWO[3], VALUES[1]),
        // testRowTwo-3
        new KeyValue(ROWS_TWO[3], FAMILIES[0], QUALIFIERS_TWO[0], VALUES[1]),
        new KeyValue(ROWS_TWO[3], FAMILIES[0], QUALIFIERS_TWO[2], VALUES[1]),
        new KeyValue(ROWS_TWO[3], FAMILIES[0], QUALIFIERS_TWO[3], VALUES[1]),
        new KeyValue(ROWS_TWO[3], FAMILIES[1], QUALIFIERS_TWO[0], VALUES[1]),
        new KeyValue(ROWS_TWO[3], FAMILIES[1], QUALIFIERS_TWO[2], VALUES[1]),
        new KeyValue(ROWS_TWO[3], FAMILIES[1], QUALIFIERS_TWO[3], VALUES[1]),
    };
    verifyScanFull(s, kvs);
  }

  // TODO: This is important... need many more tests for ordering, etc
  // There are limited tests elsewhere but we need HRegion level ones here
  @Test
  public void testFilterList() throws IOException {

    // Test getting a single row, single key using Row, Qualifier, and Value
    // regular expression and substring filters
    // Use must pass all
    List<Filter> filters = new ArrayList<Filter>();
    filters.add(new RowFilter(CompareOp.EQUAL, new RegexStringComparator(".+-2")));
    filters.add(new QualifierFilter(CompareOp.EQUAL, new RegexStringComparator(".+-2")));
    filters.add(new ValueFilter(CompareOp.EQUAL, new SubstringComparator("One")));
    Filter f = new FilterList(Operator.MUST_PASS_ALL, filters);
    Scan s = new Scan();
    s.addFamily(FAMILIES[0]);
    s.setFilter(f);
    KeyValue [] kvs = {
        new KeyValue(ROWS_ONE[2], FAMILIES[0], QUALIFIERS_ONE[2], VALUES[0])
    };
    verifyScanFull(s, kvs);

    // Test getting everything with a MUST_PASS_ONE filter including row, qf, val
    // regular expression and substring filters
    filters.clear();
    filters.add(new RowFilter(CompareOp.EQUAL, new RegexStringComparator(".+Two.+")));
    filters.add(new QualifierFilter(CompareOp.EQUAL, new RegexStringComparator(".+-2")));
    filters.add(new ValueFilter(CompareOp.EQUAL, new SubstringComparator("One")));
    f = new FilterList(Operator.MUST_PASS_ONE, filters);
    s = new Scan();
    s.setFilter(f);
    verifyScanNoEarlyOut(s, this.numRows, this.colsPerRow);


  }

  @Test
  public void testFirstKeyOnlyFilter() throws IOException {
    Scan s = new Scan();
    s.setFilter(new FirstKeyOnlyFilter());
    // Expected KVs, the first KV from each of the remaining 6 rows
    KeyValue [] kvs = {
        new KeyValue(ROWS_ONE[0], FAMILIES[0], QUALIFIERS_ONE[0], VALUES[0]),
        new KeyValue(ROWS_ONE[2], FAMILIES[0], QUALIFIERS_ONE[0], VALUES[0]),
        new KeyValue(ROWS_ONE[3], FAMILIES[0], QUALIFIERS_ONE[0], VALUES[0]),
        new KeyValue(ROWS_TWO[0], FAMILIES[0], QUALIFIERS_TWO[0], VALUES[1]),
        new KeyValue(ROWS_TWO[2], FAMILIES[0], QUALIFIERS_TWO[0], VALUES[1]),
        new KeyValue(ROWS_TWO[3], FAMILIES[0], QUALIFIERS_TWO[0], VALUES[1])
    };
    verifyScanFull(s, kvs);
  }

  @Test
  public void testFilterListWithSingleColumnValueFilter() throws IOException {
    // Test for HBASE-3191

    // Scan using SingleColumnValueFilter
    SingleColumnValueFilter f1 = new SingleColumnValueFilter(FAMILIES[0], QUALIFIERS_ONE[0],
          CompareOp.EQUAL, VALUES[0]);
    f1.setFilterIfMissing( true );
    Scan s1 = new Scan();
    s1.addFamily(FAMILIES[0]);
    s1.setFilter(f1);
    KeyValue [] kvs1 = {
        new KeyValue(ROWS_ONE[0], FAMILIES[0], QUALIFIERS_ONE[0], VALUES[0]),
        new KeyValue(ROWS_ONE[0], FAMILIES[0], QUALIFIERS_ONE[2], VALUES[0]),
        new KeyValue(ROWS_ONE[0], FAMILIES[0], QUALIFIERS_ONE[3], VALUES[0]),
        new KeyValue(ROWS_ONE[2], FAMILIES[0], QUALIFIERS_ONE[0], VALUES[0]),
        new KeyValue(ROWS_ONE[2], FAMILIES[0], QUALIFIERS_ONE[2], VALUES[0]),
        new KeyValue(ROWS_ONE[2], FAMILIES[0], QUALIFIERS_ONE[3], VALUES[0]),
        new KeyValue(ROWS_ONE[3], FAMILIES[0], QUALIFIERS_ONE[0], VALUES[0]),
        new KeyValue(ROWS_ONE[3], FAMILIES[0], QUALIFIERS_ONE[2], VALUES[0]),
        new KeyValue(ROWS_ONE[3], FAMILIES[0], QUALIFIERS_ONE[3], VALUES[0]),
    };
    verifyScanNoEarlyOut(s1, 3, 3);
    verifyScanFull(s1, kvs1);

    // Scan using another SingleColumnValueFilter, expect disjoint result
    SingleColumnValueFilter f2 = new SingleColumnValueFilter(FAMILIES[0], QUALIFIERS_TWO[0],
        CompareOp.EQUAL, VALUES[1]);
    f2.setFilterIfMissing( true );
    Scan s2 = new Scan();
    s2.addFamily(FAMILIES[0]);
    s2.setFilter(f2);
    KeyValue [] kvs2 = {
        new KeyValue(ROWS_TWO[0], FAMILIES[0], QUALIFIERS_TWO[0], VALUES[1]),
        new KeyValue(ROWS_TWO[0], FAMILIES[0], QUALIFIERS_TWO[2], VALUES[1]),
        new KeyValue(ROWS_TWO[0], FAMILIES[0], QUALIFIERS_TWO[3], VALUES[1]),
        new KeyValue(ROWS_TWO[2], FAMILIES[0], QUALIFIERS_TWO[0], VALUES[1]),
        new KeyValue(ROWS_TWO[2], FAMILIES[0], QUALIFIERS_TWO[2], VALUES[1]),
        new KeyValue(ROWS_TWO[2], FAMILIES[0], QUALIFIERS_TWO[3], VALUES[1]),
        new KeyValue(ROWS_TWO[3], FAMILIES[0], QUALIFIERS_TWO[0], VALUES[1]),
        new KeyValue(ROWS_TWO[3], FAMILIES[0], QUALIFIERS_TWO[2], VALUES[1]),
        new KeyValue(ROWS_TWO[3], FAMILIES[0], QUALIFIERS_TWO[3], VALUES[1]),
    };
    verifyScanNoEarlyOut(s2, 3, 3);
    verifyScanFull(s2, kvs2);

    // Scan, ORing the two previous filters, expect unified result
    FilterList f = new FilterList(Operator.MUST_PASS_ONE);
    f.addFilter(f1);
    f.addFilter(f2);
    Scan s = new Scan();
    s.addFamily(FAMILIES[0]);
    s.setFilter(f);
    KeyValue [] kvs = {
        new KeyValue(ROWS_ONE[0], FAMILIES[0], QUALIFIERS_ONE[0], VALUES[0]),
        new KeyValue(ROWS_ONE[0], FAMILIES[0], QUALIFIERS_ONE[2], VALUES[0]),
        new KeyValue(ROWS_ONE[0], FAMILIES[0], QUALIFIERS_ONE[3], VALUES[0]),
        new KeyValue(ROWS_ONE[2], FAMILIES[0], QUALIFIERS_ONE[0], VALUES[0]),
        new KeyValue(ROWS_ONE[2], FAMILIES[0], QUALIFIERS_ONE[2], VALUES[0]),
        new KeyValue(ROWS_ONE[2], FAMILIES[0], QUALIFIERS_ONE[3], VALUES[0]),
        new KeyValue(ROWS_ONE[3], FAMILIES[0], QUALIFIERS_ONE[0], VALUES[0]),
        new KeyValue(ROWS_ONE[3], FAMILIES[0], QUALIFIERS_ONE[2], VALUES[0]),
        new KeyValue(ROWS_ONE[3], FAMILIES[0], QUALIFIERS_ONE[3], VALUES[0]),
        new KeyValue(ROWS_TWO[0], FAMILIES[0], QUALIFIERS_TWO[0], VALUES[1]),
        new KeyValue(ROWS_TWO[0], FAMILIES[0], QUALIFIERS_TWO[2], VALUES[1]),
        new KeyValue(ROWS_TWO[0], FAMILIES[0], QUALIFIERS_TWO[3], VALUES[1]),
        new KeyValue(ROWS_TWO[2], FAMILIES[0], QUALIFIERS_TWO[0], VALUES[1]),
        new KeyValue(ROWS_TWO[2], FAMILIES[0], QUALIFIERS_TWO[2], VALUES[1]),
        new KeyValue(ROWS_TWO[2], FAMILIES[0], QUALIFIERS_TWO[3], VALUES[1]),
        new KeyValue(ROWS_TWO[3], FAMILIES[0], QUALIFIERS_TWO[0], VALUES[1]),
        new KeyValue(ROWS_TWO[3], FAMILIES[0], QUALIFIERS_TWO[2], VALUES[1]),
        new KeyValue(ROWS_TWO[3], FAMILIES[0], QUALIFIERS_TWO[3], VALUES[1]),
    };
    verifyScanNoEarlyOut(s, 6, 3);
    verifyScanFull(s, kvs);
  }

  // HBASE-9747
  @Test
  public void testFilterListWithPrefixFilter() throws IOException {
    byte[] family = Bytes.toBytes("f1");
    byte[] qualifier = Bytes.toBytes("q1");
    HTableDescriptor htd = new HTableDescriptor(TableName.valueOf("TestFilter"));
    htd.addFamily(new HColumnDescriptor(family));
    HRegionInfo info = new HRegionInfo(htd.getTableName(), null, null, false);
    Region testRegion = HBaseTestingUtility.createRegionAndWAL(info, TEST_UTIL.getDataTestDir(),
        TEST_UTIL.getConfiguration(), htd);

    for(int i=0; i<5; i++) {
      Put p = new Put(Bytes.toBytes((char)('a'+i) + "row"));
      p.setDurability(Durability.SKIP_WAL);
      p.addColumn(family, qualifier, Bytes.toBytes(String.valueOf(111 + i)));
      testRegion.put(p);
    }
    testRegion.flush(true);

    // rows starting with "b"
    PrefixFilter pf = new PrefixFilter(new byte[] {'b'}) ;
    // rows with value of column 'q1' set to '113'
    SingleColumnValueFilter scvf = new SingleColumnValueFilter(
        family, qualifier, CompareOp.EQUAL, Bytes.toBytes("113"));
    // combine these two with OR in a FilterList
    FilterList filterList = new FilterList(Operator.MUST_PASS_ONE, pf, scvf);

    Scan s1 = new Scan();
    s1.setFilter(filterList);
    InternalScanner scanner = testRegion.getScanner(s1);
    List<Cell> results = new ArrayList<Cell>();
    int resultCount = 0;
    while (scanner.next(results)) {
      resultCount++;
      byte[] row =  CellUtil.cloneRow(results.get(0));
      LOG.debug("Found row: " + Bytes.toStringBinary(row));
      assertTrue(Bytes.equals(row, Bytes.toBytes("brow"))
          || Bytes.equals(row, Bytes.toBytes("crow")));
      results.clear();
    }
    assertEquals(2, resultCount);
    scanner.close();

    WAL wal = ((HRegion)testRegion).getWAL();
    ((HRegion)testRegion).close();
    wal.close();
  }

  @Test
  public void testSingleColumnValueFilter() throws IOException {

    // From HBASE-1821
    // Desired action is to combine two SCVF in a FilterList
    // Want to return only rows that match both conditions

    // Need to change one of the group one columns to use group two value
    Put p = new Put(ROWS_ONE[2]);
    p.addColumn(FAMILIES[0], QUALIFIERS_ONE[2], VALUES[1]);
    this.region.put(p);

    // Now let's grab rows that have Q_ONE[0](VALUES[0]) and Q_ONE[2](VALUES[1])
    // Since group two rows don't have these qualifiers, they will pass
    // so limiting scan to group one
    List<Filter> filters = new ArrayList<Filter>();
    filters.add(new SingleColumnValueFilter(FAMILIES[0], QUALIFIERS_ONE[0],
        CompareOp.EQUAL, VALUES[0]));
    filters.add(new SingleColumnValueFilter(FAMILIES[0], QUALIFIERS_ONE[2],
        CompareOp.EQUAL, VALUES[1]));
    Filter f = new FilterList(Operator.MUST_PASS_ALL, filters);
    Scan s = new Scan(ROWS_ONE[0], ROWS_TWO[0]);
    s.addFamily(FAMILIES[0]);
    s.setFilter(f);
    // Expect only one row, all qualifiers
    KeyValue [] kvs = {
        new KeyValue(ROWS_ONE[2], FAMILIES[0], QUALIFIERS_ONE[0], VALUES[0]),
        new KeyValue(ROWS_ONE[2], FAMILIES[0], QUALIFIERS_ONE[2], VALUES[1]),
        new KeyValue(ROWS_ONE[2], FAMILIES[0], QUALIFIERS_ONE[3], VALUES[0])
    };
    verifyScanNoEarlyOut(s, 1, 3);
    verifyScanFull(s, kvs);

    // In order to get expected behavior without limiting to group one
    // need to wrap SCVFs in SkipFilters
    filters = new ArrayList<Filter>();
    filters.add(new SkipFilter(new SingleColumnValueFilter(FAMILIES[0], QUALIFIERS_ONE[0],
        CompareOp.EQUAL, VALUES[0])));
    filters.add(new SkipFilter(new SingleColumnValueFilter(FAMILIES[0], QUALIFIERS_ONE[2],
        CompareOp.EQUAL, VALUES[1])));
    f = new FilterList(Operator.MUST_PASS_ALL, filters);
    s = new Scan(ROWS_ONE[0], ROWS_TWO[0]);
    s.addFamily(FAMILIES[0]);
    s.setFilter(f);
    // Expect same KVs
    verifyScanNoEarlyOut(s, 1, 3);
    verifyScanFull(s, kvs);

    // More tests from HBASE-1821 for Clint and filterIfMissing flag

    byte [][] ROWS_THREE = {
        Bytes.toBytes("rowThree-0"), Bytes.toBytes("rowThree-1"),
        Bytes.toBytes("rowThree-2"), Bytes.toBytes("rowThree-3")
    };

    // Give row 0 and 2 QUALIFIERS_ONE[0] (VALUE[0] VALUE[1])
    // Give row 1 and 3 QUALIFIERS_ONE[1] (VALUE[0] VALUE[1])

    KeyValue [] srcKVs = new KeyValue [] {
        new KeyValue(ROWS_THREE[0], FAMILIES[0], QUALIFIERS_ONE[0], VALUES[0]),
        new KeyValue(ROWS_THREE[1], FAMILIES[0], QUALIFIERS_ONE[0], VALUES[1]),
        new KeyValue(ROWS_THREE[2], FAMILIES[0], QUALIFIERS_ONE[1], VALUES[0]),
        new KeyValue(ROWS_THREE[3], FAMILIES[0], QUALIFIERS_ONE[1], VALUES[1])
    };

    for(KeyValue kv : srcKVs) {
      Put put = new Put(CellUtil.cloneRow(kv)).add(kv);
      put.setDurability(Durability.SKIP_WAL);
      this.region.put(put);
    }

    // Match VALUES[0] against QUALIFIERS_ONE[0] with filterIfMissing = false
    // Expect 3 rows (0, 2, 3)
    SingleColumnValueFilter scvf = new SingleColumnValueFilter(FAMILIES[0],
        QUALIFIERS_ONE[0], CompareOp.EQUAL, VALUES[0]);
    s = new Scan(ROWS_THREE[0], Bytes.toBytes("rowThree-4"));
    s.addFamily(FAMILIES[0]);
    s.setFilter(scvf);
    kvs = new KeyValue [] { srcKVs[0], srcKVs[2], srcKVs[3] };
    verifyScanFull(s, kvs);

    // Match VALUES[0] against QUALIFIERS_ONE[0] with filterIfMissing = true
    // Expect 1 row (0)
    scvf = new SingleColumnValueFilter(FAMILIES[0], QUALIFIERS_ONE[0],
        CompareOp.EQUAL, VALUES[0]);
    scvf.setFilterIfMissing(true);
    s = new Scan(ROWS_THREE[0], Bytes.toBytes("rowThree-4"));
    s.addFamily(FAMILIES[0]);
    s.setFilter(scvf);
    kvs = new KeyValue [] { srcKVs[0] };
    verifyScanFull(s, kvs);

    // Match VALUES[1] against QUALIFIERS_ONE[1] with filterIfMissing = true
    // Expect 1 row (3)
    scvf = new SingleColumnValueFilter(FAMILIES[0],
        QUALIFIERS_ONE[1], CompareOp.EQUAL, VALUES[1]);
    scvf.setFilterIfMissing(true);
    s = new Scan(ROWS_THREE[0], Bytes.toBytes("rowThree-4"));
    s.addFamily(FAMILIES[0]);
    s.setFilter(scvf);
    kvs = new KeyValue [] { srcKVs[3] };
    verifyScanFull(s, kvs);

    // Add QUALIFIERS_ONE[1] to ROWS_THREE[0] with VALUES[0]
    KeyValue kvA = new KeyValue(ROWS_THREE[0], FAMILIES[0], QUALIFIERS_ONE[1], VALUES[0]);
    this.region.put(new Put(CellUtil.cloneRow(kvA)).add(kvA));

    // Match VALUES[1] against QUALIFIERS_ONE[1] with filterIfMissing = true
    // Expect 1 row (3)
    scvf = new SingleColumnValueFilter(FAMILIES[0],
        QUALIFIERS_ONE[1], CompareOp.EQUAL, VALUES[1]);
    scvf.setFilterIfMissing(true);
    s = new Scan(ROWS_THREE[0], Bytes.toBytes("rowThree-4"));
    s.addFamily(FAMILIES[0]);
    s.setFilter(scvf);
    kvs = new KeyValue [] { srcKVs[3] };
    verifyScanFull(s, kvs);

  }

  private void verifyScan(Scan s, long expectedRows, long expectedKeys)
  throws IOException {
    InternalScanner scanner = this.region.getScanner(s);
    List<Cell> results = new ArrayList<Cell>();
    int i = 0;
    for (boolean done = true; done; i++) {
      done = scanner.next(results);
      Arrays.sort(results.toArray(new KeyValue[results.size()]),
          CellComparator.COMPARATOR);
      LOG.info("counter=" + i + ", " + results);
      if (results.isEmpty()) break;
      assertTrue("Scanned too many rows! Only expected " + expectedRows +
          " total but already scanned " + (i+1), expectedRows > i);
      assertEquals("Expected " + expectedKeys + " keys per row but " +
          "returned " + results.size(), expectedKeys, results.size());
      results.clear();
    }
    assertEquals("Expected " + expectedRows + " rows but scanned " + i +
        " rows", expectedRows, i);
  }

  private void verifyScanNoEarlyOut(Scan s, long expectedRows,
      long expectedKeys)
  throws IOException {
    InternalScanner scanner = this.region.getScanner(s);
    List<Cell> results = new ArrayList<Cell>();
    int i = 0;
    for (boolean done = true; done; i++) {
      done = scanner.next(results);
      Arrays.sort(results.toArray(new KeyValue[results.size()]),
          CellComparator.COMPARATOR);
      LOG.info("counter=" + i + ", " + results);
      if(results.isEmpty()) break;
      assertTrue("Scanned too many rows! Only expected " + expectedRows +
          " total but already scanned " + (i+1), expectedRows > i);
      assertEquals("Expected " + expectedKeys + " keys per row but " +
          "returned " + results.size(), expectedKeys, results.size());
      results.clear();
    }
    assertEquals("Expected " + expectedRows + " rows but scanned " + i +
        " rows", expectedRows, i);
  }

  private void verifyScanFull(Scan s, KeyValue [] kvs)
  throws IOException {
    InternalScanner scanner = this.region.getScanner(s);
    List<Cell> results = new ArrayList<Cell>();
    int row = 0;
    int idx = 0;
    for (boolean done = true; done; row++) {
      done = scanner.next(results);
      Arrays.sort(results.toArray(new KeyValue[results.size()]),
          CellComparator.COMPARATOR);
      if(results.isEmpty()) break;
      assertTrue("Scanned too many keys! Only expected " + kvs.length +
          " total but already scanned " + (results.size() + idx) +
          (results.isEmpty() ? "" : "(" + results.get(0).toString() + ")"),
          kvs.length >= idx + results.size());
      for (Cell kv : results) {
        LOG.info("row=" + row + ", result=" + kv.toString() +
            ", match=" + kvs[idx].toString());
        assertTrue("Row mismatch", CellUtil.matchingRow(kv, kvs[idx]));
        assertTrue("Family mismatch", CellUtil.matchingFamily(kv, kvs[idx]));
        assertTrue("Qualifier mismatch", CellUtil.matchingQualifier(kv, kvs[idx]));
        assertTrue("Value mismatch", CellUtil.matchingValue(kv, kvs[idx]));
        idx++;
      }
      results.clear();
    }
    LOG.info("Looked at " + row + " rows with " + idx + " keys");
    assertEquals("Expected " + kvs.length + " total keys but scanned " + idx,
        kvs.length, idx);
  }

  private void verifyScanFullNoValues(Scan s, KeyValue [] kvs, boolean useLen)
  throws IOException {
    InternalScanner scanner = this.region.getScanner(s);
    List<Cell> results = new ArrayList<Cell>();
    int row = 0;
    int idx = 0;
    for (boolean more = true; more; row++) {
      more = scanner.next(results);
      Arrays.sort(results.toArray(new KeyValue[results.size()]),
          CellComparator.COMPARATOR);
      if(results.isEmpty()) break;
      assertTrue("Scanned too many keys! Only expected " + kvs.length +
          " total but already scanned " + (results.size() + idx) +
          (results.isEmpty() ? "" : "(" + results.get(0).toString() + ")"),
          kvs.length >= idx + results.size());
      for(Cell kv : results) {
        LOG.info("row=" + row + ", result=" + kv.toString() +
            ", match=" + kvs[idx].toString());

        assertTrue("Row mismatch", CellUtil.matchingRow(kv, kvs[idx]));
        assertTrue("Family mismatch", CellUtil.matchingFamily(kv, kvs[idx]));
        assertTrue("Qualifier mismatch", CellUtil.matchingQualifier(kv, kvs[idx]));
        assertFalse("Should not have returned whole value", CellUtil.matchingValue(kv, kvs[idx]));
        if (useLen) {
          assertEquals("Value in result is not SIZEOF_INT",
                     kv.getValueLength(), Bytes.SIZEOF_INT);
          LOG.info("idx = "  + idx + ", len=" + kvs[idx].getValueLength()
              + ", actual=" +  Bytes.toInt(CellUtil.cloneValue(kv)));
          assertEquals("Scan value should be the length of the actual value. ",
                     kvs[idx].getValueLength(), Bytes.toInt(CellUtil.cloneValue(kv)) );
          LOG.info("good");
        } else {
          assertEquals("Value in result is not empty", kv.getValueLength(), 0);
        }
        idx++;
      }
      results.clear();
    }
    LOG.info("Looked at " + row + " rows with " + idx + " keys");
    assertEquals("Expected " + kvs.length + " total keys but scanned " + idx,
        kvs.length, idx);
  }

  public void testColumnPaginationFilterColumnOffset() throws Exception {
    KeyValue [] expectedKVs = {
      // testRowOne-0
      new KeyValue(ROWS_ONE[0], FAMILIES[0], QUALIFIERS_ONE[2], VALUES[0]),
      // testRowOne-2
      new KeyValue(ROWS_ONE[2], FAMILIES[0], QUALIFIERS_ONE[2], VALUES[0]),
      // testRowOne-3
      new KeyValue(ROWS_ONE[3], FAMILIES[0], QUALIFIERS_ONE[2], VALUES[0]),
      // testRowTwo-0
      new KeyValue(ROWS_TWO[0], FAMILIES[0], QUALIFIERS_TWO[0], VALUES[1]),
      // testRowTwo-2
      new KeyValue(ROWS_TWO[2], FAMILIES[0], QUALIFIERS_TWO[0], VALUES[1]),
      // testRowTwo-3
      new KeyValue(ROWS_TWO[3], FAMILIES[0], QUALIFIERS_TWO[0], VALUES[1]),
    };
    KeyValue [] expectedKVs1 = {
      // testRowTwo-0
      new KeyValue(ROWS_TWO[0], FAMILIES[0], QUALIFIERS_TWO[2], VALUES[1]),
      new KeyValue(ROWS_TWO[0], FAMILIES[0], QUALIFIERS_TWO[3], VALUES[1]),
      // testRowTwo-2
      new KeyValue(ROWS_TWO[2], FAMILIES[0], QUALIFIERS_TWO[2], VALUES[1]),
      new KeyValue(ROWS_TWO[2], FAMILIES[0], QUALIFIERS_TWO[3], VALUES[1]),
      // testRowTwo-3
      new KeyValue(ROWS_TWO[3], FAMILIES[0], QUALIFIERS_TWO[2], VALUES[1]),
      new KeyValue(ROWS_TWO[3], FAMILIES[0], QUALIFIERS_TWO[3], VALUES[1])
    };
    KeyValue [] expectedKVs2 = {
      // testRowTwo-0
      new KeyValue(ROWS_TWO[0], FAMILIES[0], QUALIFIERS_TWO[2], VALUES[1]),
      new KeyValue(ROWS_TWO[0], FAMILIES[0], QUALIFIERS_TWO[3], VALUES[1]),
      new KeyValue(ROWS_TWO[0], FAMILIES[1], QUALIFIERS_TWO[0], VALUES[1]),
      // testRowTwo-2
      new KeyValue(ROWS_TWO[2], FAMILIES[0], QUALIFIERS_TWO[2], VALUES[1]),
      new KeyValue(ROWS_TWO[2], FAMILIES[0], QUALIFIERS_TWO[3], VALUES[1]),
      new KeyValue(ROWS_TWO[2], FAMILIES[1], QUALIFIERS_TWO[0], VALUES[1]),
      // testRowTwo-3
      new KeyValue(ROWS_TWO[3], FAMILIES[0], QUALIFIERS_TWO[2], VALUES[1]),
      new KeyValue(ROWS_TWO[3], FAMILIES[0], QUALIFIERS_TWO[3], VALUES[1]),
      new KeyValue(ROWS_TWO[3], FAMILIES[1], QUALIFIERS_TWO[0], VALUES[1])
    };
    KeyValue [] expectedKVs3 = {
      // testRowTwo-0
      new KeyValue(ROWS_TWO[0], FAMILIES[1], QUALIFIERS_TWO[2], VALUES[1]),
      new KeyValue(ROWS_TWO[0], FAMILIES[1], QUALIFIERS_TWO[3], VALUES[1]),
      // testRowTwo-2
      new KeyValue(ROWS_TWO[2], FAMILIES[1], QUALIFIERS_TWO[2], VALUES[1]),
      new KeyValue(ROWS_TWO[2], FAMILIES[1], QUALIFIERS_TWO[3], VALUES[1]),
      // testRowTwo-3
      new KeyValue(ROWS_TWO[3], FAMILIES[1], QUALIFIERS_TWO[2], VALUES[1]),
      new KeyValue(ROWS_TWO[3], FAMILIES[1], QUALIFIERS_TWO[3], VALUES[1]),
    };
    Scan s = new Scan();

    // Page size 1.
    long expectedRows = 6;
    long expectedKeys = 1;
    s.setFilter(new ColumnPaginationFilter(1, QUALIFIERS_ONE[1]));
    verifyScan(s, expectedRows, expectedKeys);
    this.verifyScanFull(s, expectedKVs);

    // Page size 2.
    expectedRows = 3;
    expectedKeys = 2;
    s = new Scan();
    s.setFilter(new ColumnPaginationFilter(2, QUALIFIERS_TWO[2]));
    verifyScan(s, expectedRows, expectedKeys);
    this.verifyScanFull(s, expectedKVs1);

    // Page size 3 across multiple column families.
    expectedRows = 3;
    expectedKeys = 3;
    s = new Scan();
    s.setFilter(new ColumnPaginationFilter(3, QUALIFIERS_TWO[2]));
    verifyScan(s, expectedRows, expectedKeys);
    this.verifyScanFull(s, expectedKVs2);

    // Page size 2 restricted to one column family.
    expectedRows = 3;
    expectedKeys = 2;
    s = new Scan();
    s.addFamily(FAMILIES[1]);
    s.setFilter(new ColumnPaginationFilter(2, QUALIFIERS_TWO[2]));
    this.verifyScanFull(s, expectedKVs3);
  }

  @Test
  public void testColumnPaginationFilter() throws Exception {
    // Test that the filter skips multiple column versions.
    Put p = new Put(ROWS_ONE[0]);
    p.setDurability(Durability.SKIP_WAL);
    p.addColumn(FAMILIES[0], QUALIFIERS_ONE[0], VALUES[0]);
    this.region.put(p);
      this.region.flush(true);

      // Set of KVs (page: 1; pageSize: 1) - the first set of 1 column per row
      KeyValue [] expectedKVs = {
        // testRowOne-0
        new KeyValue(ROWS_ONE[0], FAMILIES[0], QUALIFIERS_ONE[0], VALUES[0]),
        // testRowOne-2
        new KeyValue(ROWS_ONE[2], FAMILIES[0], QUALIFIERS_ONE[0], VALUES[0]),
        // testRowOne-3
        new KeyValue(ROWS_ONE[3], FAMILIES[0], QUALIFIERS_ONE[0], VALUES[0]),
        // testRowTwo-0
        new KeyValue(ROWS_TWO[0], FAMILIES[0], QUALIFIERS_TWO[0], VALUES[1]),
        // testRowTwo-2
        new KeyValue(ROWS_TWO[2], FAMILIES[0], QUALIFIERS_TWO[0], VALUES[1]),
        // testRowTwo-3
        new KeyValue(ROWS_TWO[3], FAMILIES[0], QUALIFIERS_TWO[0], VALUES[1])
      };

      // Set of KVs (page: 3; pageSize: 1)  - the third set of 1 column per row
      KeyValue [] expectedKVs2 = {
        // testRowOne-0
        new KeyValue(ROWS_ONE[0], FAMILIES[0], QUALIFIERS_ONE[3], VALUES[0]),
        // testRowOne-2
        new KeyValue(ROWS_ONE[2], FAMILIES[0], QUALIFIERS_ONE[3], VALUES[0]),
        // testRowOne-3
        new KeyValue(ROWS_ONE[3], FAMILIES[0], QUALIFIERS_ONE[3], VALUES[0]),
        // testRowTwo-0
        new KeyValue(ROWS_TWO[0], FAMILIES[0], QUALIFIERS_TWO[3], VALUES[1]),
        // testRowTwo-2
        new KeyValue(ROWS_TWO[2], FAMILIES[0], QUALIFIERS_TWO[3], VALUES[1]),
        // testRowTwo-3
        new KeyValue(ROWS_TWO[3], FAMILIES[0], QUALIFIERS_TWO[3], VALUES[1]),
      };

      // Set of KVs (page: 2; pageSize 2)  - the 2nd set of 2 columns per row
      KeyValue [] expectedKVs3 = {
        // testRowOne-0
        new KeyValue(ROWS_ONE[0], FAMILIES[0], QUALIFIERS_ONE[3], VALUES[0]),
        new KeyValue(ROWS_ONE[0], FAMILIES[1], QUALIFIERS_ONE[0], VALUES[0]),
        // testRowOne-2
        new KeyValue(ROWS_ONE[2], FAMILIES[0], QUALIFIERS_ONE[3], VALUES[0]),
        new KeyValue(ROWS_ONE[2], FAMILIES[1], QUALIFIERS_ONE[0], VALUES[0]),
        // testRowOne-3
        new KeyValue(ROWS_ONE[3], FAMILIES[0], QUALIFIERS_ONE[3], VALUES[0]),
        new KeyValue(ROWS_ONE[3], FAMILIES[1], QUALIFIERS_ONE[0], VALUES[0]),
        // testRowTwo-0
        new KeyValue(ROWS_TWO[0], FAMILIES[0], QUALIFIERS_TWO[3], VALUES[1]),
        new KeyValue(ROWS_TWO[0], FAMILIES[1], QUALIFIERS_TWO[0], VALUES[1]),
        // testRowTwo-2
        new KeyValue(ROWS_TWO[2], FAMILIES[0], QUALIFIERS_TWO[3], VALUES[1]),
        new KeyValue(ROWS_TWO[2], FAMILIES[1], QUALIFIERS_TWO[0], VALUES[1]),
        // testRowTwo-3
        new KeyValue(ROWS_TWO[3], FAMILIES[0], QUALIFIERS_TWO[3], VALUES[1]),
        new KeyValue(ROWS_TWO[3], FAMILIES[1], QUALIFIERS_TWO[0], VALUES[1]),
      };


      // Set of KVs (page: 2; pageSize 2)  - the 2nd set of 2 columns per row
      KeyValue [] expectedKVs4 = {

      };

      long expectedRows = this.numRows;
      long expectedKeys = 1;
      Scan s = new Scan();


      // Page 1; 1 Column per page  (Limit 1, Offset 0)
      s.setFilter(new ColumnPaginationFilter(1,0));
      verifyScan(s, expectedRows, expectedKeys);
      this.verifyScanFull(s, expectedKVs);

      // Page 3; 1 Result per page  (Limit 1, Offset 2)
      s.setFilter(new ColumnPaginationFilter(1,2));
      verifyScan(s, expectedRows, expectedKeys);
      this.verifyScanFull(s, expectedKVs2);

      // Page 2; 2 Results per page (Limit 2, Offset 2)
      s.setFilter(new ColumnPaginationFilter(2,2));
      expectedKeys = 2;
      verifyScan(s, expectedRows, expectedKeys);
      this.verifyScanFull(s, expectedKVs3);

      // Page 8; 20 Results per page (no results) (Limit 20, Offset 140)
      s.setFilter(new ColumnPaginationFilter(20,140));
      expectedKeys = 0;
      expectedRows = 0;
      verifyScan(s, expectedRows, 0);
      this.verifyScanFull(s, expectedKVs4);
  }

  @Test
  public void testKeyOnlyFilter() throws Exception {

    // KVs in first 6 rows
    KeyValue [] expectedKVs = {
      // testRowOne-0
      new KeyValue(ROWS_ONE[0], FAMILIES[0], QUALIFIERS_ONE[0], VALUES[0]),
      new KeyValue(ROWS_ONE[0], FAMILIES[0], QUALIFIERS_ONE[2], VALUES[0]),
      new KeyValue(ROWS_ONE[0], FAMILIES[0], QUALIFIERS_ONE[3], VALUES[0]),
      new KeyValue(ROWS_ONE[0], FAMILIES[1], QUALIFIERS_ONE[0], VALUES[0]),
      new KeyValue(ROWS_ONE[0], FAMILIES[1], QUALIFIERS_ONE[2], VALUES[0]),
      new KeyValue(ROWS_ONE[0], FAMILIES[1], QUALIFIERS_ONE[3], VALUES[0]),
      // testRowOne-2
      new KeyValue(ROWS_ONE[2], FAMILIES[0], QUALIFIERS_ONE[0], VALUES[0]),
      new KeyValue(ROWS_ONE[2], FAMILIES[0], QUALIFIERS_ONE[2], VALUES[0]),
      new KeyValue(ROWS_ONE[2], FAMILIES[0], QUALIFIERS_ONE[3], VALUES[0]),
      new KeyValue(ROWS_ONE[2], FAMILIES[1], QUALIFIERS_ONE[0], VALUES[0]),
      new KeyValue(ROWS_ONE[2], FAMILIES[1], QUALIFIERS_ONE[2], VALUES[0]),
      new KeyValue(ROWS_ONE[2], FAMILIES[1], QUALIFIERS_ONE[3], VALUES[0]),
      // testRowOne-3
      new KeyValue(ROWS_ONE[3], FAMILIES[0], QUALIFIERS_ONE[0], VALUES[0]),
      new KeyValue(ROWS_ONE[3], FAMILIES[0], QUALIFIERS_ONE[2], VALUES[0]),
      new KeyValue(ROWS_ONE[3], FAMILIES[0], QUALIFIERS_ONE[3], VALUES[0]),
      new KeyValue(ROWS_ONE[3], FAMILIES[1], QUALIFIERS_ONE[0], VALUES[0]),
      new KeyValue(ROWS_ONE[3], FAMILIES[1], QUALIFIERS_ONE[2], VALUES[0]),
      new KeyValue(ROWS_ONE[3], FAMILIES[1], QUALIFIERS_ONE[3], VALUES[0]),
      // testRowTwo-0
      new KeyValue(ROWS_TWO[0], FAMILIES[0], QUALIFIERS_TWO[0], VALUES[1]),
      new KeyValue(ROWS_TWO[0], FAMILIES[0], QUALIFIERS_TWO[2], VALUES[1]),
      new KeyValue(ROWS_TWO[0], FAMILIES[0], QUALIFIERS_TWO[3], VALUES[1]),
      new KeyValue(ROWS_TWO[0], FAMILIES[1], QUALIFIERS_TWO[0], VALUES[1]),
      new KeyValue(ROWS_TWO[0], FAMILIES[1], QUALIFIERS_TWO[2], VALUES[1]),
      new KeyValue(ROWS_TWO[0], FAMILIES[1], QUALIFIERS_TWO[3], VALUES[1]),
      // testRowTwo-2
      new KeyValue(ROWS_TWO[2], FAMILIES[0], QUALIFIERS_TWO[0], VALUES[1]),
      new KeyValue(ROWS_TWO[2], FAMILIES[0], QUALIFIERS_TWO[2], VALUES[1]),
      new KeyValue(ROWS_TWO[2], FAMILIES[0], QUALIFIERS_TWO[3], VALUES[1]),
      new KeyValue(ROWS_TWO[2], FAMILIES[1], QUALIFIERS_TWO[0], VALUES[1]),
      new KeyValue(ROWS_TWO[2], FAMILIES[1], QUALIFIERS_TWO[2], VALUES[1]),
      new KeyValue(ROWS_TWO[2], FAMILIES[1], QUALIFIERS_TWO[3], VALUES[1]),
      // testRowTwo-3
      new KeyValue(ROWS_TWO[3], FAMILIES[0], QUALIFIERS_TWO[0], VALUES[1]),
      new KeyValue(ROWS_TWO[3], FAMILIES[0], QUALIFIERS_TWO[2], VALUES[1]),
      new KeyValue(ROWS_TWO[3], FAMILIES[0], QUALIFIERS_TWO[3], VALUES[1]),
      new KeyValue(ROWS_TWO[3], FAMILIES[1], QUALIFIERS_TWO[0], VALUES[1]),
      new KeyValue(ROWS_TWO[3], FAMILIES[1], QUALIFIERS_TWO[2], VALUES[1]),
      new KeyValue(ROWS_TWO[3], FAMILIES[1], QUALIFIERS_TWO[3], VALUES[1])
    };

    // Grab all 6 rows
    long expectedRows = 6;
    long expectedKeys = this.colsPerRow;
    for (boolean useLen : new boolean[]{false,true}) {
      Scan s = new Scan();
      s.setFilter(new KeyOnlyFilter(useLen));
      verifyScan(s, expectedRows, expectedKeys);
      verifyScanFullNoValues(s, expectedKVs, useLen);
    }
  }

  /**
   * Filter which makes sleeps for a second between each row of a scan.
   * This can be useful for manual testing of bugs like HBASE-5973. For example:
   * <code>
   * create 't1', 'f1'
   * 1.upto(100)  { |x| put 't1', 'r' + x.to_s, 'f1:q1', 'hi' }
   * import org.apache.hadoop.hbase.filter.TestFilter
   * scan 't1', { FILTER => TestFilter::SlowScanFilter.new(), CACHE => 50 }
   * </code>
   */
  public static class SlowScanFilter extends FilterBase {
    private static Thread ipcHandlerThread = null;

    @Override
    public byte [] toByteArray() {return null;}

    @Override
    public ReturnCode filterKeyValue(Cell ignored) throws IOException {
      return ReturnCode.INCLUDE;
    }

    @Override
    public boolean filterRow() throws IOException {
      ipcHandlerThread = Thread.currentThread();
      try {
        LOG.info("Handler thread " + ipcHandlerThread + " sleeping in filter...");
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        Throwables.propagate(e);
      }
      return super.filterRow();
    }
  }

  // TODO: intentionally disabled?
  public void testNestedFilterListWithSCVF() throws IOException {
    byte[] columnStatus = Bytes.toBytes("S");
    HTableDescriptor htd = new HTableDescriptor(TableName.valueOf("testNestedFilterListWithSCVF"));
    htd.addFamily(new HColumnDescriptor(FAMILIES[0]));
    HRegionInfo info = new HRegionInfo(htd.getTableName(), null, null, false);
    Region testRegion = HBaseTestingUtility.createRegionAndWAL(info, TEST_UTIL.getDataTestDir(),
        TEST_UTIL.getConfiguration(), htd);
    for(int i=0; i<10; i++) {
      Put p = new Put(Bytes.toBytes("row" + i));
      p.setDurability(Durability.SKIP_WAL);
      p.addColumn(FAMILIES[0], columnStatus, Bytes.toBytes(i % 2));
      testRegion.put(p);
    }
    testRegion.flush(true);
    // 1. got rows > "row4"
    Filter rowFilter = new RowFilter(CompareOp.GREATER,new BinaryComparator(Bytes.toBytes("row4")));
    Scan s1 = new Scan();
    s1.setFilter(rowFilter);
    InternalScanner scanner = testRegion.getScanner(s1);
    List<Cell> results = new ArrayList<Cell>();
    int i = 5;
    for (boolean done = true; done; i++) {
      done = scanner.next(results);
      assertTrue(CellUtil.matchingRow(results.get(0), Bytes.toBytes("row" + i)));
      assertEquals(Bytes.toInt(CellUtil.cloneValue(results.get(0))), i%2);
      results.clear();
    }
    // 2. got rows <= "row4" and S=
    FilterList subFilterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
    Filter subFilter1 = new RowFilter(CompareOp.LESS_OR_EQUAL,
      new BinaryComparator(Bytes.toBytes("row4")));
    subFilterList.addFilter(subFilter1);
    Filter subFilter2 = new SingleColumnValueFilter(FAMILIES[0], columnStatus, CompareOp.EQUAL,
      Bytes.toBytes(0));
    subFilterList.addFilter(subFilter2);
    s1 = new Scan();
    s1.setFilter(subFilterList);
    scanner = testRegion.getScanner(s1);
    results = new ArrayList<Cell>();
    for (i=0; i<=4; i+=2) {
      scanner.next(results);
      assertTrue(CellUtil.matchingRow(results.get(0), Bytes.toBytes("row" + i)));
      assertEquals(Bytes.toInt(CellUtil.cloneValue(results.get(0))), i%2);
      results.clear();
    }
    assertFalse(scanner.next(results));
    // 3. let's begin to verify nested filter list
    // 3.1 add rowFilter, then add subFilterList
    FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE);
    filterList.addFilter(rowFilter);
    filterList.addFilter(subFilterList);
    s1 = new Scan();
    s1.setFilter(filterList);
    scanner = testRegion.getScanner(s1);
    results = new ArrayList<Cell>();
    for (i=0; i<=4; i+=2) {
      scanner.next(results);
      assertTrue(CellUtil.matchingRow(results.get(0), Bytes.toBytes("row" + i)));
      assertEquals(Bytes.toInt(CellUtil.cloneValue(results.get(0))), i%2);
      results.clear();
    }
    for (i=5; i<=9; i++) {
      scanner.next(results);
      assertTrue(CellUtil.matchingRow(results.get(0), Bytes.toBytes("row" + i)));
      assertEquals(Bytes.toInt(CellUtil.cloneValue(results.get(0))), i%2);
      results.clear();
    }
    assertFalse(scanner.next(results));
    // 3.2 MAGIC here! add subFilterList first, then add rowFilter
    filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE);
    filterList.addFilter(subFilterList);
    filterList.addFilter(rowFilter);
    s1 = new Scan();
    s1.setFilter(filterList);
    scanner = testRegion.getScanner(s1);
    results = new ArrayList<Cell>();
    for (i=0; i<=4; i+=2) {
      scanner.next(results);
      assertTrue(CellUtil.matchingRow(results.get(0), Bytes.toBytes("row" + i)));
      assertEquals(Bytes.toInt(CellUtil.cloneValue(results.get(0))), i%2);
      results.clear();
    }
    for (i=5; i<=9; i++) {
      scanner.next(results);
      assertTrue(CellUtil.matchingRow(results.get(0), Bytes.toBytes("row" + i)));
      assertEquals(Bytes.toInt(CellUtil.cloneValue(results.get(0))), i%2);
      results.clear();
    }
    assertFalse(scanner.next(results));
    WAL wal = ((HRegion)testRegion).getWAL();
    ((HRegion)testRegion).close();
    wal.close();
  }
}
