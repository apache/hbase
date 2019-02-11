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
package org.apache.hadoop.hbase.util;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.util.RegionSplitter.DecimalStringSplit;
import org.apache.hadoop.hbase.util.RegionSplitter.HexStringSplit;
import org.apache.hadoop.hbase.util.RegionSplitter.SplitAlgorithm;
import org.apache.hadoop.hbase.util.RegionSplitter.UniformSplit;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests for {@link RegionSplitter}, which can create a pre-split table or do a
 * rolling split of an existing table.
 */
@Category({MiscTests.class, MediumTests.class})
public class TestRegionSplitter {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestRegionSplitter.class);

    private final static Logger LOG = LoggerFactory.getLogger(TestRegionSplitter.class);
    private final static HBaseTestingUtility UTIL = new HBaseTestingUtility();
    private final static String CF_NAME = "SPLIT_TEST_CF";
    private final static byte xFF = (byte) 0xff;

    @Rule
    public TestName name = new TestName();

    @BeforeClass
    public static void setup() throws Exception {
        UTIL.startMiniCluster();
    }

    @AfterClass
    public static void teardown() throws Exception {
        UTIL.shutdownMiniCluster();
    }

    /**
     * Test creating a pre-split table using the HexStringSplit algorithm.
     */
  @Test
  public void testCreatePresplitTableHex() throws Exception {
    final List<byte[]> expectedBounds = new ArrayList<>(17);
    expectedBounds.add(ArrayUtils.EMPTY_BYTE_ARRAY);
    expectedBounds.add(Bytes.toBytes("10000000"));
    expectedBounds.add(Bytes.toBytes("20000000"));
    expectedBounds.add(Bytes.toBytes("30000000"));
    expectedBounds.add(Bytes.toBytes("40000000"));
    expectedBounds.add(Bytes.toBytes("50000000"));
    expectedBounds.add(Bytes.toBytes("60000000"));
    expectedBounds.add(Bytes.toBytes("70000000"));
    expectedBounds.add(Bytes.toBytes("80000000"));
    expectedBounds.add(Bytes.toBytes("90000000"));
    expectedBounds.add(Bytes.toBytes("a0000000"));
    expectedBounds.add(Bytes.toBytes("b0000000"));
    expectedBounds.add(Bytes.toBytes("c0000000"));
    expectedBounds.add(Bytes.toBytes("d0000000"));
    expectedBounds.add(Bytes.toBytes("e0000000"));
    expectedBounds.add(Bytes.toBytes("f0000000"));
    expectedBounds.add(ArrayUtils.EMPTY_BYTE_ARRAY);

          // Do table creation/pre-splitting and verification of region boundaries
    preSplitTableAndVerify(expectedBounds,
        HexStringSplit.class.getSimpleName(),
        TableName.valueOf(name.getMethodName()));
  }

  /**
   * Test creating a pre-split table using the UniformSplit algorithm.
   */
  @Test
  public void testCreatePresplitTableUniform() throws Exception {
    List<byte[]> expectedBounds = new ArrayList<>(17);
    expectedBounds.add(ArrayUtils.EMPTY_BYTE_ARRAY);
    expectedBounds.add(new byte[] {      0x10, 0, 0, 0, 0, 0, 0, 0});
    expectedBounds.add(new byte[] {      0x20, 0, 0, 0, 0, 0, 0, 0});
    expectedBounds.add(new byte[] {      0x30, 0, 0, 0, 0, 0, 0, 0});
    expectedBounds.add(new byte[] {      0x40, 0, 0, 0, 0, 0, 0, 0});
    expectedBounds.add(new byte[] { 0x50, 0, 0, 0, 0, 0, 0, 0 });
    expectedBounds.add(new byte[] { 0x60, 0, 0, 0, 0, 0, 0, 0 });
    expectedBounds.add(new byte[] { 0x70, 0, 0, 0, 0, 0, 0, 0 });
    expectedBounds.add(new byte[] { (byte) 0x80, 0, 0, 0, 0, 0, 0, 0 });
    expectedBounds.add(new byte[] { (byte) 0x90, 0, 0, 0, 0, 0, 0, 0 });
    expectedBounds.add(new byte[] {(byte)0xa0, 0, 0, 0, 0, 0, 0, 0});
    expectedBounds.add(new byte[] { (byte) 0xb0, 0, 0, 0, 0, 0, 0, 0 });
    expectedBounds.add(new byte[] { (byte) 0xc0, 0, 0, 0, 0, 0, 0, 0 });
    expectedBounds.add(new byte[] { (byte) 0xd0, 0, 0, 0, 0, 0, 0, 0 });
    expectedBounds.add(new byte[] {(byte)0xe0, 0, 0, 0, 0, 0, 0, 0});
    expectedBounds.add(new byte[] { (byte) 0xf0, 0, 0, 0, 0, 0, 0, 0 });
    expectedBounds.add(ArrayUtils.EMPTY_BYTE_ARRAY);

    // Do table creation/pre-splitting and verification of region boundaries
    preSplitTableAndVerify(expectedBounds, UniformSplit.class.getSimpleName(),
        TableName.valueOf(name.getMethodName()));
  }

  /**
   * Unit tests for the HexStringSplit algorithm. Makes sure it divides up the
   * space of keys in the way that we expect.
   */
  @Test
  public void unitTestHexStringSplit() {
    HexStringSplit splitter = new HexStringSplit();
    // Check splitting while starting from scratch

    byte[][] twoRegionsSplits = splitter.split(2);
    assertEquals(1, twoRegionsSplits.length);
    assertArrayEquals(Bytes.toBytes("80000000"), twoRegionsSplits[0]);

    byte[][] threeRegionsSplits = splitter.split(3);
    assertEquals(2, threeRegionsSplits.length);
    byte[] expectedSplit0 = Bytes.toBytes("55555555");
    assertArrayEquals(expectedSplit0, threeRegionsSplits[0]);
    byte[] expectedSplit1 = Bytes.toBytes("aaaaaaaa");
    assertArrayEquals(expectedSplit1, threeRegionsSplits[1]);

    // Check splitting existing regions that have start and end points
    byte[] splitPoint = splitter.split(Bytes.toBytes("10000000"), Bytes.toBytes("30000000"));
    assertArrayEquals(Bytes.toBytes("20000000"), splitPoint);

    byte[] lastRow = Bytes.toBytes("ffffffff");
    assertArrayEquals(lastRow, splitter.lastRow());
    byte[] firstRow = Bytes.toBytes("00000000");
    assertArrayEquals(firstRow, splitter.firstRow());

    // Halfway between 00... and 20... should be 10...
    splitPoint = splitter.split(firstRow, Bytes.toBytes("20000000"));
    assertArrayEquals(Bytes.toBytes("10000000"), splitPoint);

    // Halfway between df... and ff... should be ef....
    splitPoint = splitter.split(Bytes.toBytes("dfffffff"), lastRow);
    assertArrayEquals(Bytes.toBytes("efffffff"), splitPoint);

    // Check splitting region with multiple mappers per region
    byte[][] splits = splitter.split(Bytes.toBytes("00000000"), Bytes.toBytes("30000000"),
        3, false);
    assertEquals(2, splits.length);
    assertArrayEquals(Bytes.toBytes("10000000"), splits[0]);
    assertArrayEquals(Bytes.toBytes("20000000"), splits[1]);

    splits = splitter.split(Bytes.toBytes("00000000"), Bytes.toBytes("20000000"), 2, true);
    assertEquals(3, splits.length);
    assertArrayEquals(Bytes.toBytes("10000000"), splits[1]);
  }

  /**
   * Unit tests for the DecimalStringSplit algorithm. Makes sure it divides up the
   * space of keys in the way that we expect.
   */
  @Test
  public void unitTestDecimalStringSplit() {
    DecimalStringSplit splitter = new DecimalStringSplit();
    // Check splitting while starting from scratch

    byte[][] twoRegionsSplits = splitter.split(2);
    assertEquals(1, twoRegionsSplits.length);
    assertArrayEquals(Bytes.toBytes("50000000"), twoRegionsSplits[0]);

    byte[][] threeRegionsSplits = splitter.split(3);
    assertEquals(2, threeRegionsSplits.length);
    byte[] expectedSplit0 = Bytes.toBytes("33333333");
    assertArrayEquals(expectedSplit0, threeRegionsSplits[0]);
    byte[] expectedSplit1 = Bytes.toBytes("66666666");
    assertArrayEquals(expectedSplit1, threeRegionsSplits[1]);

    // Check splitting existing regions that have start and end points
    byte[] splitPoint = splitter.split(Bytes.toBytes("10000000"), Bytes.toBytes("30000000"));
    assertArrayEquals(Bytes.toBytes("20000000"), splitPoint);

    byte[] lastRow = Bytes.toBytes("99999999");
    assertArrayEquals(lastRow, splitter.lastRow());
    byte[] firstRow = Bytes.toBytes("00000000");
    assertArrayEquals(firstRow, splitter.firstRow());

    // Halfway between 00... and 20... should be 10...
    splitPoint = splitter.split(firstRow, Bytes.toBytes("20000000"));
    assertArrayEquals(Bytes.toBytes("10000000"), splitPoint);

    // Halfway between 00... and 19... should be 09...
    splitPoint = splitter.split(firstRow, Bytes.toBytes("19999999"));
    assertArrayEquals(Bytes.toBytes("09999999"), splitPoint);

    // Halfway between 79... and 99... should be 89....
    splitPoint = splitter.split(Bytes.toBytes("79999999"), lastRow);
    assertArrayEquals(Bytes.toBytes("89999999"), splitPoint);

    // Check splitting region with multiple mappers per region
    byte[][] splits = splitter.split(Bytes.toBytes("00000000"), Bytes.toBytes("30000000"),
        3, false);
    assertEquals(2, splits.length);
    assertArrayEquals(Bytes.toBytes("10000000"), splits[0]);
    assertArrayEquals(Bytes.toBytes("20000000"), splits[1]);

    splits = splitter.split(Bytes.toBytes("00000000"), Bytes.toBytes("20000000"), 2, true);
    assertEquals(3, splits.length);
    assertArrayEquals(Bytes.toBytes("10000000"), splits[1]);
  }

  /**
   * Unit tests for the UniformSplit algorithm. Makes sure it divides up the space of
   * keys in the way that we expect.
   */
  @Test
  public void unitTestUniformSplit() {
    UniformSplit splitter = new UniformSplit();

    // Check splitting while starting from scratch
    try {
      splitter.split(1);
      throw new AssertionError("Splitting into <2 regions should have thrown exception");
    } catch (IllegalArgumentException e) { }

    byte[][] twoRegionsSplits = splitter.split(2);
    assertEquals(1, twoRegionsSplits.length);
    assertArrayEquals(twoRegionsSplits[0], new byte[] { (byte) 0x80, 0, 0, 0, 0, 0, 0, 0 });

    byte[][] threeRegionsSplits = splitter.split(3);
    assertEquals(2, threeRegionsSplits.length);
    byte[] expectedSplit0 = new byte[] {0x55, 0x55, 0x55, 0x55, 0x55, 0x55, 0x55, 0x55};
    assertArrayEquals(expectedSplit0, threeRegionsSplits[0]);
    byte[] expectedSplit1 = new byte[] {(byte)0xAA, (byte)0xAA, (byte)0xAA, (byte)0xAA,
      (byte)0xAA, (byte)0xAA, (byte)0xAA, (byte)0xAA};
    assertArrayEquals(expectedSplit1, threeRegionsSplits[1]);

    // Check splitting existing regions that have start and end points
    byte[] splitPoint = splitter.split(new byte[] {0x10}, new byte[] {0x30});
    assertArrayEquals(new byte[] { 0x20 }, splitPoint);

    byte[] lastRow = new byte[] {xFF, xFF, xFF, xFF, xFF, xFF, xFF, xFF};
    assertArrayEquals(lastRow, splitter.lastRow());
    byte[] firstRow = ArrayUtils.EMPTY_BYTE_ARRAY;
    assertArrayEquals(firstRow, splitter.firstRow());

    splitPoint = splitter.split(firstRow, new byte[] {0x20});
    assertArrayEquals(splitPoint, new byte[] { 0x10 });

    splitPoint = splitter.split(new byte[] {(byte)0xdf, xFF, xFF, xFF, xFF,
      xFF, xFF, xFF}, lastRow);
    assertArrayEquals(splitPoint, new byte[] { (byte) 0xef, xFF, xFF, xFF, xFF, xFF, xFF, xFF});

    splitPoint = splitter.split(new byte[] {'a', 'a', 'a'}, new byte[] {'a', 'a', 'b'});
    assertArrayEquals(splitPoint, new byte[] { 'a', 'a', 'a', (byte) 0x80 });

    // Check splitting region with multiple mappers per region
    byte[][] splits = splitter.split(new byte[] {'a', 'a', 'a'}, new byte[] {'a', 'a', 'd'},
        3, false);
    assertEquals(2, splits.length);
    assertArrayEquals(splits[0], new byte[]{'a', 'a', 'b'});
    assertArrayEquals(splits[1], new byte[]{'a', 'a', 'c'});

    splits = splitter.split(new byte[] {'a', 'a', 'a'}, new byte[] {'a', 'a', 'e'}, 2, true);
    assertEquals(3, splits.length);
    assertArrayEquals(splits[1], new byte[] { 'a', 'a', 'c'});
  }

  @Test
  public void testUserInput() {
    SplitAlgorithm algo = new HexStringSplit();
    assertFalse(splitFailsPrecondition(algo)); // default settings are fine
    assertFalse(splitFailsPrecondition(algo, "00", "AA")); // custom is fine
    assertTrue(splitFailsPrecondition(algo, "AA", "00")); // range error
    assertTrue(splitFailsPrecondition(algo, "AA", "AA")); // range error
    assertFalse(splitFailsPrecondition(algo, "0", "2", 3)); // should be fine
    assertFalse(splitFailsPrecondition(algo, "0", "A", 11)); // should be fine
    assertTrue(splitFailsPrecondition(algo, "0", "A", 12)); // too granular

    algo = new DecimalStringSplit();
    assertFalse(splitFailsPrecondition(algo)); // default settings are fine
    assertFalse(splitFailsPrecondition(algo, "00", "99")); // custom is fine
    assertTrue(splitFailsPrecondition(algo, "99", "00")); // range error
    assertTrue(splitFailsPrecondition(algo, "99", "99")); // range error
    assertFalse(splitFailsPrecondition(algo, "0", "2", 3)); // should be fine
    assertFalse(splitFailsPrecondition(algo, "0", "9", 10)); // should be fine
    assertTrue(splitFailsPrecondition(algo, "0", "9", 11)); // too granular

    algo = new UniformSplit();
    assertFalse(splitFailsPrecondition(algo)); // default settings are fine
    assertFalse(splitFailsPrecondition(algo, "\\x00", "\\xAA")); // custom is fine
    assertTrue(splitFailsPrecondition(algo, "\\xAA", "\\x00")); // range error
    assertTrue(splitFailsPrecondition(algo, "\\xAA", "\\xAA")); // range error
    assertFalse(splitFailsPrecondition(algo, "\\x00", "\\x02", 3)); // should be fine
    assertFalse(splitFailsPrecondition(algo, "\\x00", "\\x0A", 11)); // should be fine
    assertFalse(splitFailsPrecondition(algo, "\\x00", "\\x0A", 12)); // should be fine
  }

  private boolean splitFailsPrecondition(SplitAlgorithm algo) {
    return splitFailsPrecondition(algo, 100);
  }

  private boolean splitFailsPrecondition(SplitAlgorithm algo, String firstRow,
      String lastRow) {
    return splitFailsPrecondition(algo, firstRow, lastRow, 100);
  }

  private boolean splitFailsPrecondition(SplitAlgorithm algo, String firstRow,
      String lastRow, int numRegions) {
    algo.setFirstRow(firstRow);
    algo.setLastRow(lastRow);
    return splitFailsPrecondition(algo, numRegions);
  }

  private boolean splitFailsPrecondition(SplitAlgorithm algo, int numRegions) {
    try {
      byte[][] s = algo.split(numRegions);
      LOG.debug("split algo = " + algo);
      if (s != null) {
        StringBuilder sb = new StringBuilder();
        for (byte[] b : s) {
          sb.append(Bytes.toStringBinary(b) + "  ");
        }
        LOG.debug(sb.toString());
      }
      return false;
    } catch (IllegalArgumentException e) {
      return true;
    } catch (IllegalStateException e) {
      return true;
    } catch (IndexOutOfBoundsException e) {
      return true;
    }
  }

  /**
   * Creates a pre-split table with expectedBounds.size()+1 regions, then
   * verifies that the region boundaries are the same as the expected
   * region boundaries in expectedBounds.
   * @throws Various junit assertions
   */
  private void preSplitTableAndVerify(List<byte[]> expectedBounds,
      String splitClass, TableName tableName) throws Exception {
    final int numRegions = expectedBounds.size()-1;
    final Configuration conf = UTIL.getConfiguration();
    conf.setInt("split.count", numRegions);
    SplitAlgorithm splitAlgo = RegionSplitter.newSplitAlgoInstance(conf, splitClass);
    RegionSplitter.createPresplitTable(tableName, splitAlgo, new String[] { CF_NAME }, conf);
    verifyBounds(expectedBounds, tableName);
  }

  @Test
  public void noopRollingSplit() throws Exception {
    final List<byte[]> expectedBounds = new ArrayList<>(1);
    expectedBounds.add(ArrayUtils.EMPTY_BYTE_ARRAY);
    rollingSplitAndVerify(TableName.valueOf(TestRegionSplitter.class.getSimpleName()),
        "UniformSplit", expectedBounds);
  }

  private void rollingSplitAndVerify(TableName tableName, String splitClass,
      List<byte[]> expectedBounds)  throws Exception {
    final Configuration conf = UTIL.getConfiguration();

    // Set this larger than the number of splits so RegionSplitter won't block
    conf.setInt("split.outstanding", 5);
    SplitAlgorithm splitAlgo = RegionSplitter.newSplitAlgoInstance(conf, splitClass);
    RegionSplitter.rollingSplit(tableName, splitAlgo, conf);
    verifyBounds(expectedBounds, tableName);
  }

  private void verifyBounds(List<byte[]> expectedBounds, TableName tableName)
          throws Exception {
    // Get region boundaries from the cluster and verify their endpoints
    final int numRegions = expectedBounds.size()-1;
    try (Table table = UTIL.getConnection().getTable(tableName);
        RegionLocator locator = UTIL.getConnection().getRegionLocator(tableName)) {
      final List<HRegionLocation> regionInfoMap = locator.getAllRegionLocations();
      assertEquals(numRegions, regionInfoMap.size());
      for (HRegionLocation entry : regionInfoMap) {
        final HRegionInfo regionInfo = entry.getRegionInfo();
        byte[] regionStart = regionInfo.getStartKey();
        byte[] regionEnd = regionInfo.getEndKey();

        // This region's start key should be one of the region boundaries
        int startBoundaryIndex = indexOfBytes(expectedBounds, regionStart);
        assertNotSame(-1, startBoundaryIndex);

        // This region's end key should be the region boundary that comes
        // after the starting boundary.
        byte[] expectedRegionEnd = expectedBounds.get(startBoundaryIndex + 1);
        assertEquals(0, Bytes.compareTo(regionEnd, expectedRegionEnd));
      }
    }
  }

  /**
   * List.indexOf() doesn't really work for a List&lt;byte[]>, because byte[]
   * doesn't override equals(). This method checks whether a list contains
   * a given element by checking each element using the byte array comparator.
   * @return the index of the first element that equals compareTo, or -1 if no elements are equal.
   */
  static private int indexOfBytes(List<byte[]> list,  byte[] compareTo) {
    int listIndex = 0;
    for(byte[] elem: list) {
      if(Bytes.BYTES_COMPARATOR.compare(elem, compareTo) == 0) {
        return listIndex;
      }
      listIndex++;
    }
    return -1;
  }

}

