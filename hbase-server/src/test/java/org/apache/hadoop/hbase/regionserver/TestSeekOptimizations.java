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

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.regionserver.InternalScanner.NextState;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * Test various seek optimizations for correctness and check if they are
 * actually saving I/O operations.
 */
@RunWith(Parameterized.class)
@Category(MediumTests.class)
public class TestSeekOptimizations {

  private static final Log LOG =
      LogFactory.getLog(TestSeekOptimizations.class);

  // Constants
  private static final String FAMILY = "myCF";
  private static final byte[] FAMILY_BYTES = Bytes.toBytes(FAMILY);

  private static final int PUTS_PER_ROW_COL = 50;
  private static final int DELETES_PER_ROW_COL = 10;

  private static final int NUM_ROWS = 3;
  private static final int NUM_COLS = 3;

  private static final boolean VERBOSE = false;

  /**
   * Disable this when this test fails hopelessly and you need to debug a
   * simpler case.
   */
  private static final boolean USE_MANY_STORE_FILES = true;

  private static final int[][] COLUMN_SETS = new int[][] {
    {},  // All columns
    {0},
    {1},
    {0, 2},
    {1, 2},
    {0, 1, 2},
  };

  // Both start row and end row are inclusive here for the purposes of this
  // test.
  private static final int[][] ROW_RANGES = new int[][] {
    {-1, -1},
    {0, 1},
    {1, 1},
    {1, 2},
    {0, 2}
  };

  private static final int[] MAX_VERSIONS_VALUES = new int[] { 1, 2 };

  // Instance variables
  private HRegion region;
  private Put put;
  private Delete del;
  private Random rand;
  private Set<Long> putTimestamps = new HashSet<Long>();
  private Set<Long> delTimestamps = new HashSet<Long>();
  private List<Cell> expectedKVs = new ArrayList<Cell>();

  private Compression.Algorithm comprAlgo;
  private BloomType bloomType;

  private long totalSeekDiligent, totalSeekLazy;
  
  private final static HBaseTestingUtility TEST_UTIL = HBaseTestingUtility.createLocalHTU();

  @Parameters
  public static final Collection<Object[]> parameters() {
    return HBaseTestingUtility.BLOOM_AND_COMPRESSION_COMBINATIONS;
  }

  public TestSeekOptimizations(Compression.Algorithm comprAlgo,
      BloomType bloomType) {
    this.comprAlgo = comprAlgo;
    this.bloomType = bloomType;
  }

  @Before
  public void setUp() {
    rand = new Random(91238123L);
    expectedKVs.clear();
  }

  @Test
  public void testMultipleTimestampRanges() throws IOException {
    // enable seek counting
    StoreFileScanner.instrument();

    region = TEST_UTIL.createTestRegion("testMultipleTimestampRanges",
        new HColumnDescriptor(FAMILY)
            .setCompressionType(comprAlgo)
            .setBloomFilterType(bloomType)
            .setMaxVersions(3)
    );

    // Delete the given timestamp and everything before.
    final long latestDelTS = USE_MANY_STORE_FILES ? 1397 : -1;

    createTimestampRange(1, 50, -1);
    createTimestampRange(51, 100, -1);
    if (USE_MANY_STORE_FILES) {
      createTimestampRange(100, 500, 127);
      createTimestampRange(900, 1300, -1);
      createTimestampRange(1301, 2500, latestDelTS);
      createTimestampRange(2502, 2598, -1);
      createTimestampRange(2599, 2999, -1);
    }

    prepareExpectedKVs(latestDelTS);

    for (int[] columnArr : COLUMN_SETS) {
      for (int[] rowRange : ROW_RANGES) {
        for (int maxVersions : MAX_VERSIONS_VALUES) {
          for (boolean lazySeekEnabled : new boolean[] { false, true }) {
            testScan(columnArr, lazySeekEnabled, rowRange[0], rowRange[1],
                maxVersions);
          }
        }
      }
    }

    final double seekSavings = 1 - totalSeekLazy * 1.0 / totalSeekDiligent;
    System.err.println("For bloom=" + bloomType + ", compr=" + comprAlgo +
        " total seeks without optimization: " + totalSeekDiligent
        + ", with optimization: " + totalSeekLazy + " (" +
        String.format("%.2f%%", totalSeekLazy * 100.0 / totalSeekDiligent) +
        "), savings: " + String.format("%.2f%%",
            100.0 * seekSavings) + "\n");

    // Test that lazy seeks are buying us something. Without the actual
    // implementation of the lazy seek optimization this will be 0.
    final double expectedSeekSavings = 0.0;
    assertTrue("Lazy seek is only saving " +
        String.format("%.2f%%", seekSavings * 100) + " seeks but should " +
        "save at least " + String.format("%.2f%%", expectedSeekSavings * 100),
        seekSavings >= expectedSeekSavings);
  }

  private void testScan(final int[] columnArr, final boolean lazySeekEnabled,
      final int startRow, final int endRow, int maxVersions)
      throws IOException {
    StoreScanner.enableLazySeekGlobally(lazySeekEnabled);
    final Scan scan = new Scan();
    final Set<String> qualSet = new HashSet<String>();
    for (int iColumn : columnArr) {
      String qualStr = getQualStr(iColumn);
      scan.addColumn(FAMILY_BYTES, Bytes.toBytes(qualStr));
      qualSet.add(qualStr);
    }
    scan.setMaxVersions(maxVersions);
    scan.setStartRow(rowBytes(startRow));

    // Adjust for the fact that for multi-row queries the end row is exclusive.
    {
      final byte[] scannerStopRow =
          rowBytes(endRow + (startRow != endRow ? 1 : 0));
      scan.setStopRow(scannerStopRow);
    }

    final long initialSeekCount = StoreFileScanner.getSeekCount();
    final InternalScanner scanner = region.getScanner(scan);
    final List<Cell> results = new ArrayList<Cell>();
    final List<Cell> actualKVs = new ArrayList<Cell>();

    // Such a clumsy do-while loop appears to be the official way to use an
    // internalScanner. scanner.next() return value refers to the _next_
    // result, not to the one already returned in results.
    boolean hasNext;
    do {
      hasNext = NextState.hasMoreValues(scanner.next(results));
      actualKVs.addAll(results);
      results.clear();
    } while (hasNext);

    List<Cell> filteredKVs = filterExpectedResults(qualSet,
        rowBytes(startRow), rowBytes(endRow), maxVersions);
    final String rowRestrictionStr =
        (startRow == -1 && endRow == -1) ? "all rows" : (
            startRow == endRow ? ("row=" + startRow) : ("startRow="
            + startRow + ", " + "endRow=" + endRow));
    final String columnRestrictionStr =
        columnArr.length == 0 ? "all columns"
            : ("columns=" + Arrays.toString(columnArr));
    final String testDesc =
        "Bloom=" + bloomType + ", compr=" + comprAlgo + ", "
            + (scan.isGetScan() ? "Get" : "Scan") + ": "
            + columnRestrictionStr + ", " + rowRestrictionStr
            + ", maxVersions=" + maxVersions + ", lazySeek=" + lazySeekEnabled;
    long seekCount = StoreFileScanner.getSeekCount() - initialSeekCount;
    if (VERBOSE) {
      System.err.println("Seek count: " + seekCount + ", KVs returned: "
        + actualKVs.size() + ". " + testDesc +
        (lazySeekEnabled ? "\n" : ""));
    }
    if (lazySeekEnabled) {
      totalSeekLazy += seekCount;
    } else {
      totalSeekDiligent += seekCount;
    }
    assertKVListsEqual(testDesc, filteredKVs, actualKVs);
  }

  private List<Cell> filterExpectedResults(Set<String> qualSet,
      byte[] startRow, byte[] endRow, int maxVersions) {
    final List<Cell> filteredKVs = new ArrayList<Cell>();
    final Map<String, Integer> verCount = new HashMap<String, Integer>();
    for (Cell kv : expectedKVs) {
      if (startRow.length > 0 &&
          Bytes.compareTo(kv.getRowArray(), kv.getRowOffset(), kv.getRowLength(),
              startRow, 0, startRow.length) < 0) {
        continue;
      }

      // In this unit test the end row is always inclusive.
      if (endRow.length > 0 &&
          Bytes.compareTo(kv.getRowArray(), kv.getRowOffset(), kv.getRowLength(),
              endRow, 0, endRow.length) > 0) {
        continue;
      }

      if (!qualSet.isEmpty() && (!CellUtil.matchingFamily(kv, FAMILY_BYTES)
          || !qualSet.contains(Bytes.toString(CellUtil.cloneQualifier(kv))))) {
        continue;
      }

      final String rowColStr =
        Bytes.toStringBinary(CellUtil.cloneRow(kv)) + "/"
            + Bytes.toStringBinary(CellUtil.cloneFamily(kv)) + ":"
            + Bytes.toStringBinary(CellUtil.cloneQualifier(kv));
      final Integer curNumVer = verCount.get(rowColStr);
      final int newNumVer = curNumVer != null ? (curNumVer + 1) : 1;
      if (newNumVer <= maxVersions) {
        filteredKVs.add(kv);
        verCount.put(rowColStr, newNumVer);
      }
    }

    return filteredKVs;
  }

  private void prepareExpectedKVs(long latestDelTS) {
    final List<Cell> filteredKVs = new ArrayList<Cell>();
    for (Cell kv : expectedKVs) {
      if (kv.getTimestamp() > latestDelTS || latestDelTS == -1) {
        filteredKVs.add(kv);
      }
    }
    expectedKVs = filteredKVs;
    Collections.sort(expectedKVs, KeyValue.COMPARATOR);
  }

  public void put(String qual, long ts) {
    if (!putTimestamps.contains(ts)) {
      put.add(FAMILY_BYTES, Bytes.toBytes(qual), ts, createValue(ts));
      putTimestamps.add(ts);
    }
    if (VERBOSE) {
      LOG.info("put: row " + Bytes.toStringBinary(put.getRow())
          + ", cf " + FAMILY + ", qualifier " + qual + ", ts " + ts);
    }
  }

  private byte[] createValue(long ts) {
    return Bytes.toBytes("value" + ts);
  }

  public void delAtTimestamp(String qual, long ts) {
    del.deleteColumn(FAMILY_BYTES, Bytes.toBytes(qual), ts);
    logDelete(qual, ts, "at");
  }

  private void logDelete(String qual, long ts, String delType) {
    if (VERBOSE) {
      LOG.info("del " + delType + ": row "
          + Bytes.toStringBinary(put.getRow()) + ", cf " + FAMILY
          + ", qualifier " + qual + ", ts " + ts);
    }
  }

  private void delUpToTimestamp(String qual, long upToTS) {
    del.deleteColumns(FAMILY_BYTES, Bytes.toBytes(qual), upToTS);
    logDelete(qual, upToTS, "up to and including");
  }

  private long randLong(long n) {
    long l = rand.nextLong();
    if (l == Long.MIN_VALUE)
      l = Long.MAX_VALUE;
    return Math.abs(l) % n;
  }

  private long randBetween(long a, long b) {
    long x = a + randLong(b - a + 1);
    assertTrue(a <= x && x <= b);
    return x;
  }

  private final String rowStr(int i) {
    return ("row" + i).intern();
  }

  private final byte[] rowBytes(int i) {
    if (i == -1) {
      return HConstants.EMPTY_BYTE_ARRAY;
    }
    return Bytes.toBytes(rowStr(i));
  }

  private final String getQualStr(int i) {
    return ("qual" + i).intern();
  }

  public void createTimestampRange(long minTS, long maxTS,
      long deleteUpToTS) throws IOException {
    assertTrue(minTS < maxTS);
    assertTrue(deleteUpToTS == -1
        || (minTS <= deleteUpToTS && deleteUpToTS <= maxTS));

    for (int iRow = 0; iRow < NUM_ROWS; ++iRow) {
      final String row = rowStr(iRow);
      final byte[] rowBytes = Bytes.toBytes(row);
      for (int iCol = 0; iCol < NUM_COLS; ++iCol) {
        final String qual = getQualStr(iCol);
        final byte[] qualBytes = Bytes.toBytes(qual);
        put = new Put(rowBytes);

        putTimestamps.clear();
        put(qual, minTS);
        put(qual, maxTS);
        for (int i = 0; i < PUTS_PER_ROW_COL; ++i) {
          put(qual, randBetween(minTS, maxTS));
        }

        long[] putTimestampList = new long[putTimestamps.size()];
        {
          int i = 0;
          for (long ts : putTimestamps) {
            putTimestampList[i++] = ts;
          }
        }

        // Delete a predetermined number of particular timestamps
        delTimestamps.clear();
        assertTrue(putTimestampList.length >= DELETES_PER_ROW_COL);
        int numToDel = DELETES_PER_ROW_COL;
        int tsRemaining = putTimestampList.length;
        del = new Delete(rowBytes);
        for (long ts : putTimestampList) {
          if (rand.nextInt(tsRemaining) < numToDel) {
            delAtTimestamp(qual, ts);
            putTimestamps.remove(ts);
            --numToDel;
          }

          if (--tsRemaining == 0) {
            break;
          }
        }

        // Another type of delete: everything up to the given timestamp.
        if (deleteUpToTS != -1) {
          delUpToTimestamp(qual, deleteUpToTS);
        }

        region.put(put);
        if (!del.isEmpty()) {
          region.delete(del);
        }

        // Add remaining timestamps (those we have not deleted) to expected
        // results
        for (long ts : putTimestamps) {
          expectedKVs.add(new KeyValue(rowBytes, FAMILY_BYTES, qualBytes, ts,
              KeyValue.Type.Put));
        }
      }
    }

    region.flush(true);
  }

  @After
  public void tearDown() throws IOException {
    if (region != null) {
      HRegion.closeHRegion(region);
    }

    // We have to re-set the lazy seek flag back to the default so that other
    // unit tests are not affected.
    StoreScanner.enableLazySeekGlobally(
        StoreScanner.LAZY_SEEK_ENABLED_BY_DEFAULT);
  }


  public void assertKVListsEqual(String additionalMsg,
      final List<? extends Cell> expected,
      final List<? extends Cell> actual) {
    final int eLen = expected.size();
    final int aLen = actual.size();
    final int minLen = Math.min(eLen, aLen);

    int i;
    for (i = 0; i < minLen
        && KeyValue.COMPARATOR.compareOnlyKeyPortion(expected.get(i), actual.get(i)) == 0;
        ++i) {}

    if (additionalMsg == null) {
      additionalMsg = "";
    }
    if (!additionalMsg.isEmpty()) {
      additionalMsg = ". " + additionalMsg;
    }

    if (eLen != aLen || i != minLen) {
      throw new AssertionError(
          "Expected and actual KV arrays differ at position " + i + ": " +
          HBaseTestingUtility.safeGetAsStr(expected, i) + " (length " + eLen +") vs. " +
          HBaseTestingUtility.safeGetAsStr(actual, i) + " (length " + aLen + ")" + additionalMsg);
    }
  }

}

