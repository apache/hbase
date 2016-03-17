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
package org.apache.hadoop.hbase;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.ClientScanner;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.ColumnRangeFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.FirstKeyValueMatchingQualifiersFilter;
import org.apache.hadoop.hbase.filter.RandomRowFilter;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * These tests are focused on testing how partial results appear to a client. Partial results are
 * {@link Result}s that contain only a portion of a row's complete list of cells. Partial results
 * are formed when the server breaches its maximum result size when trying to service a client's RPC
 * request. It is the responsibility of the scanner on the client side to recognize when partial
 * results have been returned and to take action to form the complete results.
 * <p>
 * Unless the flag {@link Scan#setAllowPartialResults(boolean)} has been set to true, the caller of
 * {@link ResultScanner#next()} should never see partial results.
 */
@Category(MediumTests.class)
public class TestPartialResultsFromClientSide {
  private static final Log LOG = LogFactory.getLog(TestPartialResultsFromClientSide.class);

  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private final static int MINICLUSTER_SIZE = 5;
  private static Table TABLE = null;

  /**
   * Table configuration
   */
  private static TableName TABLE_NAME = TableName.valueOf("testTable");

  private static int NUM_ROWS = 5;
  private static byte[] ROW = Bytes.toBytes("testRow");
  private static byte[][] ROWS = HTestConst.makeNAscii(ROW, NUM_ROWS);

  // Should keep this value below 10 to keep generation of expected kv's simple. If above 10 then
  // table/row/cf1/... will be followed by table/row/cf10/... instead of table/row/cf2/... which
  // breaks the simple generation of expected kv's
  private static int NUM_FAMILIES = 10;
  private static byte[] FAMILY = Bytes.toBytes("testFamily");
  private static byte[][] FAMILIES = HTestConst.makeNAscii(FAMILY, NUM_FAMILIES);

  private static int NUM_QUALIFIERS = 10;
  private static byte[] QUALIFIER = Bytes.toBytes("testQualifier");
  private static byte[][] QUALIFIERS = HTestConst.makeNAscii(QUALIFIER, NUM_QUALIFIERS);

  private static int VALUE_SIZE = 1024;
  private static byte[] VALUE = Bytes.createMaxByteArray(VALUE_SIZE);

  private static int NUM_COLS = NUM_FAMILIES * NUM_QUALIFIERS;

  // Approximation of how large the heap size of cells in our table. Should be accessed through
  // getCellHeapSize().
  private static long CELL_HEAP_SIZE = -1;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniCluster(MINICLUSTER_SIZE);
    TEST_UTIL.getAdmin().setBalancerRunning(false, true);
    TABLE = createTestTable(TABLE_NAME, ROWS, FAMILIES, QUALIFIERS, VALUE);
  }

  static Table createTestTable(TableName name, byte[][] rows, byte[][] families,
      byte[][] qualifiers, byte[] cellValue) throws IOException {
    Table ht = TEST_UTIL.createTable(name, families);
    List<Put> puts = createPuts(rows, families, qualifiers, cellValue);
    ht.put(puts);

    return ht;
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  /**
   * Ensure that the expected key values appear in a result returned from a scanner that is
   * combining partial results into complete results
   * @throws Exception
   */
  @Test
  public void testExpectedValuesOfPartialResults() throws Exception {
    testExpectedValuesOfPartialResults(false);
    testExpectedValuesOfPartialResults(true);
  }

  public void testExpectedValuesOfPartialResults(boolean reversed) throws Exception {
    Scan partialScan = new Scan();
    partialScan.setMaxVersions();
    // Max result size of 1 ensures that each RPC request will return a single cell. The scanner
    // will need to reconstruct the results into a complete result before returning to the caller
    partialScan.setMaxResultSize(1);
    partialScan.setReversed(reversed);
    ResultScanner partialScanner = TABLE.getScanner(partialScan);

    final int startRow = reversed ? ROWS.length - 1 : 0;
    final int endRow = reversed ? -1 : ROWS.length;
    final int loopDelta = reversed ? -1 : 1;
    String message;

    for (int row = startRow; row != endRow; row = row + loopDelta) {
      message = "Ensuring the expected keyValues are present for row " + row;
      List<Cell> expectedKeyValues = createKeyValuesForRow(ROWS[row], FAMILIES, QUALIFIERS, VALUE);
      Result result = partialScanner.next();
      assertFalse(result.isPartial());
      verifyResult(result, expectedKeyValues, message);
    }

    partialScanner.close();
  }

  /**
   * Ensure that we only see Results marked as partial when the allowPartial flag is set
   * @throws Exception
   */
  @Test
  public void testAllowPartialResults() throws Exception {
    Scan scan = new Scan();
    scan.setAllowPartialResults(true);
    scan.setMaxResultSize(1);
    ResultScanner scanner = TABLE.getScanner(scan);
    Result result = scanner.next();

    assertTrue(result != null);
    assertTrue(result.isPartial());
    assertTrue(result.rawCells() != null);
    assertTrue(result.rawCells().length == 1);

    scanner.close();

    scan.setAllowPartialResults(false);
    scanner = TABLE.getScanner(scan);
    result = scanner.next();

    assertTrue(result != null);
    assertTrue(!result.isPartial());
    assertTrue(result.rawCells() != null);
    assertTrue(result.rawCells().length == NUM_COLS);

    scanner.close();
  }

  /**
   * Ensure that the results returned from a scanner that retrieves all results in a single RPC call
   * matches the results that are returned from a scanner that must incrementally combine partial
   * results into complete results. A variety of scan configurations can be tested
   * @throws Exception
   */
  @Test
  public void testEquivalenceOfScanResults() throws Exception {
    Scan oneShotScan = new Scan();
    oneShotScan.setMaxResultSize(Long.MAX_VALUE);

    Scan partialScan = new Scan(oneShotScan);
    partialScan.setMaxResultSize(1);

    testEquivalenceOfScanResults(TABLE, oneShotScan, partialScan);
  }

  public void testEquivalenceOfScanResults(Table table, Scan scan1, Scan scan2) throws Exception {
    ResultScanner scanner1 = table.getScanner(scan1);
    ResultScanner scanner2 = table.getScanner(scan2);

    Result r1 = null;
    Result r2 = null;
    int count = 0;

    while ((r1 = scanner1.next()) != null) {
      r2 = scanner2.next();

      assertTrue(r2 != null);
      compareResults(r1, r2, "Comparing result #" + count);
      count++;
    }

    r2 = scanner2.next();
    assertTrue("r2: " + r2 + " Should be null", r2 == null);

    scanner1.close();
    scanner2.close();
  }

  /**
   * Order of cells in partial results matches the ordering of cells from complete results
   * @throws Exception
   */
  @Test
  public void testOrderingOfCellsInPartialResults() throws Exception {
    Scan scan = new Scan();

    for (int col = 1; col <= NUM_COLS; col++) {
      scan.setMaxResultSize(getResultSizeForNumberOfCells(col));
      testOrderingOfCellsInPartialResults(scan);

      // Test again with a reversed scanner
      scan.setReversed(true);
      testOrderingOfCellsInPartialResults(scan);
    }
  }

  public void testOrderingOfCellsInPartialResults(final Scan basePartialScan) throws Exception {
    // Scan that retrieves results in pieces (partials). By setting allowPartialResults to be true
    // the results will NOT be reconstructed and instead the caller will see the partial results
    // returned by the server
    Scan partialScan = new Scan(basePartialScan);
    partialScan.setAllowPartialResults(true);
    ResultScanner partialScanner = TABLE.getScanner(partialScan);

    // Scan that retrieves all table results in single RPC request
    Scan oneShotScan = new Scan(basePartialScan);
    oneShotScan.setMaxResultSize(Long.MAX_VALUE);
    oneShotScan.setCaching(ROWS.length);
    ResultScanner oneShotScanner = TABLE.getScanner(oneShotScan);

    Result oneShotResult = oneShotScanner.next();
    Result partialResult = null;
    int iterationCount = 0;

    while (oneShotResult != null && oneShotResult.rawCells() != null) {
      List<Cell> aggregatePartialCells = new ArrayList<Cell>();
      do {
        partialResult = partialScanner.next();
        assertTrue("Partial Result is null. iteration: " + iterationCount, partialResult != null);
        assertTrue("Partial cells are null. iteration: " + iterationCount,
            partialResult.rawCells() != null);

        for (Cell c : partialResult.rawCells()) {
          aggregatePartialCells.add(c);
        }
      } while (partialResult.isPartial());

      assertTrue("Number of cells differs. iteration: " + iterationCount,
          oneShotResult.rawCells().length == aggregatePartialCells.size());
      final Cell[] oneShotCells = oneShotResult.rawCells();
      for (int cell = 0; cell < oneShotCells.length; cell++) {
        Cell oneShotCell = oneShotCells[cell];
        Cell partialCell = aggregatePartialCells.get(cell);

        assertTrue("One shot cell was null", oneShotCell != null);
        assertTrue("Partial cell was null", partialCell != null);
        assertTrue("Cell differs. oneShotCell:" + oneShotCell + " partialCell:" + partialCell,
            oneShotCell.equals(partialCell));
      }

      oneShotResult = oneShotScanner.next();
      iterationCount++;
    }

    assertTrue(partialScanner.next() == null);

    partialScanner.close();
    oneShotScanner.close();
  }

  /**
   * Setting the max result size allows us to control how many cells we expect to see on each call
   * to next on the scanner. Test a variety of different sizes for correctness
   * @throws Exception
   */
  @Test
  public void testExpectedNumberOfCellsPerPartialResult() throws Exception {
    Scan scan = new Scan();
    testExpectedNumberOfCellsPerPartialResult(scan);

    scan.setReversed(true);
    testExpectedNumberOfCellsPerPartialResult(scan);
  }

  public void testExpectedNumberOfCellsPerPartialResult(Scan baseScan) throws Exception {
    for (int expectedCells = 1; expectedCells <= NUM_COLS; expectedCells++) {
      testExpectedNumberOfCellsPerPartialResult(baseScan, expectedCells);
    }
  }

  public void testExpectedNumberOfCellsPerPartialResult(Scan baseScan, int expectedNumberOfCells)
      throws Exception {

    if (LOG.isInfoEnabled()) LOG.info("groupSize:" + expectedNumberOfCells);

    // Use the cellHeapSize to set maxResultSize such that we know how many cells to expect back
    // from the call. The returned results should NOT exceed expectedNumberOfCells but may be less
    // than it in cases where expectedNumberOfCells is not an exact multiple of the number of
    // columns in the table.
    Scan scan = new Scan(baseScan);
    scan.setAllowPartialResults(true);
    scan.setMaxResultSize(getResultSizeForNumberOfCells(expectedNumberOfCells));

    ResultScanner scanner = TABLE.getScanner(scan);
    Result result = null;
    byte[] prevRow = null;
    while ((result = scanner.next()) != null) {
      assertTrue(result.rawCells() != null);

      // Cases when cell count won't equal expectedNumberOfCells:
      // 1. Returned result is the final result needed to form the complete result for that row
      // 2. It is the first result we have seen for that row and thus may have been fetched as
      // the last group of cells that fit inside the maxResultSize
      assertTrue(
          "Result's cell count differed from expected number. result: " + result,
          result.rawCells().length == expectedNumberOfCells || !result.isPartial()
              || !Bytes.equals(prevRow, result.getRow()));
      prevRow = result.getRow();
    }

    scanner.close();
  }

  /**
   * @return The approximate heap size of a cell in the test table. All cells should have
   *         approximately the same heap size, so the value is cached to avoid repeating the
   *         calculation
   * @throws Exception
   */
  private long getCellHeapSize() throws Exception {
    if (CELL_HEAP_SIZE == -1) {
      // Do a partial scan that will return a single result with a single cell
      Scan scan = new Scan();
      scan.setMaxResultSize(1);
      scan.setAllowPartialResults(true);
      ResultScanner scanner = TABLE.getScanner(scan);

      Result result = scanner.next();

      assertTrue(result != null);
      assertTrue(result.rawCells() != null);
      assertTrue(result.rawCells().length == 1);

      CELL_HEAP_SIZE = CellUtil.estimatedHeapSizeOf(result.rawCells()[0]);
      if (LOG.isInfoEnabled()) LOG.info("Cell heap size: " + CELL_HEAP_SIZE);
      scanner.close();
    }

    return CELL_HEAP_SIZE;
  }

  /**
   * @param numberOfCells
   * @return the result size that should be used in {@link Scan#setMaxResultSize(long)} if you want
   *         the server to return exactly numberOfCells cells
   * @throws Exception
   */
  private long getResultSizeForNumberOfCells(int numberOfCells) throws Exception {
    return getCellHeapSize() * numberOfCells;
  }

  /**
   * Test various combinations of batching and partial results for correctness
   */
  @Test
  public void testPartialResultsAndBatch() throws Exception {
    for (int batch = 1; batch <= NUM_COLS / 4; batch++) {
      for (int cellsPerPartial = 1; cellsPerPartial <= NUM_COLS / 4; cellsPerPartial++) {
        testPartialResultsAndBatch(batch, cellsPerPartial);
      }
    }
  }

  public void testPartialResultsAndBatch(final int batch, final int cellsPerPartialResult)
      throws Exception {
    if (LOG.isInfoEnabled()) {
      LOG.info("batch: " + batch + " cellsPerPartialResult: " + cellsPerPartialResult);
    }

    Scan scan = new Scan();
    scan.setMaxResultSize(getResultSizeForNumberOfCells(cellsPerPartialResult));
    scan.setBatch(batch);
    ResultScanner scanner = TABLE.getScanner(scan);
    Result result = scanner.next();
    int repCount = 0;

    while ((result = scanner.next()) != null) {
      assertTrue(result.rawCells() != null);

      if (result.isPartial()) {
        final String error =
            "Cells:" + result.rawCells().length + " Batch size:" + batch
                + " cellsPerPartialResult:" + cellsPerPartialResult + " rep:" + repCount;
        assertTrue(error, result.rawCells().length <= Math.min(batch, cellsPerPartialResult));
      } else {
        assertTrue(result.rawCells().length <= batch);
      }
      repCount++;
    }

    scanner.close();
  }

  /**
   * Test the method {@link Result#createCompleteResult(List)}
   * @throws Exception
   */
  @Test
  public void testPartialResultsReassembly() throws Exception {
    Scan scan = new Scan();
    testPartialResultsReassembly(scan);
    scan.setReversed(true);
    testPartialResultsReassembly(scan);
  }

  public void testPartialResultsReassembly(Scan scanBase) throws Exception {
    Scan partialScan = new Scan(scanBase);
    partialScan.setMaxResultSize(1);
    partialScan.setAllowPartialResults(true);
    ResultScanner partialScanner = TABLE.getScanner(partialScan);

    Scan oneShotScan = new Scan(scanBase);
    oneShotScan.setMaxResultSize(Long.MAX_VALUE);
    ResultScanner oneShotScanner = TABLE.getScanner(oneShotScan);

    ArrayList<Result> partials = new ArrayList<>();
    for (int i = 0; i < NUM_ROWS; i++) {
      Result partialResult = null;
      Result completeResult = null;
      Result oneShotResult = null;
      partials.clear();

      do {
        partialResult = partialScanner.next();
        partials.add(partialResult);
      } while (partialResult != null && partialResult.isPartial());

      completeResult = Result.createCompleteResult(partials);
      oneShotResult = oneShotScanner.next();

      compareResults(completeResult, oneShotResult, null);
    }

    assertTrue(oneShotScanner.next() == null);
    assertTrue(partialScanner.next() == null);

    oneShotScanner.close();
    partialScanner.close();
  }

  /**
   * When reconstructing the complete result from its partials we ensure that the row of each
   * partial result is the same. If one of the rows differs, an exception is thrown.
   */
  @Test
  public void testExceptionThrownOnMismatchedPartialResults() throws IOException {
    assertTrue(NUM_ROWS >= 2);

    ArrayList<Result> partials = new ArrayList<>();
    Scan scan = new Scan();
    scan.setMaxResultSize(Long.MAX_VALUE);
    ResultScanner scanner = TABLE.getScanner(scan);
    Result r1 = scanner.next();
    partials.add(r1);
    Result r2 = scanner.next();
    partials.add(r2);

    assertFalse(Bytes.equals(r1.getRow(), r2.getRow()));

    try {
      Result.createCompleteResult(partials);
      fail("r1 and r2 are from different rows. It should not be possible to combine them into"
          + " a single result");
    } catch (IOException e) {
    }

    scanner.close();
  }

  /**
   * When a scan has a filter where {@link org.apache.hadoop.hbase.filter.Filter#hasFilterRow()} is
   * true, the scanner should not return partial results. The scanner cannot return partial results
   * because the entire row needs to be read for the include/exclude decision to be made
   */
  @Test
  public void testNoPartialResultsWhenRowFilterPresent() throws Exception {
    Scan scan = new Scan();
    scan.setMaxResultSize(1);
    scan.setAllowPartialResults(true);
    // If a filter hasFilter() is true then partial results should not be returned else filter
    // application server side would break.
    scan.setFilter(new RandomRowFilter(1.0f));
    ResultScanner scanner = TABLE.getScanner(scan);

    Result r = null;
    while ((r = scanner.next()) != null) {
      assertFalse(r.isPartial());
    }

    scanner.close();
  }

  /**
   * Examine the interaction between the maxResultSize and caching. If the caching limit is reached
   * before the maxResultSize limit, we should not see partial results. On the other hand, if the
   * maxResultSize limit is reached before the caching limit, it is likely that partial results will
   * be seen.
   * @throws Exception
   */
  @Test
  public void testPartialResultsAndCaching() throws Exception {
    for (int caching = 1; caching <= NUM_ROWS; caching++) {
      for (int maxResultRows = 0; maxResultRows <= NUM_ROWS; maxResultRows++) {
        testPartialResultsAndCaching(maxResultRows, caching);
      }
    }
  }

  /**
   * @param resultSizeRowLimit The row limit that will be enforced through maxResultSize
   * @param cachingRowLimit The row limit that will be enforced through caching
   * @throws Exception
   */
  public void testPartialResultsAndCaching(int resultSizeRowLimit, int cachingRowLimit)
      throws Exception {
    Scan scan = new Scan();
    scan.setAllowPartialResults(true);

    // The number of cells specified in the call to getResultSizeForNumberOfCells is offset to
    // ensure that the result size we specify is not an exact multiple of the number of cells
    // in a row. This ensures that partial results will be returned when the result size limit
    // is reached before the caching limit.
    int cellOffset = NUM_COLS / 3;
    long maxResultSize = getResultSizeForNumberOfCells(resultSizeRowLimit * NUM_COLS + cellOffset);
    scan.setMaxResultSize(maxResultSize);
    scan.setCaching(cachingRowLimit);

    ResultScanner scanner = TABLE.getScanner(scan);
    ClientScanner clientScanner = (ClientScanner) scanner;
    Result r = null;

    // Approximate the number of rows we expect will fit into the specified max rsult size. If this
    // approximation is less than caching, then we expect that the max result size limit will be
    // hit before the caching limit and thus partial results may be seen
    boolean expectToSeePartialResults = resultSizeRowLimit < cachingRowLimit;
    while ((r = clientScanner.next()) != null) {
      assertTrue(!r.isPartial() || expectToSeePartialResults);
    }

    scanner.close();
  }

  /**
   * Small scans should not return partial results because it would prevent small scans from
   * retrieving all of the necessary results in a single RPC request which is what makese small
   * scans useful. Thus, ensure that even when {@link Scan#getAllowPartialResults()} is true, small
   * scans do not return partial results
   * @throws Exception
   */
  @Test
  public void testSmallScansDoNotAllowPartials() throws Exception {
    Scan scan = new Scan();
    testSmallScansDoNotAllowPartials(scan);
    scan.setReversed(true);
    testSmallScansDoNotAllowPartials(scan);
  }

  public void testSmallScansDoNotAllowPartials(Scan baseScan) throws Exception {
    Scan scan = new Scan(baseScan);
    scan.setAllowPartialResults(true);
    scan.setSmall(true);
    scan.setMaxResultSize(1);

    ResultScanner scanner = TABLE.getScanner(scan);
    Result r = null;

    while ((r = scanner.next()) != null) {
      assertFalse(r.isPartial());
    }

    scanner.close();
  }

  /**
   * Make puts to put the input value into each combination of row, family, and qualifier
   * @param rows
   * @param families
   * @param qualifiers
   * @param value
   * @return
   * @throws IOException
   */
  static ArrayList<Put> createPuts(byte[][] rows, byte[][] families, byte[][] qualifiers,
      byte[] value) throws IOException {
    Put put;
    ArrayList<Put> puts = new ArrayList<>();

    for (int row = 0; row < rows.length; row++) {
      put = new Put(rows[row]);
      for (int fam = 0; fam < families.length; fam++) {
        for (int qual = 0; qual < qualifiers.length; qual++) {
          KeyValue kv = new KeyValue(rows[row], families[fam], qualifiers[qual], qual, value);
          put.add(kv);
        }
      }
      puts.add(put);
    }

    return puts;
  }

  /**
   * Make key values to represent each possible combination of family and qualifier in the specified
   * row.
   * @param row
   * @param families
   * @param qualifiers
   * @param value
   * @return
   */
  static ArrayList<Cell> createKeyValuesForRow(byte[] row, byte[][] families, byte[][] qualifiers,
      byte[] value) {
    ArrayList<Cell> outList = new ArrayList<>();
    for (int fam = 0; fam < families.length; fam++) {
      for (int qual = 0; qual < qualifiers.length; qual++) {
        outList.add(new KeyValue(row, families[fam], qualifiers[qual], qual, value));
      }
    }
    return outList;
  }

  /**
   * Verifies that result contains all the key values within expKvList. Fails the test otherwise
   * @param result
   * @param expKvList
   * @param msg
   */
  static void verifyResult(Result result, List<Cell> expKvList, String msg) {
    if (LOG.isInfoEnabled()) {
      LOG.info(msg);
      LOG.info("Expected count: " + expKvList.size());
      LOG.info("Actual count: " + result.size());
    }

    if (expKvList.size() == 0) return;

    int i = 0;
    for (Cell kv : result.rawCells()) {
      if (i >= expKvList.size()) {
        break; // we will check the size later
      }

      Cell kvExp = expKvList.get(i++);
      assertTrue("Not equal. get kv: " + kv.toString() + " exp kv: " + kvExp.toString(),
          kvExp.equals(kv));
    }

    assertEquals(expKvList.size(), result.size());
  }

  /**
   * Compares two results and fails the test if the results are different
   * @param r1
   * @param r2
   * @param message
   */
  static void compareResults(Result r1, Result r2, final String message) {
    if (LOG.isInfoEnabled()) {
      if (message != null) LOG.info(message);
      LOG.info("r1: " + r1);
      LOG.info("r2: " + r2);
    }

    final String failureMessage = "Results r1:" + r1 + " \nr2:" + r2 + " are not equivalent";
    if (r1 == null && r2 == null) fail(failureMessage);
    else if (r1 == null || r2 == null) fail(failureMessage);

    try {
      Result.compareResults(r1, r2);
    } catch (Exception e) {
      fail(failureMessage);
    }
  }

  @Test
  public void testReadPointAndPartialResults() throws Exception {
    TableName testName = TableName.valueOf("testReadPointAndPartialResults");
    int numRows = 5;
    int numFamilies = 5;
    int numQualifiers = 5;
    byte[][] rows = HTestConst.makeNAscii(Bytes.toBytes("testRow"), numRows);
    byte[][] families = HTestConst.makeNAscii(Bytes.toBytes("testFamily"), numFamilies);
    byte[][] qualifiers = HTestConst.makeNAscii(Bytes.toBytes("testQualifier"), numQualifiers);
    byte[] value = Bytes.createMaxByteArray(100);

    Table tmpTable = createTestTable(testName, rows, families, qualifiers, value);

    Scan scan = new Scan();
    scan.setMaxResultSize(1);
    scan.setAllowPartialResults(true);

    // Open scanner before deletes
    ResultScanner scanner = tmpTable.getScanner(scan);

    Delete delete1 = new Delete(rows[0]);
    delete1.addColumn(families[0], qualifiers[0], 0);
    tmpTable.delete(delete1);

    Delete delete2 = new Delete(rows[1]);
    delete2.addColumn(families[1], qualifiers[1], 1);
    tmpTable.delete(delete2);

    // Should see all cells because scanner was opened prior to deletes
    int scannerCount = countCellsFromScanner(scanner);
    int expectedCount = numRows * numFamilies * numQualifiers;
    assertTrue("scannerCount: " + scannerCount + " expectedCount: " + expectedCount,
        scannerCount == expectedCount);

    // Minus 2 for the two cells that were deleted
    scanner = tmpTable.getScanner(scan);
    scannerCount = countCellsFromScanner(scanner);
    expectedCount = numRows * numFamilies * numQualifiers - 2;
    assertTrue("scannerCount: " + scannerCount + " expectedCount: " + expectedCount,
        scannerCount == expectedCount);

    scanner = tmpTable.getScanner(scan);
    // Put in 2 new rows. The timestamps differ from the deleted rows
    Put put1 = new Put(rows[0]);
    put1.add(new KeyValue(rows[0], families[0], qualifiers[0], 1, value));
    tmpTable.put(put1);

    Put put2 = new Put(rows[1]);
    put2.add(new KeyValue(rows[1], families[1], qualifiers[1], 2, value));
    tmpTable.put(put2);

    // Scanner opened prior to puts. Cell count shouldn't have changed
    scannerCount = countCellsFromScanner(scanner);
    expectedCount = numRows * numFamilies * numQualifiers - 2;
    assertTrue("scannerCount: " + scannerCount + " expectedCount: " + expectedCount,
        scannerCount == expectedCount);

    // Now the scanner should see the cells that were added by puts
    scanner = tmpTable.getScanner(scan);
    scannerCount = countCellsFromScanner(scanner);
    expectedCount = numRows * numFamilies * numQualifiers;
    assertTrue("scannerCount: " + scannerCount + " expectedCount: " + expectedCount,
        scannerCount == expectedCount);

    TEST_UTIL.deleteTable(testName);
  }

  /**
   * Exhausts the scanner by calling next repetitively. Once completely exhausted, close scanner and
   * return total cell count
   * @param scanner
   * @return
   * @throws Exception
   */
  private int countCellsFromScanner(ResultScanner scanner) throws Exception {
    Result result = null;
    int numCells = 0;
    while ((result = scanner.next()) != null) {
      numCells += result.rawCells().length;
    }

    scanner.close();
    return numCells;
  }

  /**
   * Test partial Result re-assembly in the presence of different filters. The Results from the
   * partial scanner should match the Results returned from a scanner that receives all of the
   * results in one RPC to the server. The partial scanner is tested with a variety of different
   * result sizes (all of which are less than the size necessary to fetch an entire row)
   * @throws Exception
   */
  @Test
  public void testPartialResultsWithColumnFilter() throws Exception {
    testPartialResultsWithColumnFilter(new FirstKeyOnlyFilter());
    testPartialResultsWithColumnFilter(new ColumnPrefixFilter(Bytes.toBytes("testQualifier5")));
    testPartialResultsWithColumnFilter(new ColumnRangeFilter(Bytes.toBytes("testQualifer1"), true,
        Bytes.toBytes("testQualifier7"), true));

    Set<byte[]> qualifiers = new LinkedHashSet<>();
    qualifiers.add(Bytes.toBytes("testQualifier5"));
    testPartialResultsWithColumnFilter(new FirstKeyValueMatchingQualifiersFilter(qualifiers));
  }

  public void testPartialResultsWithColumnFilter(Filter filter) throws Exception {
    assertTrue(!filter.hasFilterRow());

    Scan partialScan = new Scan();
    partialScan.setFilter(filter);

    Scan oneshotScan = new Scan();
    oneshotScan.setFilter(filter);
    oneshotScan.setMaxResultSize(Long.MAX_VALUE);

    for (int i = 1; i <= NUM_COLS; i++) {
      partialScan.setMaxResultSize(getResultSizeForNumberOfCells(i));
      testEquivalenceOfScanResults(TABLE, partialScan, oneshotScan);
    }
  }

  private void moveRegion(Table table, int index) throws IOException{
    List<Pair<HRegionInfo, ServerName>> regions = MetaTableAccessor
        .getTableRegionsAndLocations(TEST_UTIL.getConnection(),
            table.getName());
    assertEquals(1, regions.size());
    HRegionInfo regionInfo = regions.get(0).getFirst();
    ServerName name = TEST_UTIL.getHBaseCluster().getRegionServer(index).getServerName();
    TEST_UTIL.getAdmin().move(regionInfo.getEncodedNameAsBytes(),
        Bytes.toBytes(name.getServerName()));
  }

  private void assertCell(Cell cell, byte[] row, byte[] cf, byte[] cq) {
    assertArrayEquals(row,
        Bytes.copy(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength()));
    assertArrayEquals(cf,
        Bytes.copy(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength()));
    assertArrayEquals(cq,
        Bytes.copy(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()));
  }

  @Test
  public void testPartialResultWhenRegionMove() throws IOException {
    Table table=createTestTable(TableName.valueOf("testPartialResultWhenRegionMove"),
        ROWS, FAMILIES, QUALIFIERS, VALUE);

    moveRegion(table, 1);

    Scan scan = new Scan();
    scan.setMaxResultSize(1);
    scan.setAllowPartialResults(true);
    ResultScanner scanner = table.getScanner(scan);
    for (int i = 0; i < NUM_FAMILIES * NUM_QUALIFIERS - 1; i++) {
      scanner.next();
    }
    Result result1 = scanner.next();
    assertEquals(1, result1.rawCells().length);
    Cell c1 = result1.rawCells()[0];
    assertCell(c1, ROWS[0], FAMILIES[NUM_FAMILIES - 1], QUALIFIERS[NUM_QUALIFIERS - 1]);
    assertFalse(result1.isPartial());

    moveRegion(table, 2);

    Result result2 = scanner.next();
    assertEquals(1, result2.rawCells().length);
    Cell c2 = result2.rawCells()[0];
    assertCell(c2, ROWS[1], FAMILIES[0], QUALIFIERS[0]);
    assertTrue(result2.isPartial());

    moveRegion(table, 3);

    Result result3 = scanner.next();
    assertEquals(1, result3.rawCells().length);
    Cell c3 = result3.rawCells()[0];
    assertCell(c3, ROWS[1], FAMILIES[0], QUALIFIERS[1]);
    assertTrue(result3.isPartial());

  }

  @Test
  public void testReversedPartialResultWhenRegionMove() throws IOException {
    Table table=createTestTable(TableName.valueOf("testReversedPartialResultWhenRegionMove"),
        ROWS, FAMILIES, QUALIFIERS, VALUE);

    moveRegion(table, 1);

    Scan scan = new Scan();
    scan.setMaxResultSize(1);
    scan.setAllowPartialResults(true);
    scan.setReversed(true);
    ResultScanner scanner = table.getScanner(scan);
    for (int i = 0; i < NUM_FAMILIES * NUM_QUALIFIERS-1; i++) {
      scanner.next();
    }
    Result result1 = scanner.next();
    assertEquals(1, result1.rawCells().length);
    Cell c1 = result1.rawCells()[0];
    assertCell(c1, ROWS[NUM_ROWS-1], FAMILIES[NUM_FAMILIES - 1], QUALIFIERS[NUM_QUALIFIERS - 1]);
    assertFalse(result1.isPartial());

    moveRegion(table, 2);

    Result result2 = scanner.next();
    assertEquals(1, result2.rawCells().length);
    Cell c2 = result2.rawCells()[0];
    assertCell(c2, ROWS[NUM_ROWS-2], FAMILIES[0], QUALIFIERS[0]);
    assertTrue(result2.isPartial());

    moveRegion(table, 3);

    Result result3 = scanner.next();
    assertEquals(1, result3.rawCells().length);
    Cell c3 = result3.rawCells()[0];
    assertCell(c3, ROWS[NUM_ROWS-2], FAMILIES[0], QUALIFIERS[1]);
    assertTrue(result3.isPartial());

  }

  @Test
  public void testCompleteResultWhenRegionMove() throws IOException {
    Table table=createTestTable(TableName.valueOf("testCompleteResultWhenRegionMove"),
        ROWS, FAMILIES, QUALIFIERS, VALUE);

    moveRegion(table, 1);

    Scan scan = new Scan();
    scan.setMaxResultSize(1);
    scan.setCaching(1);
    ResultScanner scanner = table.getScanner(scan);

    Result result1 = scanner.next();
    assertEquals(NUM_FAMILIES * NUM_QUALIFIERS, result1.rawCells().length);
    Cell c1 = result1.rawCells()[0];
    assertCell(c1, ROWS[0], FAMILIES[0], QUALIFIERS[0]);
    assertFalse(result1.isPartial());

    moveRegion(table, 2);

    Result result2 = scanner.next();
    assertEquals(NUM_FAMILIES * NUM_QUALIFIERS, result2.rawCells().length);
    Cell c2 = result2.rawCells()[0];
    assertCell(c2, ROWS[1], FAMILIES[0], QUALIFIERS[0]);
    assertFalse(result2.isPartial());

    moveRegion(table, 3);

    Result result3 = scanner.next();
    assertEquals(NUM_FAMILIES * NUM_QUALIFIERS, result3.rawCells().length);
    Cell c3 = result3.rawCells()[0];
    assertCell(c3, ROWS[2], FAMILIES[0], QUALIFIERS[0]);
    assertFalse(result3.isPartial());

  }

  @Test
  public void testReversedCompleteResultWhenRegionMove() throws IOException {
    Table table=createTestTable(TableName.valueOf("testReversedCompleteResultWhenRegionMove"),
        ROWS, FAMILIES, QUALIFIERS, VALUE);

    moveRegion(table, 1);

    Scan scan = new Scan();
    scan.setMaxResultSize(1);
    scan.setCaching(1);
    scan.setReversed(true);
    ResultScanner scanner = table.getScanner(scan);

    Result result1 = scanner.next();
    assertEquals(NUM_FAMILIES*NUM_QUALIFIERS, result1.rawCells().length);
    Cell c1 = result1.rawCells()[0];
    assertCell(c1, ROWS[NUM_ROWS-1], FAMILIES[0], QUALIFIERS[0]);
    assertFalse(result1.isPartial());

    moveRegion(table, 2);

    Result result2 = scanner.next();
    assertEquals(NUM_FAMILIES*NUM_QUALIFIERS, result2.rawCells().length);
    Cell c2 = result2.rawCells()[0];
    assertCell(c2, ROWS[NUM_ROWS-2], FAMILIES[0], QUALIFIERS[0]);
    assertFalse(result2.isPartial());

    moveRegion(table, 3);

    Result result3 = scanner.next();
    assertEquals(NUM_FAMILIES*NUM_QUALIFIERS, result3.rawCells().length);
    Cell c3 = result3.rawCells()[0];
    assertCell(c3, ROWS[NUM_ROWS-3], FAMILIES[0], QUALIFIERS[0]);
    assertFalse(result3.isPartial());

  }

  @Test
  public void testBatchingResultWhenRegionMove() throws IOException {
    Table table =
        createTestTable(TableName.valueOf("testBatchingResultWhenRegionMove"), ROWS, FAMILIES,
            QUALIFIERS, VALUE);

    moveRegion(table, 1);

    Scan scan = new Scan();
    scan.setCaching(1);
    scan.setBatch(1);

    ResultScanner scanner = table.getScanner(scan);
    for (int i = 0; i < NUM_FAMILIES * NUM_QUALIFIERS - 1; i++) {
      scanner.next();
    }
    Result result1 = scanner.next();
    assertEquals(1, result1.rawCells().length);
    Cell c1 = result1.rawCells()[0];
    assertCell(c1, ROWS[0], FAMILIES[NUM_FAMILIES - 1], QUALIFIERS[NUM_QUALIFIERS - 1]);

    moveRegion(table, 2);

    Result result2 = scanner.next();
    assertEquals(1, result2.rawCells().length);
    Cell c2 = result2.rawCells()[0];
    assertCell(c2, ROWS[1], FAMILIES[0], QUALIFIERS[0]);

    moveRegion(table, 3);

    Result result3 = scanner.next();
    assertEquals(1, result3.rawCells().length);
    Cell c3 = result3.rawCells()[0];
    assertCell(c3, ROWS[1], FAMILIES[0], QUALIFIERS[1]);
  }


}