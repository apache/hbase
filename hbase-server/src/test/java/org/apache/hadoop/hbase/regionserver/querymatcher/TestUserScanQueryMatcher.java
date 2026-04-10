/*
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
package org.apache.hadoop.hbase.regionserver.querymatcher;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.ExtendedCell;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeepDeletedCells;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.regionserver.ScanInfo;
import org.apache.hadoop.hbase.regionserver.querymatcher.ScanQueryMatcher.MatchCode;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({ RegionServerTests.class, SmallTests.class })
public class TestUserScanQueryMatcher extends AbstractTestScanQueryMatcher {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestUserScanQueryMatcher.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestUserScanQueryMatcher.class);

  /**
   * This is a cryptic test. It is checking that we don't include a fake cell. See HBASE-16074 for
   * background.
   */
  @Test
  public void testNeverIncludeFakeCell() throws IOException {
    long now = EnvironmentEdgeManager.currentTime();
    // Do with fam2 which has a col2 qualifier.
    UserScanQueryMatcher qm = UserScanQueryMatcher.create(scan,
      new ScanInfo(this.conf, fam2, 10, 1, ttl, KeepDeletedCells.FALSE,
        HConstants.DEFAULT_BLOCKSIZE, 0, rowComparator, false),
      get.getFamilyMap().get(fam2), now - ttl, now, null);
    ExtendedCell kv = new KeyValue(row1, fam2, col2, 1, data);
    ExtendedCell cell = PrivateCellUtil.createLastOnRowCol(kv);
    qm.setToNewRow(kv);
    MatchCode code = qm.match(cell);
    assertFalse(code.compareTo(MatchCode.SEEK_NEXT_COL) != 0);
  }

  @Test
  public void testMatchExplicitColumns() throws IOException {
    // Moving up from the Tracker by using Gets and List<KeyValue> instead
    // of just byte []

    // Expected result
    List<MatchCode> expected = new ArrayList<>(6);
    expected.add(ScanQueryMatcher.MatchCode.SEEK_NEXT_COL);
    expected.add(ScanQueryMatcher.MatchCode.INCLUDE_AND_SEEK_NEXT_COL);
    expected.add(ScanQueryMatcher.MatchCode.SEEK_NEXT_COL);
    expected.add(ScanQueryMatcher.MatchCode.INCLUDE_AND_SEEK_NEXT_COL);
    expected.add(ScanQueryMatcher.MatchCode.INCLUDE_AND_SEEK_NEXT_ROW);
    expected.add(ScanQueryMatcher.MatchCode.DONE);

    long now = EnvironmentEdgeManager.currentTime();
    // 2,4,5
    UserScanQueryMatcher qm = UserScanQueryMatcher.create(
      scan, new ScanInfo(this.conf, fam2, 0, 1, ttl, KeepDeletedCells.FALSE,
        HConstants.DEFAULT_BLOCKSIZE, 0, rowComparator, false),
      get.getFamilyMap().get(fam2), now - ttl, now, null);

    List<KeyValue> memstore = new ArrayList<>(6);
    memstore.add(new KeyValue(row1, fam2, col1, 1, data));
    memstore.add(new KeyValue(row1, fam2, col2, 1, data));
    memstore.add(new KeyValue(row1, fam2, col3, 1, data));
    memstore.add(new KeyValue(row1, fam2, col4, 1, data));
    memstore.add(new KeyValue(row1, fam2, col5, 1, data));

    memstore.add(new KeyValue(row2, fam1, col1, data));

    List<ScanQueryMatcher.MatchCode> actual = new ArrayList<>(memstore.size());
    KeyValue k = memstore.get(0);
    qm.setToNewRow(k);

    for (KeyValue kv : memstore) {
      actual.add(qm.match(kv));
    }

    assertEquals(expected.size(), actual.size());
    for (int i = 0; i < expected.size(); i++) {
      LOG.debug("expected " + expected.get(i) + ", actual " + actual.get(i));
      assertEquals(expected.get(i), actual.get(i));
    }
  }

  @Test
  public void testMatch_Wildcard() throws IOException {
    // Moving up from the Tracker by using Gets and List<KeyValue> instead
    // of just byte []

    // Expected result
    List<MatchCode> expected = new ArrayList<>(6);
    expected.add(ScanQueryMatcher.MatchCode.INCLUDE);
    expected.add(ScanQueryMatcher.MatchCode.INCLUDE);
    expected.add(ScanQueryMatcher.MatchCode.INCLUDE);
    expected.add(ScanQueryMatcher.MatchCode.INCLUDE);
    expected.add(ScanQueryMatcher.MatchCode.INCLUDE);
    expected.add(ScanQueryMatcher.MatchCode.DONE);

    long now = EnvironmentEdgeManager.currentTime();
    UserScanQueryMatcher qm = UserScanQueryMatcher.create(scan, new ScanInfo(this.conf, fam2, 0, 1,
      ttl, KeepDeletedCells.FALSE, HConstants.DEFAULT_BLOCKSIZE, 0, rowComparator, false), null,
      now - ttl, now, null);

    List<KeyValue> memstore = new ArrayList<>(6);
    memstore.add(new KeyValue(row1, fam2, col1, 1, data));
    memstore.add(new KeyValue(row1, fam2, col2, 1, data));
    memstore.add(new KeyValue(row1, fam2, col3, 1, data));
    memstore.add(new KeyValue(row1, fam2, col4, 1, data));
    memstore.add(new KeyValue(row1, fam2, col5, 1, data));
    memstore.add(new KeyValue(row2, fam1, col1, 1, data));

    List<ScanQueryMatcher.MatchCode> actual = new ArrayList<>(memstore.size());

    KeyValue k = memstore.get(0);
    qm.setToNewRow(k);

    for (KeyValue kv : memstore) {
      actual.add(qm.match(kv));
    }

    assertEquals(expected.size(), actual.size());
    for (int i = 0; i < expected.size(); i++) {
      LOG.debug("expected " + expected.get(i) + ", actual " + actual.get(i));
      assertEquals(expected.get(i), actual.get(i));
    }
  }

  /**
   * Verify that {@link ScanQueryMatcher} only skips expired KeyValue instances and does not exit
   * early from the row (skipping later non-expired KeyValues). This version mimics a Get with
   * explicitly specified column qualifiers.
   */
  @Test
  public void testMatch_ExpiredExplicit() throws IOException {

    long testTTL = 1000;
    MatchCode[] expected = new MatchCode[] { ScanQueryMatcher.MatchCode.SEEK_NEXT_COL,
      ScanQueryMatcher.MatchCode.INCLUDE_AND_SEEK_NEXT_COL,
      ScanQueryMatcher.MatchCode.SEEK_NEXT_COL,
      ScanQueryMatcher.MatchCode.INCLUDE_AND_SEEK_NEXT_COL,
      ScanQueryMatcher.MatchCode.SEEK_NEXT_ROW, ScanQueryMatcher.MatchCode.DONE };

    long now = EnvironmentEdgeManager.currentTime();
    UserScanQueryMatcher qm = UserScanQueryMatcher.create(scan,
      new ScanInfo(this.conf, fam2, 0, 1, testTTL, KeepDeletedCells.FALSE,
        HConstants.DEFAULT_BLOCKSIZE, 0, rowComparator, false),
      get.getFamilyMap().get(fam2), now - testTTL, now, null);

    KeyValue[] kvs = new KeyValue[] { new KeyValue(row1, fam2, col1, now - 100, data),
      new KeyValue(row1, fam2, col2, now - 50, data),
      new KeyValue(row1, fam2, col3, now - 5000, data),
      new KeyValue(row1, fam2, col4, now - 500, data),
      new KeyValue(row1, fam2, col5, now - 10000, data),
      new KeyValue(row2, fam1, col1, now - 10, data) };

    KeyValue k = kvs[0];
    qm.setToNewRow(k);

    List<MatchCode> actual = new ArrayList<>(kvs.length);
    for (KeyValue kv : kvs) {
      actual.add(qm.match(kv));
    }

    assertEquals(expected.length, actual.size());
    for (int i = 0; i < expected.length; i++) {
      LOG.debug("expected " + expected[i] + ", actual " + actual.get(i));
      assertEquals(expected[i], actual.get(i));
    }
  }

  /**
   * Verify that {@link ScanQueryMatcher} only skips expired KeyValue instances and does not exit
   * early from the row (skipping later non-expired KeyValues). This version mimics a Get with
   * wildcard-inferred column qualifiers.
   */
  @Test
  public void testMatch_ExpiredWildcard() throws IOException {

    long testTTL = 1000;
    MatchCode[] expected =
      new MatchCode[] { ScanQueryMatcher.MatchCode.INCLUDE, ScanQueryMatcher.MatchCode.INCLUDE,
        ScanQueryMatcher.MatchCode.SEEK_NEXT_COL, ScanQueryMatcher.MatchCode.INCLUDE,
        ScanQueryMatcher.MatchCode.SEEK_NEXT_COL, ScanQueryMatcher.MatchCode.DONE };

    long now = EnvironmentEdgeManager.currentTime();
    UserScanQueryMatcher qm = UserScanQueryMatcher.create(scan, new ScanInfo(this.conf, fam2, 0, 1,
      testTTL, KeepDeletedCells.FALSE, HConstants.DEFAULT_BLOCKSIZE, 0, rowComparator, false), null,
      now - testTTL, now, null);

    KeyValue[] kvs = new KeyValue[] { new KeyValue(row1, fam2, col1, now - 100, data),
      new KeyValue(row1, fam2, col2, now - 50, data),
      new KeyValue(row1, fam2, col3, now - 5000, data),
      new KeyValue(row1, fam2, col4, now - 500, data),
      new KeyValue(row1, fam2, col5, now - 10000, data),
      new KeyValue(row2, fam1, col1, now - 10, data) };
    KeyValue k = kvs[0];
    qm.setToNewRow(k);

    List<ScanQueryMatcher.MatchCode> actual = new ArrayList<>(kvs.length);
    for (KeyValue kv : kvs) {
      actual.add(qm.match(kv));
    }

    assertEquals(expected.length, actual.size());
    for (int i = 0; i < expected.length; i++) {
      LOG.debug("expected " + expected[i] + ", actual " + actual.get(i));
      assertEquals(expected[i], actual.get(i));
    }
  }

  private static class AlwaysIncludeAndSeekNextRowFilter extends FilterBase {
    @Override
    public ReturnCode filterCell(final Cell c) {
      return ReturnCode.INCLUDE_AND_SEEK_NEXT_ROW;
    }
  }

  @Test
  public void testMatchWhenFilterReturnsIncludeAndSeekNextRow() throws IOException {
    List<MatchCode> expected = new ArrayList<>();
    expected.add(ScanQueryMatcher.MatchCode.INCLUDE_AND_SEEK_NEXT_ROW);
    expected.add(ScanQueryMatcher.MatchCode.DONE);

    Scan scanWithFilter = new Scan(scan).setFilter(new AlwaysIncludeAndSeekNextRowFilter());

    long now = EnvironmentEdgeManager.currentTime();

    // scan with column 2,4,5
    UserScanQueryMatcher qm = UserScanQueryMatcher.create(
      scanWithFilter, new ScanInfo(this.conf, fam2, 0, 1, ttl, KeepDeletedCells.FALSE,
        HConstants.DEFAULT_BLOCKSIZE, 0, rowComparator, false),
      get.getFamilyMap().get(fam2), now - ttl, now, null);

    List<KeyValue> memstore = new ArrayList<>();
    // ColumnTracker will return INCLUDE_AND_SEEK_NEXT_COL , and filter will return
    // INCLUDE_AND_SEEK_NEXT_ROW, so final match code will be INCLUDE_AND_SEEK_NEXT_ROW.
    memstore.add(new KeyValue(row1, fam2, col2, 1, data));
    memstore.add(new KeyValue(row2, fam1, col1, data));

    List<ScanQueryMatcher.MatchCode> actual = new ArrayList<>(memstore.size());
    KeyValue k = memstore.get(0);
    qm.setToNewRow(k);

    for (KeyValue kv : memstore) {
      actual.add(qm.match(kv));
    }

    assertEquals(expected.size(), actual.size());
    for (int i = 0; i < expected.size(); i++) {
      LOG.debug("expected " + expected.get(i) + ", actual " + actual.get(i));
      assertEquals(expected.get(i), actual.get(i));
    }
  }

  private static class AlwaysIncludeFilter extends FilterBase {
    @Override
    public ReturnCode filterCell(final Cell c) {
      return ReturnCode.INCLUDE;
    }
  }

  /**
   * A filter whose {@link #getSkipHint(Cell)} always returns a fixed {@link ExtendedCell} target,
   * simulating a range-driven filter (e.g. {@code MultiRowRangeFilter}) that knows the next
   * interesting position without inspecting the skipped cell. The filter's
   * {@link #filterCell(Cell)} is intentionally left as the default include-all so that it does not
   * interfere with tests that need cells to reach that method.
   */
  private static class FixedSkipHintFilter extends FilterBase {
    final ExtendedCell hint;

    FixedSkipHintFilter(ExtendedCell hint) {
      this.hint = hint;
    }

    @Override
    public Cell getSkipHint(Cell skippedCell) throws IOException {
      return hint;
    }
  }

  /**
   * Here is the unit test for UserScanQueryMatcher#mergeFilterResponse, when the number of cells
   * exceed the versions requested in scan, we should return SEEK_NEXT_COL, but if current match
   * code is INCLUDE_AND_SEEK_NEXT_ROW, we can optimize to choose the max step between SEEK_NEXT_COL
   * and INCLUDE_AND_SEEK_NEXT_ROW, which is SEEK_NEXT_ROW. <br/>
   */
  @Test
  public void testMergeFilterResponseCase1() throws IOException {
    List<MatchCode> expected = new ArrayList<>();
    expected.add(MatchCode.INCLUDE);
    expected.add(MatchCode.INCLUDE);
    expected.add(MatchCode.SEEK_NEXT_ROW);

    Scan scanWithFilter = new Scan(scan).setFilter(new AlwaysIncludeFilter()).readVersions(2);

    long now = EnvironmentEdgeManager.currentTime();
    // scan with column 2,4,5, the family with maxVersion = 3
    UserScanQueryMatcher qm = UserScanQueryMatcher.create(
      scanWithFilter, new ScanInfo(this.conf, fam2, 0, 3, ttl, KeepDeletedCells.FALSE,
        HConstants.DEFAULT_BLOCKSIZE, 0, rowComparator, false),
      get.getFamilyMap().get(fam2), now - ttl, now, null);

    List<KeyValue> memstore = new ArrayList<>();
    memstore.add(new KeyValue(row1, fam1, col5, 1, data)); // match code will be INCLUDE
    memstore.add(new KeyValue(row1, fam1, col5, 2, data)); // match code will be INCLUDE

    // match code will be SEEK_NEXT_ROW , which is max(INCLUDE_AND_SEEK_NEXT_ROW, SEEK_NEXT_COL).
    memstore.add(new KeyValue(row1, fam1, col5, 3, data));

    KeyValue k = memstore.get(0);
    qm.setToNewRow(k);

    for (int i = 0; i < memstore.size(); i++) {
      assertEquals(expected.get(i), qm.match(memstore.get(i)));
    }

    scanWithFilter = new Scan(scan).setFilter(new AlwaysIncludeFilter()).readVersions(1);
    qm = UserScanQueryMatcher.create(
      scanWithFilter, new ScanInfo(this.conf, fam2, 0, 2, ttl, KeepDeletedCells.FALSE,
        HConstants.DEFAULT_BLOCKSIZE, 0, rowComparator, false),
      get.getFamilyMap().get(fam2), now - ttl, now, null);

    List<KeyValue> memstore2 = new ArrayList<>();
    memstore2.add(new KeyValue(row2, fam1, col2, 1, data)); // match code will be INCLUDE
    // match code will be SEEK_NEXT_COL, which is max(INCLUDE_AND_SEEK_NEXT_COL, SEEK_NEXT_COL).
    memstore2.add(new KeyValue(row2, fam1, col2, 2, data));

    k = memstore2.get(0);
    qm.setToNewRow(k);

    assertEquals(MatchCode.INCLUDE, qm.match(memstore2.get(0)));
    assertEquals(MatchCode.SEEK_NEXT_COL, qm.match(memstore2.get(1)));
  }

  /**
   * Here is the unit test for UserScanQueryMatcher#mergeFilterResponse: the match code may be
   * changed to SEEK_NEXT_COL or INCLUDE_AND_SEEK_NEXT_COL after merging with filterResponse, even
   * if the passed match code is neither SEEK_NEXT_COL nor INCLUDE_AND_SEEK_NEXT_COL. In that case,
   * we need to make sure that the ColumnTracker has been switched to the next column. <br/>
   * An effective test way is: we only need to check the cell from getKeyForNextColumn(). because
   * that as long as the UserScanQueryMatcher returns SEEK_NEXT_COL or INCLUDE_AND_SEEK_NEXT_COL,
   * UserScanQueryMatcher#getKeyForNextColumn should return an cell whose column is larger than the
   * current cell's.
   */
  @Test
  public void testMergeFilterResponseCase2() throws Exception {
    List<MatchCode> expected = new ArrayList<>();
    expected.add(ScanQueryMatcher.MatchCode.INCLUDE);
    expected.add(ScanQueryMatcher.MatchCode.INCLUDE);
    expected.add(ScanQueryMatcher.MatchCode.INCLUDE);
    expected.add(ScanQueryMatcher.MatchCode.SEEK_NEXT_COL);

    Scan scanWithFilter = new Scan(scan).setFilter(new AlwaysIncludeFilter()).readVersions(3);

    long now = EnvironmentEdgeManager.currentTime();

    // scan with column 2,4,5, the family with maxVersion = 5
    UserScanQueryMatcher qm = UserScanQueryMatcher.create(
      scanWithFilter, new ScanInfo(this.conf, fam2, 0, 5, ttl, KeepDeletedCells.FALSE,
        HConstants.DEFAULT_BLOCKSIZE, 0, rowComparator, false),
      get.getFamilyMap().get(fam2), now - ttl, now, null);

    List<KeyValue> memstore = new ArrayList<>();

    memstore.add(new KeyValue(row1, fam1, col2, 1, data)); // match code will be INCLUDE
    memstore.add(new KeyValue(row1, fam1, col2, 2, data)); // match code will be INCLUDE
    memstore.add(new KeyValue(row1, fam1, col2, 3, data)); // match code will be INCLUDE
    memstore.add(new KeyValue(row1, fam1, col2, 4, data)); // match code will be SEEK_NEXT_COL

    KeyValue k = memstore.get(0);
    qm.setToNewRow(k);

    for (int i = 0; i < memstore.size(); i++) {
      assertEquals(expected.get(i), qm.match(memstore.get(i)));
    }

    // For last cell, the query matcher will return SEEK_NEXT_COL, and the
    // ColumnTracker will skip to the next column, which is col4.
    ExtendedCell lastCell = memstore.get(memstore.size() - 1);
    Cell nextCell = qm.getKeyForNextColumn(lastCell);
    assertArrayEquals(nextCell.getQualifierArray(), col4);
  }

  // -----------------------------------------------------------------------
  // Tests for HBASE-29974: Filter#getSkipHint consulted at matchColumn
  // structural short-circuits (time-range, column, and version gates).
  // -----------------------------------------------------------------------

  /**
   * HBASE-29974 Path 2 — time-range upper-bound gate (tsCmp &gt; 0).
   * <p>
   * When a cell's timestamp is newer than the scan's time-range maximum ({@code ts >= maxTs}), the
   * original code returns {@link MatchCode#SKIP} without ever calling {@code filterCell}. With the
   * fix, the matcher must first ask the filter for a skip hint. If the filter returns a non-null
   * hint, {@link MatchCode#SEEK_NEXT_USING_HINT} must be returned and
   * {@link UserScanQueryMatcher#getNextKeyHint} must return that hint.
   */
  @Test
  public void testSkipHintConsultedForCellNewerThanTimeRange() throws IOException {
    long now = EnvironmentEdgeManager.currentTime();
    long minTs = now - 2000;
    long maxTs = now - 1000;

    // The hint points to a cell in the middle of the valid time range so the scanner can resume.
    KeyValue hintCell = new KeyValue(row2, fam2, col1, now - 1500, data);
    Scan timeRangeScan = new Scan().addFamily(fam2).setTimeRange(minTs, maxTs)
      .setFilter(new FixedSkipHintFilter(hintCell));

    long now2 = EnvironmentEdgeManager.currentTime();
    UserScanQueryMatcher qm = UserScanQueryMatcher.create(timeRangeScan,
      new ScanInfo(this.conf, fam2, 0, 1, ttl, KeepDeletedCells.FALSE, HConstants.DEFAULT_BLOCKSIZE,
        0, rowComparator, false),
      null, 0, now2, null);

    // ts=now2 >= maxTs, so tsCmp > 0: time-range gate fires before filterCell (Path 2).
    KeyValue tooNew = new KeyValue(row1, fam2, col1, now2, data);
    qm.setToNewRow(tooNew);

    assertEquals("Filter hint must promote SKIP to SEEK_NEXT_USING_HINT",
      MatchCode.SEEK_NEXT_USING_HINT, qm.match(tooNew));
    assertEquals("getNextKeyHint must return the pending skip hint", hintCell,
      qm.getNextKeyHint(tooNew));
    // pendingSkipHint is consumed on the first getNextKeyHint call; subsequent calls fall through
    // to FilterBase#getNextCellHint which returns null.
    assertNull("pendingSkipHint must be cleared after being drained", qm.getNextKeyHint(tooNew));
  }

  /**
   * HBASE-29974 Path 2 — time-range lower-bound gate (tsCmp &lt; 0).
   * <p>
   * When a cell's timestamp is older than the scan's time-range minimum ({@code ts < minTs}), the
   * original code returns the column-tracker's next-row-or-next-column suggestion without
   * consulting the filter. With the fix, the matcher must first ask the filter for a skip hint, and
   * prefer it over the column-tracker's answer when non-null.
   */
  @Test
  public void testSkipHintConsultedForCellOlderThanTimeRange() throws IOException {
    long now = EnvironmentEdgeManager.currentTime();
    long minTs = now - 500;
    long maxTs = now;

    KeyValue hintCell = new KeyValue(row2, fam2, col1, now - 100, data);
    Scan timeRangeScan = new Scan().addFamily(fam2).setTimeRange(minTs, maxTs)
      .setFilter(new FixedSkipHintFilter(hintCell));

    UserScanQueryMatcher qm = UserScanQueryMatcher.create(timeRangeScan,
      new ScanInfo(this.conf, fam2, 0, 1, ttl, KeepDeletedCells.FALSE, HConstants.DEFAULT_BLOCKSIZE,
        0, rowComparator, false),
      null, 0, now, null);

    // ts = now-1000 < minTs (now-500), so tsCmp < 0: time-range gate fires (Path 2).
    KeyValue tooOld = new KeyValue(row1, fam2, col1, now - 1000, data);
    qm.setToNewRow(tooOld);

    assertEquals("Filter hint must promote column-tracker result to SEEK_NEXT_USING_HINT",
      MatchCode.SEEK_NEXT_USING_HINT, qm.match(tooOld));
    assertEquals("getNextKeyHint must return the pending skip hint", hintCell,
      qm.getNextKeyHint(tooOld));
    assertNull("pendingSkipHint must be cleared after being drained", qm.getNextKeyHint(tooOld));
  }

  /**
   * HBASE-29974 Path 2 — time-range gate, null hint falls through.
   * <p>
   * When the filter's {@link org.apache.hadoop.hbase.filter.Filter#getSkipHint(Cell)} returns
   * {@code null}, the matcher must fall back to the original structural skip code (SKIP or the
   * column-tracker result) with no regression.
   */
  @Test
  public void testNullSkipHintFallsThroughToOriginalCodeOnTimeRangeGate() throws IOException {
    long now = EnvironmentEdgeManager.currentTime();
    long minTs = now - 2000;
    long maxTs = now - 1000;

    // AlwaysIncludeFilter does not override getSkipHint(), so it returns null.
    Scan timeRangeScan =
      new Scan().addFamily(fam2).setTimeRange(minTs, maxTs).setFilter(new AlwaysIncludeFilter());

    long now2 = EnvironmentEdgeManager.currentTime();
    UserScanQueryMatcher qm = UserScanQueryMatcher.create(timeRangeScan,
      new ScanInfo(this.conf, fam2, 0, 1, ttl, KeepDeletedCells.FALSE, HConstants.DEFAULT_BLOCKSIZE,
        0, rowComparator, false),
      null, 0, now2, null);

    KeyValue tooNew = new KeyValue(row1, fam2, col1, now2, data);
    qm.setToNewRow(tooNew);

    // With a null hint the gate must still return the original SKIP code.
    assertEquals("Null getSkipHint must not change the original SKIP match code", MatchCode.SKIP,
      qm.match(tooNew));
  }

  /**
   * HBASE-29974 Path 3 — column-exclusion gate ({@code checkColumn() != INCLUDE}).
   * <p>
   * When an explicit-column scan encounters a qualifier not in its requested set, the column
   * tracker returns a skip/seek code before {@code filterCell} is ever reached. With the fix, the
   * matcher must consult {@code getSkipHint} first, and return
   * {@link MatchCode#SEEK_NEXT_USING_HINT} when a non-null hint is available.
   */
  @Test
  public void testSkipHintConsultedForExcludedColumn() throws IOException {
    long now = EnvironmentEdgeManager.currentTime();

    // get.getFamilyMap().get(fam2) contains col2, col4, col5; presenting col1 triggers exclusion.
    KeyValue hintCell = new KeyValue(row1, fam2, col2, 1, data);
    Scan scanWithFilter = new Scan(scan).setFilter(new FixedSkipHintFilter(hintCell));

    UserScanQueryMatcher qm = UserScanQueryMatcher.create(
      scanWithFilter, new ScanInfo(this.conf, fam2, 0, 1, ttl, KeepDeletedCells.FALSE,
        HConstants.DEFAULT_BLOCKSIZE, 0, rowComparator, false),
      get.getFamilyMap().get(fam2), now - ttl, now, null);

    // col1 is not in {col2, col4, col5}: checkColumn returns a non-INCLUDE code.
    KeyValue excludedCol = new KeyValue(row1, fam2, col1, 1, data);
    qm.setToNewRow(excludedCol);

    assertEquals("Filter hint must promote column-exclusion result to SEEK_NEXT_USING_HINT",
      MatchCode.SEEK_NEXT_USING_HINT, qm.match(excludedCol));
    assertEquals("getNextKeyHint must return the pending skip hint", hintCell,
      qm.getNextKeyHint(excludedCol));
    assertNull("pendingSkipHint must be cleared after being drained",
      qm.getNextKeyHint(excludedCol));
  }

  /**
   * HBASE-29974 Path 3 — version-exhaustion gate ({@code checkVersions()} returns
   * {@link MatchCode#SEEK_NEXT_COL}).
   * <p>
   * When the column tracker has already seen the maximum number of versions for a qualifier, it
   * returns SKIP or SEEK_NEXT_COL for subsequent versions, again bypassing {@code filterCell}. With
   * the fix, the matcher must consult {@code getSkipHint} at that point.
   */
  @Test
  public void testSkipHintConsultedOnVersionExhaustion() throws IOException {
    long now = EnvironmentEdgeManager.currentTime();

    KeyValue hintCell = new KeyValue(row2, fam2, col1, now, data);
    // ScanInfo maxVersions=1: column tracker allows exactly 1 version per qualifier.
    Scan scanWithFilter = new Scan().addFamily(fam2).setFilter(new FixedSkipHintFilter(hintCell));

    UserScanQueryMatcher qm = UserScanQueryMatcher.create(scanWithFilter,
      new ScanInfo(this.conf, fam2, 0, 1, ttl, KeepDeletedCells.FALSE, HConstants.DEFAULT_BLOCKSIZE,
        0, rowComparator, false),
      null, now - ttl, now, null);

    // First version of col1: passes all gates and reaches filterCell.
    KeyValue version1 = new KeyValue(row1, fam2, col1, now - 10, data);
    qm.setToNewRow(version1);
    MatchCode firstCode = qm.match(version1);
    assertEquals("First version must be included", MatchCode.INCLUDE, firstCode);

    // Second version of col1: checkVersions returns SEEK_NEXT_COL (version limit exceeded).
    // The filter hint must be consulted and SEEK_NEXT_USING_HINT returned.
    KeyValue version2 = new KeyValue(row1, fam2, col1, now - 20, data);
    assertEquals("Filter hint must promote version-exhaustion result to SEEK_NEXT_USING_HINT",
      MatchCode.SEEK_NEXT_USING_HINT, qm.match(version2));
    assertEquals("getNextKeyHint must return the pending skip hint from version gate", hintCell,
      qm.getNextKeyHint(version2));
    assertNull("pendingSkipHint must be cleared after being drained", qm.getNextKeyHint(version2));
  }

  /**
   * HBASE-29974 — {@code pendingSkipHint} must not interfere with the normal
   * {@code filterCell → SEEK_NEXT_USING_HINT → getNextCellHint} path.
   * <p>
   * If a cell passes all structural gates and reaches {@code filterCell}, which then returns
   * {@link ReturnCode#SEEK_NEXT_USING_HINT}, the existing {@link Filter#getNextCellHint(Cell)}
   * mechanism must still be used (not {@code getSkipHint}), and {@code pendingSkipHint} must remain
   * null so it does not contaminate the result.
   */
  @Test
  public void testNormalFilterCellHintPathUnaffectedBySkipHintChange() throws IOException {
    long now = EnvironmentEdgeManager.currentTime();

    final KeyValue filterHintCell = new KeyValue(row2, fam2, col1, now, data);

    // A filter that returns SEEK_NEXT_USING_HINT from filterCell and provides a hint through
    // getNextCellHint, while NOT overriding getSkipHint (returns null by default).
    FilterBase seekFilter = new FilterBase() {
      @Override
      public ReturnCode filterCell(Cell c) {
        return ReturnCode.SEEK_NEXT_USING_HINT;
      }

      @Override
      public Cell getNextCellHint(Cell currentCell) {
        return filterHintCell;
      }
    };

    Scan scanWithFilter = new Scan().addFamily(fam2).setFilter(seekFilter);

    UserScanQueryMatcher qm = UserScanQueryMatcher.create(scanWithFilter,
      new ScanInfo(this.conf, fam2, 0, 1, ttl, KeepDeletedCells.FALSE, HConstants.DEFAULT_BLOCKSIZE,
        0, rowComparator, false),
      null, now - ttl, now, null);

    KeyValue cell = new KeyValue(row1, fam2, col1, now - 10, data);
    qm.setToNewRow(cell);

    assertEquals("filterCell path must still return SEEK_NEXT_USING_HINT",
      MatchCode.SEEK_NEXT_USING_HINT, qm.match(cell));
    // pendingSkipHint was never set (getSkipHint returned null), so getNextKeyHint must
    // delegate to filter.getNextCellHint().
    assertEquals("getNextKeyHint must delegate to getNextCellHint when no skip hint is pending",
      filterHintCell, qm.getNextKeyHint(cell));
  }

  /**
   * After enough consecutive range delete markers, the matcher should switch from SKIP to
   * SEEK_NEXT_COL. Point deletes and KEEP_DELETED_CELLS always SKIP.
   */
  @Test
  public void testSeekOnRangeDelete() throws IOException {
    int n = NormalUserScanQueryMatcher.SEEK_ON_DELETE_MARKER_THRESHOLD;

    // DeleteColumn: first N-1 SKIP, N-th triggers SEEK_NEXT_COL
    assertSeekAfterThreshold(KeepDeletedCells.FALSE, Type.DeleteColumn, n);

    // DeleteFamily: same threshold behavior
    assertSeekAfterThreshold(KeepDeletedCells.FALSE, Type.DeleteFamily, n);

    // Delete (version): always SKIP (point delete, not range)
    assertAllSkip(KeepDeletedCells.FALSE, Type.Delete, n + 1);

    // KEEP_DELETED_CELLS=TRUE: always SKIP
    assertAllSkip(KeepDeletedCells.TRUE, Type.DeleteColumn, n + 1);
  }

  /**
   * DeleteColumn with empty qualifier must not cause seeking past a subsequent DeleteFamily.
   * DeleteFamily masks all columns, so it must be tracked by the delete tracker.
   */
  @Test
  public void testDeleteColumnEmptyQualifierDoesNotSkipDeleteFamily() throws IOException {
    long now = EnvironmentEdgeManager.currentTime();
    byte[] e = HConstants.EMPTY_BYTE_ARRAY;
    UserScanQueryMatcher qm = UserScanQueryMatcher.create(scan, new ScanInfo(this.conf, fam1, 0, 1,
      ttl, KeepDeletedCells.FALSE, HConstants.DEFAULT_BLOCKSIZE, 0, rowComparator, false), null,
      now - ttl, now, null);

    int n = NormalUserScanQueryMatcher.SEEK_ON_DELETE_MARKER_THRESHOLD;
    // Feed DCs with empty qualifier past the threshold, then a DF.
    // The DF must NOT be seeked past -- it must be SKIP'd so the tracker picks it up.
    qm.setToNewRow(new KeyValue(row1, fam1, e, now, Type.DeleteColumn));
    for (int i = 0; i < n + 1; i++) {
      // Empty qualifier DCs should never trigger seek, regardless of threshold
      assertEquals("DC at i=" + i, MatchCode.SKIP,
        qm.match(new KeyValue(row1, fam1, e, now - i, Type.DeleteColumn)));
    }
    KeyValue df = new KeyValue(row1, fam1, e, now - n - 1, Type.DeleteFamily);
    KeyValue put = new KeyValue(row1, fam1, col1, now - n - 1, Type.Put, data);
    // DF must be processed (SKIP), not seeked past
    assertEquals(MatchCode.SKIP, qm.match(df));
    // Put in col1 at t=now-3 should be masked by DF@t=now-3
    MatchCode putCode = qm.match(put);
    assertEquals(MatchCode.SEEK_NEXT_COL, putCode);
  }

  /**
   * DeleteColumn markers for different qualifiers should not accumulate the seek counter. Only
   * consecutive markers for the same qualifier should trigger seeking.
   */
  @Test
  public void testDeleteColumnDifferentQualifiersDoNotSeek() throws IOException {
    long now = EnvironmentEdgeManager.currentTime();
    UserScanQueryMatcher qm = UserScanQueryMatcher.create(scan, new ScanInfo(this.conf, fam1, 0, 1,
      ttl, KeepDeletedCells.FALSE, HConstants.DEFAULT_BLOCKSIZE, 0, rowComparator, false), null,
      now - ttl, now, null);

    // DCs for different qualifiers: counter resets on qualifier change, never seeks
    qm.setToNewRow(new KeyValue(row1, fam1, col1, now, Type.DeleteColumn));
    assertEquals(MatchCode.SKIP, qm.match(new KeyValue(row1, fam1, col1, now, Type.DeleteColumn)));
    assertEquals(MatchCode.SKIP,
      qm.match(new KeyValue(row1, fam1, col2, now - 1, Type.DeleteColumn)));
    assertEquals(MatchCode.SKIP,
      qm.match(new KeyValue(row1, fam1, col3, now - 2, Type.DeleteColumn)));
    assertEquals(MatchCode.SKIP,
      qm.match(new KeyValue(row1, fam1, col4, now - 3, Type.DeleteColumn)));
    assertEquals(MatchCode.SKIP,
      qm.match(new KeyValue(row1, fam1, col5, now - 4, Type.DeleteColumn)));
  }

  /**
   * Delete markers outside the scan's time range (includeDeleteMarker=false) should still
   * accumulate the seek counter and trigger SEEK_NEXT_COL after the threshold.
   */
  @Test
  public void testSeekOnRangeDeleteOutsideTimeRange() throws IOException {
    long now = EnvironmentEdgeManager.currentTime();
    long futureTs = now + 1_000_000;
    Scan scanWithTimeRange = new Scan(scan).setTimeRange(futureTs, Long.MAX_VALUE);

    UserScanQueryMatcher qm = UserScanQueryMatcher.create(scanWithTimeRange,
      new ScanInfo(this.conf, fam1, 0, 1, ttl, KeepDeletedCells.FALSE, HConstants.DEFAULT_BLOCKSIZE,
        0, rowComparator, false),
      null, now - ttl, now, null);

    int n = NormalUserScanQueryMatcher.SEEK_ON_DELETE_MARKER_THRESHOLD;
    qm.setToNewRow(new KeyValue(row1, fam1, col1, now, Type.DeleteColumn));
    // All DCs have timestamps below the time range, so includeDeleteMarker is false.
    // The seek counter should still accumulate.
    for (int i = 0; i < n - 1; i++) {
      assertEquals("DC at i=" + i, MatchCode.SKIP,
        qm.match(new KeyValue(row1, fam1, col1, now - i, Type.DeleteColumn)));
    }
    assertEquals(MatchCode.SEEK_NEXT_COL,
      qm.match(new KeyValue(row1, fam1, col1, now - n + 1, Type.DeleteColumn)));
  }

  private UserScanQueryMatcher createDeleteMatcher(KeepDeletedCells keepDeletedCells)
    throws IOException {
    long now = EnvironmentEdgeManager.currentTime();
    return UserScanQueryMatcher.create(scan, new ScanInfo(this.conf, fam1, 0, 1, ttl,
      keepDeletedCells, HConstants.DEFAULT_BLOCKSIZE, 0, rowComparator, false), null, now - ttl,
      now, null);
  }

  /** First n-1 markers SKIP, n-th triggers SEEK_NEXT_COL. */
  private void assertSeekAfterThreshold(KeepDeletedCells keepDeletedCells, Type type, int n)
    throws IOException {
    long now = EnvironmentEdgeManager.currentTime();
    UserScanQueryMatcher qm = createDeleteMatcher(keepDeletedCells);
    boolean familyLevel = type == Type.DeleteFamily || type == Type.DeleteFamilyVersion;
    byte[] qual = familyLevel ? HConstants.EMPTY_BYTE_ARRAY : col1;
    qm.setToNewRow(new KeyValue(row1, fam1, qual, now, type));
    for (int i = 0; i < n - 1; i++) {
      assertEquals("Mismatch at index " + i, MatchCode.SKIP,
        qm.match(new KeyValue(row1, fam1, qual, now - i, type)));
    }
    assertEquals("Expected SEEK_NEXT_COL at index " + (n - 1), MatchCode.SEEK_NEXT_COL,
      qm.match(new KeyValue(row1, fam1, qual, now - n + 1, type)));
  }

  /** All markers should SKIP regardless of count. */
  private void assertAllSkip(KeepDeletedCells keepDeletedCells, Type type, int count)
    throws IOException {
    long now = EnvironmentEdgeManager.currentTime();
    UserScanQueryMatcher qm = createDeleteMatcher(keepDeletedCells);
    boolean familyLevel = type == Type.DeleteFamily || type == Type.DeleteFamilyVersion;
    byte[] qual = familyLevel ? HConstants.EMPTY_BYTE_ARRAY : col1;
    qm.setToNewRow(new KeyValue(row1, fam1, qual, now, type));
    for (int i = 0; i < count; i++) {
      assertEquals("Mismatch at index " + i, MatchCode.SKIP,
        qm.match(new KeyValue(row1, fam1, qual, now - i, type)));
    }
  }
}
