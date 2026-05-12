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
package org.apache.hadoop.hbase.filter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.testclassification.FilterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@code getHintForRejectedRow} and {@code getSkipHint} delegation in composite
 * filters ({@link FilterList}, {@link SkipFilter}, {@link WhileMatchFilter}).
 */
@Tag(FilterTests.TAG)
@Tag(SmallTests.TAG)
public class TestFilterListHintDelegation {

  private static final byte[] ROW_A = Bytes.toBytes("rowA");
  private static final byte[] ROW_B = Bytes.toBytes("rowB");
  private static final byte[] ROW_C = Bytes.toBytes("rowC");
  private static final byte[] FAMILY = Bytes.toBytes("f");
  private static final byte[] QUALIFIER = Bytes.toBytes("q");

  private static KeyValue kv(byte[] row) {
    return new KeyValue(row, FAMILY, QUALIFIER, 1L, KeyValue.Type.Put, Bytes.toBytes("v"));
  }

  /** Filter that returns a fixed hint from {@code getHintForRejectedRow}. */
  private static FilterBase fixedRejectedRowHintFilter(Cell hint) {
    return new FilterBase() {
      @Override
      public Cell getHintForRejectedRow(Cell firstRowCell) {
        return hint;
      }
    };
  }

  /** Filter that rejects every row via {@code filterRowKey} and returns a fixed hint. */
  private static FilterBase rejectingRowHintFilter(Cell hint) {
    return new FilterBase() {
      @Override
      public boolean filterRowKey(Cell cell) {
        return true;
      }

      @Override
      public Cell getHintForRejectedRow(Cell firstRowCell) {
        return hint;
      }
    };
  }

  /** Filter that returns a fixed hint from {@code getSkipHint}. */
  private static FilterBase fixedSkipHintFilter(Cell hint) {
    return new FilterBase() {
      @Override
      public Cell getSkipHint(Cell skippedCell) {
        return hint;
      }
    };
  }

  /** Filter that claims {@code filterAllRemaining() == true}. */
  private static FilterBase terminatedFilter(Cell hint) {
    return new FilterBase() {
      @Override
      public boolean filterAllRemaining() {
        return true;
      }

      @Override
      public Cell getHintForRejectedRow(Cell firstRowCell) {
        return hint;
      }

      @Override
      public Cell getSkipHint(Cell skippedCell) {
        return hint;
      }
    };
  }

  // ---- AND (MUST_PASS_ALL) getHintForRejectedRow ----

  @Test
  public void testANDGetHintForRejectedRow_takesMax() throws IOException {
    Cell hintA = kv(ROW_A);
    Cell hintC = kv(ROW_C);
    FilterList fl = new FilterList(Operator.MUST_PASS_ALL,
      Arrays.asList(rejectingRowHintFilter(hintA), rejectingRowHintFilter(hintC)));
    fl.filterRowKey(kv(ROW_A));

    Cell result = fl.getHintForRejectedRow(kv(ROW_A));
    assertNotNull(result);
    assertEquals(0, CellComparator.getInstance().compare(hintC, result),
      "AND must return the farthest (max) hint");
  }

  @Test
  public void testANDGetHintForRejectedRow_ignoresNull() throws IOException {
    Cell hintB = kv(ROW_B);
    FilterList fl = new FilterList(Operator.MUST_PASS_ALL,
      Arrays.asList(rejectingRowHintFilter(null), rejectingRowHintFilter(hintB)));
    fl.filterRowKey(kv(ROW_A));

    Cell result = fl.getHintForRejectedRow(kv(ROW_A));
    assertNotNull(result);
    assertEquals(0, CellComparator.getInstance().compare(hintB, result),
      "AND must ignore null hints and return the non-null one");
  }

  @Test
  public void testANDGetHintForRejectedRow_allNull() throws IOException {
    FilterList fl = new FilterList(Operator.MUST_PASS_ALL,
      Arrays.asList(rejectingRowHintFilter(null), rejectingRowHintFilter(null)));
    fl.filterRowKey(kv(ROW_A));

    assertNull(fl.getHintForRejectedRow(kv(ROW_A)), "AND with all-null hints must return null");
  }

  @Test
  public void testANDGetHintForRejectedRow_reversed() throws IOException {
    Cell hintA = kv(ROW_A);
    Cell hintC = kv(ROW_C);
    FilterList fl = new FilterList(Operator.MUST_PASS_ALL,
      Arrays.asList(rejectingRowHintFilter(hintA), rejectingRowHintFilter(hintC)));
    fl.setReversed(true);
    fl.filterRowKey(kv(ROW_C));

    Cell result = fl.getHintForRejectedRow(kv(ROW_C));
    assertNotNull(result);
    assertEquals(0, CellComparator.getInstance().compare(hintA, result),
      "Reversed AND must return the smaller row key (farthest in reverse direction)");
  }

  // ---- AND (MUST_PASS_ALL) getSkipHint ----

  @Test
  public void testANDGetSkipHint_takesMax() throws IOException {
    Cell hintA = kv(ROW_A);
    Cell hintC = kv(ROW_C);
    FilterList fl = new FilterList(Operator.MUST_PASS_ALL,
      Arrays.asList(fixedSkipHintFilter(hintA), fixedSkipHintFilter(hintC)));

    Cell result = fl.getSkipHint(kv(ROW_A));
    assertNotNull(result);
    assertEquals(0, CellComparator.getInstance().compare(hintC, result),
      "AND getSkipHint must return the farthest (max) hint");
  }

  @Test
  public void testANDGetSkipHint_ignoresNull() throws IOException {
    Cell hintB = kv(ROW_B);
    FilterList fl = new FilterList(Operator.MUST_PASS_ALL,
      Arrays.asList(fixedSkipHintFilter(null), fixedSkipHintFilter(hintB)));

    Cell result = fl.getSkipHint(kv(ROW_A));
    assertNotNull(result);
    assertEquals(0, CellComparator.getInstance().compare(hintB, result),
      "AND getSkipHint must ignore null and return the non-null hint");
  }

  @Test
  public void testANDGetSkipHint_allNull() throws IOException {
    FilterList fl = new FilterList(Operator.MUST_PASS_ALL,
      Arrays.asList(fixedSkipHintFilter(null), fixedSkipHintFilter(null)));

    assertNull(fl.getSkipHint(kv(ROW_A)), "AND with all-null skip hints must return null");
  }

  @Test
  public void testANDGetSkipHint_reversed() throws IOException {
    Cell hintA = kv(ROW_A);
    Cell hintC = kv(ROW_C);
    FilterList fl = new FilterList(Operator.MUST_PASS_ALL,
      Arrays.asList(fixedSkipHintFilter(hintA), fixedSkipHintFilter(hintC)));
    fl.setReversed(true);

    Cell result = fl.getSkipHint(kv(ROW_C));
    assertNotNull(result);
    assertEquals(0, CellComparator.getInstance().compare(hintA, result),
      "Reversed AND getSkipHint must return the smaller row key (farthest in reverse direction)");
  }

  // ---- OR (MUST_PASS_ONE) getHintForRejectedRow ----

  @Test
  public void testORGetHintForRejectedRow_takesMin() throws IOException {
    Cell hintA = kv(ROW_A);
    Cell hintC = kv(ROW_C);
    FilterList fl = new FilterList(Operator.MUST_PASS_ONE,
      Arrays.asList(fixedRejectedRowHintFilter(hintA), fixedRejectedRowHintFilter(hintC)));

    Cell result = fl.getHintForRejectedRow(kv(ROW_A));
    assertNotNull(result);
    assertEquals(0, CellComparator.getInstance().compare(hintA, result),
      "OR must return the nearest (min) hint");
  }

  @Test
  public void testORGetHintForRejectedRow_nullReturnsNull() throws IOException {
    Cell hintC = kv(ROW_C);
    FilterList fl = new FilterList(Operator.MUST_PASS_ONE,
      Arrays.asList(fixedRejectedRowHintFilter(null), fixedRejectedRowHintFilter(hintC)));

    assertNull(fl.getHintForRejectedRow(kv(ROW_A)),
      "OR must return null if any sub-filter returns null (can't safely skip)");
  }

  @Test
  public void testORGetHintForRejectedRow_allHints() throws IOException {
    Cell hintA = kv(ROW_A);
    Cell hintB = kv(ROW_B);
    Cell hintC = kv(ROW_C);
    FilterList fl =
      new FilterList(Operator.MUST_PASS_ONE, Arrays.asList(fixedRejectedRowHintFilter(hintB),
        fixedRejectedRowHintFilter(hintA), fixedRejectedRowHintFilter(hintC)));

    Cell result = fl.getHintForRejectedRow(kv(ROW_A));
    assertNotNull(result);
    assertEquals(0, CellComparator.getInstance().compare(hintA, result),
      "OR with all hints must return the minimum");
  }

  @Test
  public void testORGetHintForRejectedRow_reversed() throws IOException {
    // In reversed scan, "min" in scan direction means the larger row key.
    Cell hintA = kv(ROW_A);
    Cell hintC = kv(ROW_C);
    FilterList fl = new FilterList(Operator.MUST_PASS_ONE,
      Arrays.asList(fixedRejectedRowHintFilter(hintA), fixedRejectedRowHintFilter(hintC)));
    fl.setReversed(true);

    Cell result = fl.getHintForRejectedRow(kv(ROW_C));
    assertNotNull(result);
    assertEquals(0, CellComparator.getInstance().compare(hintC, result),
      "Reversed OR must return the larger row key (nearest in reverse direction)");
  }

  // ---- OR (MUST_PASS_ONE) getSkipHint ----

  @Test
  public void testORGetSkipHint_takesMin() throws IOException {
    Cell hintA = kv(ROW_A);
    Cell hintC = kv(ROW_C);
    FilterList fl = new FilterList(Operator.MUST_PASS_ONE,
      Arrays.asList(fixedSkipHintFilter(hintA), fixedSkipHintFilter(hintC)));

    Cell result = fl.getSkipHint(kv(ROW_A));
    assertNotNull(result);
    assertEquals(0, CellComparator.getInstance().compare(hintA, result),
      "OR getSkipHint must return the nearest (min) hint");
  }

  @Test
  public void testORGetSkipHint_nullReturnsNull() throws IOException {
    Cell hintC = kv(ROW_C);
    FilterList fl = new FilterList(Operator.MUST_PASS_ONE,
      Arrays.asList(fixedSkipHintFilter(null), fixedSkipHintFilter(hintC)));

    assertNull(fl.getSkipHint(kv(ROW_A)),
      "OR getSkipHint must return null if any sub-filter returns null");
  }

  @Test
  public void testORGetSkipHint_allHints() throws IOException {
    Cell hintA = kv(ROW_A);
    Cell hintB = kv(ROW_B);
    Cell hintC = kv(ROW_C);
    FilterList fl = new FilterList(Operator.MUST_PASS_ONE, Arrays.asList(fixedSkipHintFilter(hintB),
      fixedSkipHintFilter(hintA), fixedSkipHintFilter(hintC)));

    Cell result = fl.getSkipHint(kv(ROW_A));
    assertNotNull(result);
    assertEquals(0, CellComparator.getInstance().compare(hintA, result),
      "OR getSkipHint with all hints must return the minimum");
  }

  @Test
  public void testORGetSkipHint_reversed() throws IOException {
    Cell hintA = kv(ROW_A);
    Cell hintC = kv(ROW_C);
    FilterList fl = new FilterList(Operator.MUST_PASS_ONE,
      Arrays.asList(fixedSkipHintFilter(hintA), fixedSkipHintFilter(hintC)));
    fl.setReversed(true);

    Cell result = fl.getSkipHint(kv(ROW_C));
    assertNotNull(result);
    assertEquals(0, CellComparator.getInstance().compare(hintC, result),
      "Reversed OR getSkipHint must return the larger row key (nearest in reverse direction)");
  }

  // ---- filterAllRemaining for getSkipHint ----

  @Test
  public void testFilterAllRemainingSubFilterSkippedForGetSkipHint() throws IOException {
    Cell terminatedHint = kv(ROW_C);
    Cell activeHint = kv(ROW_B);

    FilterList fl = new FilterList(Operator.MUST_PASS_ALL,
      Arrays.asList(terminatedFilter(terminatedHint), fixedSkipHintFilter(activeHint)));

    Cell result = fl.getSkipHint(kv(ROW_A));
    assertNotNull(result);
    assertEquals(0, CellComparator.getInstance().compare(activeHint, result),
      "Terminated sub-filters must be skipped for getSkipHint too");
  }

  @Test
  public void testORFilterAllRemainingSubFilterSkippedForGetHintForRejectedRow()
    throws IOException {
    Cell terminatedHint = kv(ROW_C);
    Cell activeHint = kv(ROW_B);

    FilterList fl = new FilterList(Operator.MUST_PASS_ONE,
      Arrays.asList(terminatedFilter(terminatedHint), fixedRejectedRowHintFilter(activeHint)));

    Cell result = fl.getHintForRejectedRow(kv(ROW_A));
    assertNotNull(result);
    assertEquals(0, CellComparator.getInstance().compare(activeHint, result),
      "OR must skip terminated sub-filters and return the active filter's hint");
  }

  @Test
  public void testORFilterAllRemainingSubFilterSkippedForGetSkipHint() throws IOException {
    Cell terminatedHint = kv(ROW_C);
    Cell activeHint = kv(ROW_B);

    FilterList fl = new FilterList(Operator.MUST_PASS_ONE,
      Arrays.asList(terminatedFilter(terminatedHint), fixedSkipHintFilter(activeHint)));

    Cell result = fl.getSkipHint(kv(ROW_A));
    assertNotNull(result);
    assertEquals(0, CellComparator.getInstance().compare(activeHint, result),
      "OR must skip terminated sub-filters for getSkipHint too");
  }

  // ---- SkipFilter delegation ----

  @Test
  public void testSkipFilterDelegatesGetHintForRejectedRow() throws IOException {
    Cell hint = kv(ROW_B);
    SkipFilter sf = new SkipFilter(fixedRejectedRowHintFilter(hint));

    Cell result = sf.getHintForRejectedRow(kv(ROW_A));
    assertNotNull(result);
    assertEquals(0, CellComparator.getInstance().compare(hint, result),
      "SkipFilter must delegate getHintForRejectedRow to wrapped filter");
  }

  @Test
  public void testSkipFilterDelegatesGetSkipHint() throws IOException {
    Cell hint = kv(ROW_B);
    SkipFilter sf = new SkipFilter(fixedSkipHintFilter(hint));

    Cell result = sf.getSkipHint(kv(ROW_A));
    assertNotNull(result);
    assertEquals(0, CellComparator.getInstance().compare(hint, result),
      "SkipFilter must delegate getSkipHint to wrapped filter");
  }

  // ---- WhileMatchFilter delegation ----

  @Test
  public void testWhileMatchFilterDelegatesGetHintForRejectedRow() throws IOException {
    Cell hint = kv(ROW_B);
    WhileMatchFilter wmf = new WhileMatchFilter(fixedRejectedRowHintFilter(hint));

    Cell result = wmf.getHintForRejectedRow(kv(ROW_A));
    assertNotNull(result);
    assertEquals(0, CellComparator.getInstance().compare(hint, result),
      "WhileMatchFilter must delegate getHintForRejectedRow to wrapped filter");
  }

  @Test
  public void testWhileMatchFilterDelegatesGetSkipHint() throws IOException {
    Cell hint = kv(ROW_B);
    WhileMatchFilter wmf = new WhileMatchFilter(fixedSkipHintFilter(hint));

    Cell result = wmf.getSkipHint(kv(ROW_A));
    assertNotNull(result);
    assertEquals(0, CellComparator.getInstance().compare(hint, result),
      "WhileMatchFilter must delegate getSkipHint to wrapped filter");
  }

  // ---- FilterList facade delegation ----

  @Test
  public void testFilterListDelegatesToFilterListBase() throws IOException {
    Cell hint = kv(ROW_B);
    // AND variant
    FilterList andList = new FilterList(Operator.MUST_PASS_ALL, rejectingRowHintFilter(hint));
    andList.filterRowKey(kv(ROW_A));
    assertNotNull(andList.getHintForRejectedRow(kv(ROW_A)),
      "FilterList(AND) must delegate getHintForRejectedRow");
    // OR variant
    FilterList orList = new FilterList(Operator.MUST_PASS_ONE, fixedSkipHintFilter(hint));
    assertNotNull(orList.getSkipHint(kv(ROW_A)), "FilterList(OR) must delegate getSkipHint");
  }

  // ---- Nested FilterList ----

  @Test
  public void testNestedFilterList() throws IOException {
    Cell hintA = kv(ROW_A);
    Cell hintB = kv(ROW_B);
    Cell hintC = kv(ROW_C);

    // Inner OR: all sub-filters reject, so OR rejects too. Returns min(hintA, hintC) = hintA.
    FilterList innerOR = new FilterList(Operator.MUST_PASS_ONE,
      Arrays.asList(rejectingRowHintFilter(hintA), rejectingRowHintFilter(hintC)));
    // Outer AND: returns max(innerOR=hintA, hintB) = hintB
    FilterList outerAND =
      new FilterList(Operator.MUST_PASS_ALL, Arrays.asList(innerOR, rejectingRowHintFilter(hintB)));
    outerAND.filterRowKey(kv(ROW_A));

    Cell result = outerAND.getHintForRejectedRow(kv(ROW_A));
    assertNotNull(result);
    assertEquals(0, CellComparator.getInstance().compare(hintB, result),
      "Nested AND(OR(A,C), B) must return max(min(A,C), B) = max(A, B) = B");
  }

  // ---- filterAllRemaining sub-filter is skipped ----

  @Test
  public void testFilterAllRemainingSubFilterSkipped() throws IOException {
    Cell terminatedHint = kv(ROW_C);
    Cell activeHint = kv(ROW_B);

    // Place the active rejecting filter first so filterRowKey reaches it before
    // encountering the terminated filter (which causes early return).
    FilterList fl = new FilterList(Operator.MUST_PASS_ALL,
      Arrays.asList(rejectingRowHintFilter(activeHint), terminatedFilter(terminatedHint)));
    fl.filterRowKey(kv(ROW_A));

    Cell result = fl.getHintForRejectedRow(kv(ROW_A));
    assertNotNull(result);
    assertEquals(0, CellComparator.getInstance().compare(activeHint, result),
      "Terminated sub-filters (filterAllRemaining=true) must be skipped");
  }

  // ---- All sub-filters terminated ----

  @Test
  public void testORAllSubFiltersTerminated_getHintForRejectedRow() throws IOException {
    Cell hint = kv(ROW_B);
    FilterList fl = new FilterList(Operator.MUST_PASS_ONE,
      Arrays.asList(terminatedFilter(hint), terminatedFilter(hint)));

    assertNull(fl.getHintForRejectedRow(kv(ROW_A)),
      "OR with all terminated sub-filters must return null for getHintForRejectedRow");
  }

  @Test
  public void testORAllSubFiltersTerminated_getSkipHint() throws IOException {
    Cell hint = kv(ROW_B);
    FilterList fl = new FilterList(Operator.MUST_PASS_ONE,
      Arrays.asList(terminatedFilter(hint), terminatedFilter(hint)));

    assertNull(fl.getSkipHint(kv(ROW_A)),
      "OR with all terminated sub-filters must return null for getSkipHint");
  }

  @Test
  public void testANDAllSubFiltersTerminated_getHintForRejectedRow() throws IOException {
    Cell hint = kv(ROW_B);
    FilterList fl = new FilterList(Operator.MUST_PASS_ALL,
      Arrays.asList(terminatedFilter(hint), terminatedFilter(hint)));

    assertNull(fl.getHintForRejectedRow(kv(ROW_A)),
      "AND with all terminated sub-filters must return null for getHintForRejectedRow");
  }

  @Test
  public void testANDAllSubFiltersTerminated_getSkipHint() throws IOException {
    Cell hint = kv(ROW_B);
    FilterList fl = new FilterList(Operator.MUST_PASS_ALL,
      Arrays.asList(terminatedFilter(hint), terminatedFilter(hint)));

    assertNull(fl.getSkipHint(kv(ROW_A)),
      "AND with all terminated sub-filters must return null for getSkipHint");
  }

  // ---- Empty FilterList ----

  @Test
  public void testEmptyFilterListReturnsNull() throws IOException {
    FilterList emptyAND = new FilterList(Operator.MUST_PASS_ALL);
    FilterList emptyOR = new FilterList(Operator.MUST_PASS_ONE);

    assertNull(emptyAND.getHintForRejectedRow(kv(ROW_A)),
      "Empty AND FilterList must return null for getHintForRejectedRow");
    assertNull(emptyAND.getSkipHint(kv(ROW_A)),
      "Empty AND FilterList must return null for getSkipHint");
    assertNull(emptyOR.getHintForRejectedRow(kv(ROW_A)),
      "Empty OR FilterList must return null for getHintForRejectedRow");
    assertNull(emptyOR.getSkipHint(kv(ROW_A)),
      "Empty OR FilterList must return null for getSkipHint");
  }

  // ---- Single filter pass-through ----

  @Test
  public void testSingleFilterAND() throws IOException {
    Cell hint = kv(ROW_B);
    FilterList fl = new FilterList(Operator.MUST_PASS_ALL, rejectingRowHintFilter(hint));
    fl.filterRowKey(kv(ROW_A));

    Cell result = fl.getHintForRejectedRow(kv(ROW_A));
    assertNotNull(result);
    assertEquals(0, CellComparator.getInstance().compare(hint, result),
      "Single-filter AND must pass through the hint unchanged");
  }

  @Test
  public void testSingleFilterOR() throws IOException {
    Cell hint = kv(ROW_B);
    FilterList fl = new FilterList(Operator.MUST_PASS_ONE, fixedRejectedRowHintFilter(hint));

    Cell result = fl.getHintForRejectedRow(kv(ROW_A));
    assertNotNull(result);
    assertEquals(0, CellComparator.getInstance().compare(hint, result),
      "Single-filter OR must pass through the hint unchanged");
  }

  // ---- AND contract: only consult rejecting sub-filters for getHintForRejectedRow ----

  @Test
  public void testANDGetHintForRejectedRow_onlyConsultsRejectingSubFilters() throws IOException {
    Cell hint = kv(ROW_C);
    FilterBase rejectingFilter = new FilterBase() {
      @Override
      public boolean filterRowKey(Cell cell) {
        return true;
      }

      @Override
      public Cell getHintForRejectedRow(Cell firstRowCell) {
        return hint;
      }
    };
    FilterBase acceptingFilter = new FilterBase() {
      @Override
      public boolean filterRowKey(Cell cell) {
        return false;
      }

      @Override
      public Cell getHintForRejectedRow(Cell firstRowCell) {
        throw new IllegalStateException(
          "Contract violation: getHintForRejectedRow called on non-rejecting filter");
      }
    };

    FilterList fl =
      new FilterList(Operator.MUST_PASS_ALL, Arrays.asList(rejectingFilter, acceptingFilter));
    assertTrue(fl.filterRowKey(kv(ROW_A)), "AND must reject when at least one sub-filter rejects");

    Cell result = fl.getHintForRejectedRow(kv(ROW_A));
    assertNotNull(result);
    assertEquals(0, CellComparator.getInstance().compare(hint, result),
      "AND must return the hint from the rejecting sub-filter only");
  }

  @Test
  public void testANDGetHintForRejectedRow_takesMaxFromRejectingFilters() throws IOException {
    Cell hintA = kv(ROW_A);
    Cell hintC = kv(ROW_C);
    FilterBase rejectToA = new FilterBase() {
      @Override
      public boolean filterRowKey(Cell cell) {
        return true;
      }

      @Override
      public Cell getHintForRejectedRow(Cell firstRowCell) {
        return hintA;
      }
    };
    FilterBase rejectToC = new FilterBase() {
      @Override
      public boolean filterRowKey(Cell cell) {
        return true;
      }

      @Override
      public Cell getHintForRejectedRow(Cell firstRowCell) {
        return hintC;
      }
    };

    FilterList fl = new FilterList(Operator.MUST_PASS_ALL, Arrays.asList(rejectToA, rejectToC));
    fl.filterRowKey(kv(ROW_A));

    Cell result = fl.getHintForRejectedRow(kv(ROW_A));
    assertNotNull(result);
    assertEquals(0, CellComparator.getInstance().compare(hintC, result),
      "AND must return max hint from rejecting sub-filters");
  }

  @Test
  public void testANDGetHintForRejectedRow_reversedTakesMaxFromRejectingFilters()
    throws IOException {
    Cell hintA = kv(ROW_A);
    Cell hintC = kv(ROW_C);
    FilterBase rejectToA = new FilterBase() {
      @Override
      public boolean filterRowKey(Cell cell) {
        return true;
      }

      @Override
      public Cell getHintForRejectedRow(Cell firstRowCell) {
        return hintA;
      }
    };
    FilterBase rejectToC = new FilterBase() {
      @Override
      public boolean filterRowKey(Cell cell) {
        return true;
      }

      @Override
      public Cell getHintForRejectedRow(Cell firstRowCell) {
        return hintC;
      }
    };

    FilterList fl = new FilterList(Operator.MUST_PASS_ALL, Arrays.asList(rejectToA, rejectToC));
    fl.setReversed(true);
    fl.filterRowKey(kv(ROW_C));

    Cell result = fl.getHintForRejectedRow(kv(ROW_C));
    assertNotNull(result);
    assertEquals(0, CellComparator.getInstance().compare(hintA, result),
      "Reversed AND must return smallest row key (farthest in reverse) from rejecting filters");
  }
}
