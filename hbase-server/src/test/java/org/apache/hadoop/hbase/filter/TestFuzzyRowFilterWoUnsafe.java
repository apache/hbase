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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mockStatic;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.testclassification.FilterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.unsafe.HBasePlatformDependent;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import org.apache.hbase.thirdparty.com.google.protobuf.UnsafeByteOperations;

import org.apache.hadoop.hbase.shaded.protobuf.generated.FilterProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.BytesBytesPair;

/**
 * Exercises {@link FuzzyRowFilter} on the "no-unsafe" code path
 * ({@code HBasePlatformDependent.unaligned() == false}) end-to-end, i.e. through the constructor,
 * {@link FuzzyRowFilter#filterCell} and {@link FuzzyRowFilter#getNextCellHint}. On this path the
 * mask uses the {0 (fixed), 1 (non-fixed)} encoding, which differs from the unsafe {-1, 2} encoding
 * that the rest of the scan machinery assumes.
 */
@Tag(FilterTests.TAG)
@Tag(SmallTests.TAG)
public class TestFuzzyRowFilterWoUnsafe {

  @BeforeAll
  public static void disableUnsafe() throws Exception {
    // Force the no-unsafe path: make HBasePlatformDependent.unaligned() return false and trigger
    // FuzzyRowFilter's static initializer while the mock is active, so its private static final
    // UNSAFE_UNALIGNED is captured as false for this JVM fork.
    try (MockedStatic<HBasePlatformDependent> mocked = mockStatic(HBasePlatformDependent.class)) {
      mocked.when(HBasePlatformDependent::isUnsafeAvailable).thenReturn(false);
      mocked.when(HBasePlatformDependent::unaligned).thenReturn(false);
      Field field = FuzzyRowFilter.class.getDeclaredField("UNSAFE_UNALIGNED");
      field.setAccessible(true);
      assertFalse(field.getBoolean(null), "expected FuzzyRowFilter to use the no-unsafe path");
    }
  }

  /**
   * A row that matches the fuzzy rule only thanks to a wildcard position must be INCLUDEd. On the
   * broken no-unsafe path the mask was shifted to all-zeroes (all positions treated as fixed), so
   * the wildcard row was wrongly rejected.
   */
  @Test
  public void testForwardMatchesWildcardRow() {
    FuzzyRowFilter filter = new FuzzyRowFilter(
      Collections.singletonList(new Pair<>(new byte[] { 1, 2, 3 }, new byte[] { 0, 1, 0 })));

    KeyValue match = KeyValueUtil.createFirstOnRow(new byte[] { 1, 99, 3 });
    assertEquals(Filter.ReturnCode.INCLUDE, filter.filterCell(match));
  }

  /**
   * For a non-matching row the next-cell hint must be the smallest row that can satisfy the rule.
   * With fixed positions 5 and 5 and a wildcard in the middle, the smallest row at or after {3,0,0}
   * is {5,0,5}. On the broken no-unsafe path the wildcard key byte was never cleared and the mask
   * was corrupted, producing a wrong hint.
   */
  @Test
  public void testForwardHintSkipsToSmallestMatchingRow() {
    FuzzyRowFilter filter = new FuzzyRowFilter(
      Collections.singletonList(new Pair<>(new byte[] { 5, 100, 5 }, new byte[] { 0, 1, 0 })));

    KeyValue current = KeyValueUtil.createFirstOnRow(new byte[] { 3, 0, 0 });
    assertEquals(Filter.ReturnCode.SEEK_NEXT_USING_HINT, filter.filterCell(current));

    Cell hint = filter.getNextCellHint(current);
    assertRow(new byte[] { 5, 0, 5 }, hint);
  }

  /**
   * No-unsafe analogue of {@code TestFuzzyRowFilter#testReverseFilterCellSkipsSameRowHint}. This is
   * the most subtle composition point this change touches: the reverse next-cell hint is computed
   * from the no-unsafe mask (updateWith -&gt; preprocessMaskForHinting({0,1}-&gt;{-1,0}) -&gt;
   * getNextForFuzzyRule(reverse)) and must still play with the HBASE-30226 same-row short-circuit.
   * A non-matching row seeks back to "abb"; revisiting "abb" must be skipped with NEXT_ROW instead
   * of recreating the same hint.
   */
  @Test
  public void testReverseHintSkipsSameRow() {
    FuzzyRowFilter filter = new FuzzyRowFilter(
      Collections.singletonList(new Pair<>(Bytes.toBytes("aaa"), new byte[] { 0, 1, 0 })));
    filter.setReversed(true);

    KeyValue abc = KeyValueUtil.createFirstOnRow(Bytes.toBytes("abc"));
    assertEquals(Filter.ReturnCode.SEEK_NEXT_USING_HINT, filter.filterCell(abc));
    assertRow(Bytes.toBytes("abb"), filter.getNextCellHint(abc));

    KeyValue abb = KeyValueUtil.createFirstOnRow(Bytes.toBytes("abb"));
    assertEquals(Filter.ReturnCode.NEXT_ROW, filter.filterCell(abb));
  }

  /**
   * With multiple fuzzy keys the RowTracker priority queue holds one (separately converted) hint
   * mask per key and must return the smallest matching row across all of them. Here key1 {5,*,5}
   * hints {5,0,5} and key2 {4,9,9} hints {4,9,9}; the smaller {4,9,9} must win.
   */
  @Test
  public void testForwardHintWithMultipleKeysReturnsSmallest() {
    FuzzyRowFilter filter =
      new FuzzyRowFilter(Arrays.asList(new Pair<>(new byte[] { 5, 100, 5 }, new byte[] { 0, 1, 0 }),
        new Pair<>(new byte[] { 4, 9, 9 }, new byte[] { 0, 0, 0 })));

    KeyValue current = KeyValueUtil.createFirstOnRow(new byte[] { 3, 0, 0 });
    assertEquals(Filter.ReturnCode.SEEK_NEXT_USING_HINT, filter.filterCell(current));
    assertRow(new byte[] { 4, 9, 9 }, filter.getNextCellHint(current));
  }

  /**
   * A FuzzyRowFilter serialized by an unsafe peer puts the mask on the wire in its preprocessed {-1
   * (fixed), 2 (non-fixed)} form. A no-unsafe server must still interpret it correctly: this is
   * exactly the {key, mask} pair {@link FuzzyRowFilter#parseFrom} hands to the constructor for such
   * a filter. Before the fix the no-unsafe constructor left {-1, 2} untouched, so
   * {@link FuzzyRowFilter#satisfiesNoUnsafe} (which keys off {0, 1}) treated every position as a
   * wildcard and stopped enforcing the fixed bytes, wrongly INCLUDEing non-matching rows.
   */
  @Test
  public void testUnsafeEncodedMaskFromPeerEnforcesFixedPositions() {
    // {-1, 2, -1} == fixed, non-fixed, fixed -> equivalent no-unsafe mask {0, 1, 0}.
    FuzzyRowFilter match = new FuzzyRowFilter(
      Collections.singletonList(new Pair<>(new byte[] { 1, 0, 3 }, new byte[] { -1, 2, -1 })));
    assertEquals(Filter.ReturnCode.INCLUDE,
      match.filterCell(KeyValueUtil.createFirstOnRow(new byte[] { 1, 99, 3 })));

    // A row that differs at a FIXED position (pos 2: 9 != 3) must not be INCLUDEd.
    FuzzyRowFilter reject = new FuzzyRowFilter(
      Collections.singletonList(new Pair<>(new byte[] { 1, 0, 3 }, new byte[] { -1, 2, -1 })));
    assertEquals(Filter.ReturnCode.SEEK_NEXT_USING_HINT,
      reject.filterCell(KeyValueUtil.createFirstOnRow(new byte[] { 1, 99, 9 })));
  }

  /**
   * Faithful wire-level companion to {@link #testUnsafeEncodedMaskFromPeerEnforcesFixedPositions}:
   * build the exact protobuf bytes an unsafe peer emits (mask in the preprocessed {-1, 2} form) and
   * run them through {@link FuzzyRowFilter#parseFrom}. A no-unsafe server must normalize the mask
   * back to {0, 1} on deserialization and still enforce the fixed positions.
   */
  @Test
  public void testParseFromUnsafeEncodedFilterEnforcesFixedPositions() throws Exception {
    byte[] wire = FilterProtos.FuzzyRowFilter.newBuilder()
      .addFuzzyKeysData(BytesBytesPair.newBuilder()
        .setFirst(UnsafeByteOperations.unsafeWrap(new byte[] { 1, 0, 3 }))
        .setSecond(UnsafeByteOperations.unsafeWrap(new byte[] { -1, 2, -1 })))
      .build().toByteArray();

    assertEquals(Filter.ReturnCode.INCLUDE, FuzzyRowFilter.parseFrom(wire)
      .filterCell(KeyValueUtil.createFirstOnRow(new byte[] { 1, 99, 3 })));
    // A row that differs at a FIXED position (pos 2: 9 != 3) must not be INCLUDEd.
    assertEquals(Filter.ReturnCode.SEEK_NEXT_USING_HINT, FuzzyRowFilter.parseFrom(wire)
      .filterCell(KeyValueUtil.createFirstOnRow(new byte[] { 1, 99, 9 })));
  }

  /**
   * A filter built on a no-unsafe server must survive its own {@link FuzzyRowFilter#toByteArray} /
   * {@link FuzzyRowFilter#parseFrom} round-trip with identical behavior and identical
   * {@code equals}/{@code hashCode} (the wire form is the canonical {0, 1}).
   */
  @Test
  public void testSerializationRoundTripPreservesFilter() throws Exception {
    FuzzyRowFilter original = new FuzzyRowFilter(
      Collections.singletonList(new Pair<>(new byte[] { 1, 2, 3 }, new byte[] { 0, 1, 0 })));
    FuzzyRowFilter parsed = FuzzyRowFilter.parseFrom(original.toByteArray());

    assertEquals(original, parsed);
    assertEquals(original.hashCode(), parsed.hashCode());
    assertEquals(Filter.ReturnCode.INCLUDE,
      parsed.filterCell(KeyValueUtil.createFirstOnRow(new byte[] { 1, 99, 3 })));
  }

  private static void assertRow(byte[] expected, Cell cell) {
    byte[] actual = Bytes.copy(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
    assertEquals(Bytes.toStringBinary(expected), Bytes.toStringBinary(actual));
  }

  // ---------------------------------------------------------------------------
  // Fine-grained unit coverage of the mask/key preprocessing helpers used above.
  // ---------------------------------------------------------------------------

  @Test
  public void testPreprocessMaskForSatisfiesNoUnsafeKeepsMaskSemantics() {
    byte[] row = new byte[] { 1, 2, 1, 3, 3 };
    byte[] fuzzyKey = new byte[] { 1, 2, 0, 3 };
    byte[] fuzzyKeyFiltered = new byte[] { 0, 2, 0, 3 };
    byte[] mask = new byte[] { 0, 0, 1, 0 };

    assertEquals(FuzzyRowFilter.SatisfiesCode.YES,
      FuzzyRowFilter.satisfiesNoUnsafe(false, row, 0, row.length, fuzzyKey, mask));
    assertEquals(FuzzyRowFilter.SatisfiesCode.NO_NEXT,
      FuzzyRowFilter.satisfiesNoUnsafe(false, row, 0, row.length, fuzzyKeyFiltered, mask));

    // On the no-unsafe path the mask must be left untouched (the broken code shifted it to 0s).
    FuzzyRowFilter.preprocessMaskForSatisfies(mask, false);
    assertArrayEquals(new byte[] { 0, 0, 1, 0 }, mask);

    assertEquals(FuzzyRowFilter.SatisfiesCode.YES,
      FuzzyRowFilter.satisfiesNoUnsafe(false, row, 0, row.length, fuzzyKey, mask));
    assertEquals(FuzzyRowFilter.SatisfiesCode.NO_NEXT,
      FuzzyRowFilter.satisfiesNoUnsafe(false, row, 0, row.length, fuzzyKeyFiltered, mask));
  }

  @Test
  public void testPreprocessMaskForHintingNoUnsafeConvertsToGetNextSemantics() {
    byte[] row = new byte[] { 1, 2, 1, 3, 3 };
    byte[] fuzzyKey = new byte[] { 1, 2, 0, 3 };
    byte[] noUnsafeMask = new byte[] { 0, 0, 1, 0 };

    byte[] convertedMask = FuzzyRowFilter.preprocessMaskForHinting(noUnsafeMask, false);
    // The original mask must not be mutated (satisfiesNoUnsafe still needs the {0, 1} form).
    assertArrayEquals(new byte[] { 0, 0, 1, 0 }, noUnsafeMask);
    assertArrayEquals(new byte[] { -1, -1, 0, -1 }, convertedMask);

    byte[] nextWithConverted =
      FuzzyRowFilter.getNextForFuzzyRule(false, row, 0, row.length, fuzzyKey, convertedMask);
    byte[] nextWithExpectedMask = FuzzyRowFilter.getNextForFuzzyRule(false, row, 0, row.length,
      fuzzyKey, new byte[] { -1, -1, 0, -1 });

    assertNotNull(nextWithConverted);
    assertArrayEquals(nextWithExpectedMask, nextWithConverted);
  }

  @Test
  public void testNoUnsafePreprocessSearchKeyClearsWildcardBytes() {
    Pair<byte[], byte[]> fuzzyData = new Pair<>(new byte[] { 1, 100, 3 }, new byte[] { 0, 1, 0 });

    FuzzyRowFilter.preprocessSearchKey(fuzzyData, false);
    byte[] convertedMask = FuzzyRowFilter.preprocessMaskForHinting(fuzzyData.getSecond(), false);
    byte[] nextForFuzzyRule = FuzzyRowFilter.getNextForFuzzyRule(false, new byte[] { 0, 0, 0 }, 0,
      3, fuzzyData.getFirst(), convertedMask);

    // The wildcard byte (100) must be cleared so the next hint is the smallest matching row.
    assertArrayEquals(new byte[] { 1, 0, 3 }, fuzzyData.getFirst());
    assertArrayEquals(new byte[] { 1, 0, 3 }, nextForFuzzyRule);
  }
}
