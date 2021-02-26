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
package org.apache.hadoop.hbase.regionserver.querymatcher;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.TreeSet;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.regionserver.querymatcher.ScanQueryMatcher.MatchCode;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ RegionServerTests.class, SmallTests.class })
public class TestExplicitColumnTracker {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestExplicitColumnTracker.class);

  private final byte[] col1 = Bytes.toBytes("col1");
  private final byte[] col2 = Bytes.toBytes("col2");
  private final byte[] col3 = Bytes.toBytes("col3");
  private final byte[] col4 = Bytes.toBytes("col4");
  private final byte[] col5 = Bytes.toBytes("col5");

  private void runTest(int maxVersions, TreeSet<byte[]> trackColumns, List<byte[]> scannerColumns,
      List<MatchCode> expected) throws IOException {
    ColumnTracker exp = new ExplicitColumnTracker(trackColumns, 0, maxVersions, Long.MIN_VALUE);

    // Initialize result
    List<ScanQueryMatcher.MatchCode> result = new ArrayList<>(scannerColumns.size());

    long timestamp = 0;
    // "Match"
    for (byte[] col : scannerColumns) {
      result.add(ScanQueryMatcher.checkColumn(exp, col, 0, col.length, ++timestamp,
        KeyValue.Type.Put.getCode(), false));
    }

    assertEquals(expected.size(), result.size());
    for (int i = 0; i < expected.size(); i++) {
      assertEquals(expected.get(i), result.get(i));
    }
  }

  @Test
  public void testGetSingleVersion() throws IOException {
    // Create tracker
    TreeSet<byte[]> columns = new TreeSet<>(Bytes.BYTES_COMPARATOR);
    // Looking for every other
    columns.add(col2);
    columns.add(col4);
    List<MatchCode> expected = new ArrayList<>(5);
    expected.add(ScanQueryMatcher.MatchCode.SEEK_NEXT_COL); // col1
    expected.add(ScanQueryMatcher.MatchCode.INCLUDE_AND_SEEK_NEXT_COL); // col2
    expected.add(ScanQueryMatcher.MatchCode.SEEK_NEXT_COL); // col3
    expected.add(ScanQueryMatcher.MatchCode.INCLUDE_AND_SEEK_NEXT_ROW); // col4
    expected.add(ScanQueryMatcher.MatchCode.SEEK_NEXT_ROW); // col5
    int maxVersions = 1;

    // Create "Scanner"
    List<byte[]> scanner = new ArrayList<>(5);
    scanner.add(col1);
    scanner.add(col2);
    scanner.add(col3);
    scanner.add(col4);
    scanner.add(col5);

    runTest(maxVersions, columns, scanner, expected);
  }

  @Test
  public void testGetMultiVersion() throws IOException {
    // Create tracker
    TreeSet<byte[]> columns = new TreeSet<>(Bytes.BYTES_COMPARATOR);
    // Looking for every other
    columns.add(col2);
    columns.add(col4);

    List<ScanQueryMatcher.MatchCode> expected = new ArrayList<>(15);
    expected.add(ScanQueryMatcher.MatchCode.SEEK_NEXT_COL);
    expected.add(ScanQueryMatcher.MatchCode.SEEK_NEXT_COL);
    expected.add(ScanQueryMatcher.MatchCode.SEEK_NEXT_COL);

    expected.add(ScanQueryMatcher.MatchCode.INCLUDE); // col2; 1st version
    expected.add(ScanQueryMatcher.MatchCode.INCLUDE_AND_SEEK_NEXT_COL); // col2; 2nd version
    expected.add(ScanQueryMatcher.MatchCode.SEEK_NEXT_COL);

    expected.add(ScanQueryMatcher.MatchCode.SEEK_NEXT_COL);
    expected.add(ScanQueryMatcher.MatchCode.SEEK_NEXT_COL);
    expected.add(ScanQueryMatcher.MatchCode.SEEK_NEXT_COL);

    expected.add(ScanQueryMatcher.MatchCode.INCLUDE); // col4; 1st version
    expected.add(ScanQueryMatcher.MatchCode.INCLUDE_AND_SEEK_NEXT_ROW); // col4; 2nd version
    expected.add(ScanQueryMatcher.MatchCode.SEEK_NEXT_ROW);

    expected.add(ScanQueryMatcher.MatchCode.SEEK_NEXT_ROW);
    expected.add(ScanQueryMatcher.MatchCode.SEEK_NEXT_ROW);
    expected.add(ScanQueryMatcher.MatchCode.SEEK_NEXT_ROW);
    int maxVersions = 2;

    // Create "Scanner"
    List<byte[]> scanner = new ArrayList<>(15);
    scanner.add(col1);
    scanner.add(col1);
    scanner.add(col1);
    scanner.add(col2);
    scanner.add(col2);
    scanner.add(col2);
    scanner.add(col3);
    scanner.add(col3);
    scanner.add(col3);
    scanner.add(col4);
    scanner.add(col4);
    scanner.add(col4);
    scanner.add(col5);
    scanner.add(col5);
    scanner.add(col5);

    // Initialize result
    runTest(maxVersions, columns, scanner, expected);
  }

  /**
   * hbase-2259
   */
  @Test
  public void testStackOverflow() throws IOException {
    int maxVersions = 1;
    TreeSet<byte[]> columns = new TreeSet<>(Bytes.BYTES_COMPARATOR);
    for (int i = 0; i < 100000; i++) {
      columns.add(Bytes.toBytes("col" + i));
    }

    ColumnTracker explicit = new ExplicitColumnTracker(columns, 0, maxVersions, Long.MIN_VALUE);
    for (int i = 0; i < 100000; i += 2) {
      byte[] col = Bytes.toBytes("col" + i);
      ScanQueryMatcher.checkColumn(explicit, col, 0, col.length, 1, KeyValue.Type.Put.getCode(),
        false);
    }
    explicit.reset();

    for (int i = 1; i < 100000; i += 2) {
      byte[] col = Bytes.toBytes("col" + i);
      ScanQueryMatcher.checkColumn(explicit, col, 0, col.length, 1, KeyValue.Type.Put.getCode(),
        false);
    }
  }

  /**
   * Regression test for HBASE-2545
   */
  @Test
  public void testInfiniteLoop() throws IOException {
    TreeSet<byte[]> columns = new TreeSet<>(Bytes.BYTES_COMPARATOR);
    columns.addAll(Arrays.asList(new byte[][] { col2, col3, col5 }));
    List<byte[]> scanner = Arrays.<byte[]> asList(new byte[][] { col1, col4 });
    List<ScanQueryMatcher.MatchCode> expected = Arrays.<ScanQueryMatcher.MatchCode> asList(
      new ScanQueryMatcher.MatchCode[] { ScanQueryMatcher.MatchCode.SEEK_NEXT_COL,
          ScanQueryMatcher.MatchCode.SEEK_NEXT_COL });
    runTest(1, columns, scanner, expected);
  }

}
