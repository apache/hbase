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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.testclassification.FilterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Scans with a {@link PrefixFilter} that meet rows shorter than the prefix.
 */
@Tag(FilterTests.TAG)
@Tag(MediumTests.TAG)
public class TestPrefixFilterShorterRow {

  private static final HBaseTestingUtil UTIL = new HBaseTestingUtil();
  private static final TableName TABLE_NAME = TableName.valueOf("TestPrefixFilterShorterRow");
  private static final byte[] FAMILY = Bytes.toBytes("f");
  private static final byte[] QUALIFIER = Bytes.toBytes("q");
  // In a cell read from a store file, the row "ab" is followed by the family length (1) and the
  // family "f".
  private static final List<String> ROWS = Arrays.asList("ab", "ab\\x00x", "ab\\x01f1", "row-a",
    "row-a/1", "row-a/2", "row-a0", "row-b", "row-b/1", "row-c/1");

  private static Table table;

  @BeforeAll
  public static void setUp() throws Exception {
    UTIL.getConfiguration().setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 1);
    UTIL.startMiniCluster(1);
    table = UTIL.createTable(TABLE_NAME, FAMILY);
    for (String row : ROWS) {
      table.put(new Put(Bytes.toBytesBinary(row)).addColumn(FAMILY, QUALIFIER, Bytes.toBytes("v")));
    }
    UTIL.flush(TABLE_NAME);
  }

  @AfterAll
  public static void tearDown() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void testBytesAfterShorterRowMatchPrefix() throws IOException {
    Scan scan = new Scan().setFilter(new PrefixFilter(Bytes.toBytesBinary("ab\\x01f")));

    assertEquals(Arrays.asList("ab\\x01f1"), scanRows(scan));
  }

  @Test
  public void testBytesAfterShorterRowAreAfterPrefix() throws IOException {
    Scan scan = new Scan().setFilter(new PrefixFilter(Bytes.toBytesBinary("ab\\x00")));

    assertEquals(Arrays.asList("ab\\x00x"), scanRows(scan));
  }

  /**
   * For a scan with explicit columns the store file scanners used to seek lazily. When the delete
   * family bloom filter tells that a store file has no delete family marker for the start row, such
   * a scanner returns a fake cell built from the scan start row without seeking. Before HBASE-27691
   * the region scanner handed that cell to the filter. Its row is the start row itself, in an array
   * that holds nothing else.
   */
  @Test
  public void testStartRowShorterThanPrefixWithExplicitColumns() throws IOException {
    Scan scan = new Scan().withStartRow(Bytes.toBytes("row-a")).addColumn(FAMILY, QUALIFIER)
      .setFilter(new PrefixFilter(Bytes.toBytes("row-a/")));

    assertEquals(Arrays.asList("row-a/1", "row-a/2"), scanRows(scan));
  }

  @Test
  public void testStartRowShorterThanPrefixesInFilterListWithOr() throws IOException {
    Filter filter = new FilterList(FilterList.Operator.MUST_PASS_ONE,
      new PrefixFilter(Bytes.toBytes("row-a/")), new PrefixFilter(Bytes.toBytes("row-b/")));
    Scan scan = new Scan().withStartRow(Bytes.toBytes("row-a")).addColumn(FAMILY, QUALIFIER)
      .setFilter(filter);

    assertEquals(Arrays.asList("row-a/1", "row-a/2", "row-b/1"), scanRows(scan));
  }

  @Test
  public void testStartRowShorterThanPrefixReversed() throws IOException {
    Scan scan = new Scan().withStartRow(Bytes.toBytes("row-b")).addColumn(FAMILY, QUALIFIER)
      .setFilter(new PrefixFilter(Bytes.toBytes("row-a/"))).setReversed(true);

    assertEquals(Arrays.asList("row-a/2", "row-a/1"), scanRows(scan));
  }

  private static List<String> scanRows(Scan scan) throws IOException {
    List<String> rows = new ArrayList<>();
    try (ResultScanner scanner = table.getScanner(scan)) {
      for (Result result : scanner) {
        rows.add(Bytes.toStringBinary(result.getRow()));
      }
    }
    return rows;
  }
}
