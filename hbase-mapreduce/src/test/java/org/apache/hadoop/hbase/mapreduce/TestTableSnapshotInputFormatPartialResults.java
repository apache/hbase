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
package org.apache.hadoop.hbase.mapreduce;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.StartTestingClusterOption;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableSnapshotScanner;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.regionserver.RowTooBigException;
import org.apache.hadoop.hbase.testclassification.MapReduceTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

@Tag(MapReduceTests.TAG)
@Tag(MediumTests.TAG)
public class TestTableSnapshotInputFormatPartialResults {
  private static final HBaseTestingUtil UTIL = new HBaseTestingUtil();
  private static final TableName TABLE = TableName.valueOf("partialResults");
  private static final String SNAPSHOT = "partialResultsSnapshot";
  private static final byte[][] FAMILIES = { Bytes.toBytes("f1"), Bytes.toBytes("f2") };
  private static final byte[] VALUE = new byte[1024];
  private static final int COLUMNS = 16;

  @BeforeAll
  public static void setUp() throws Exception {
    UTIL.startMiniCluster(
      StartTestingClusterOption.builder().numRegionServers(1).createRootDir(true).build());
    try (Table table = UTIL.createTable(TABLE, FAMILIES, new byte[][] { Bytes.toBytes(2) })) {
      for (int row = 0; row < 4; row++) {
        Put put = new Put(Bytes.toBytes(row));
        for (byte[] family : FAMILIES) {
          for (int column = 0; column < COLUMNS; column++) {
            put.addColumn(family, Bytes.toBytes(column), VALUE);
          }
        }
        table.put(put);
      }
    }
    UTIL.getAdmin().snapshot(SNAPSHOT, TABLE);
  }

  @AfterAll
  public static void tearDown() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  private static Scan scan(boolean partial, boolean filter) {
    Scan scan = new Scan().setAllowPartialResults(partial).setMaxResultSize(1);
    if (filter) {
      // The last visible cell may still be marked partial: the remaining cells are filtered out.
      scan.setFilter(
        new QualifierFilter(CompareOperator.LESS, new BinaryComparator(Bytes.toBytes(2))));
    }
    return scan;
  }

  private static int verifyResult(Result result, int offset, int firstRow, int columns,
    boolean partial, boolean filter) {
    assertNotNull(result);
    assertEquals(partial ? 1 : FAMILIES.length * columns, result.size());
    for (Cell cell : result.rawCells()) {
      int row = firstRow + offset / (FAMILIES.length * columns);
      int family = offset / columns % FAMILIES.length;
      int column = offset % columns;
      assertArrayEquals(Bytes.toBytes(row), CellUtil.cloneRow(cell));
      assertArrayEquals(FAMILIES[family], CellUtil.cloneFamily(cell));
      assertArrayEquals(Bytes.toBytes(column), CellUtil.cloneQualifier(cell));
      assertArrayEquals(VALUE, CellUtil.cloneValue(cell));
      offset++;
    }
    if (!filter || !partial) {
      assertEquals(offset % (FAMILIES.length * columns) != 0, result.mayHaveMoreCellsInRow());
    } else {
      assertTrue(result.mayHaveMoreCellsInRow());
    }
    return offset;
  }

  @ParameterizedTest
  @CsvSource({ "false,false,0", "true,false,0", "true,false,3", "true,true,3" })
  public void testSnapshotScanner(boolean partial, boolean filter, int limit) throws Exception {
    Scan scan = scan(partial, filter).setLimit(limit).setScanMetricsEnabled(true);
    int columns = filter ? 2 : COLUMNS;
    try (TableSnapshotScanner scanner =
      new TableSnapshotScanner(new Configuration(UTIL.getConfiguration()),
        UTIL.getDataTestDirOnTestFS("scanner"), SNAPSHOT, scan)) {
      int count = 0;
      for (Result result; (result = scanner.next()) != null;) {
        count = verifyResult(result, count, 0, columns, partial, filter);
      }
      assertEquals((limit > 0 ? limit : 4) * FAMILIES.length * columns, count);
      assertNull(scanner.next());
      if (limit == 0) {
        assertEquals(4, scanner.getScanMetrics().countOfRowsScanned.get());
      }
    }
  }

  private static Configuration jobConf(boolean filter, int limit) throws Exception {
    Configuration conf = new Configuration(UTIL.getConfiguration());
    conf.set(TableInputFormat.SCAN, TableMapReduceUtil.convertScanToString(scan(true, filter)));
    conf.setInt(TableSnapshotInputFormatImpl.SNAPSHOT_INPUTFORMAT_ROW_LIMIT_PER_INPUTSPLIT, limit);
    conf.setBoolean(TableSnapshotInputFormatImpl.SNAPSHOT_INPUTFORMAT_LOCALITY_ENABLED_KEY, false);
    TableSnapshotInputFormatImpl.setInput(conf, SNAPSHOT,
      UTIL.getDataTestDirOnTestFS("inputFormat"));
    return conf;
  }

  @ParameterizedTest
  @CsvSource({ "false,0", "false,1", "true,1" })
  public void testMapReduce(boolean filter, int limit) throws Exception {
    Configuration conf = jobConf(filter, limit);
    TableSnapshotInputFormat input = new TableSnapshotInputFormat();
    List<InputSplit> splits = input.getSplits(Job.getInstance(conf));
    assertEquals(2, splits.size());
    Set<Integer> firstRows = new HashSet<>();
    for (InputSplit split : splits) {
      TaskAttemptContextImpl context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
      try (RecordReader<ImmutableBytesWritable, Result> reader =
        input.createRecordReader(split, context)) {
        reader.initialize(split, context);
        int count = 0;
        int firstRow = -1;
        int columns = filter ? 2 : COLUMNS;
        while (reader.nextKeyValue()) {
          Result result = reader.getCurrentValue();
          if (firstRow < 0) {
            firstRow = Bytes.toInt(result.getRow());
            firstRows.add(firstRow);
          }
          assertArrayEquals(result.getRow(), reader.getCurrentKey().copyBytes());
          count = verifyResult(result, count, firstRow, columns, true, filter);
        }
        assertEquals((limit > 0 ? limit : 2) * FAMILIES.length * columns, count);
        assertFalse(reader.nextKeyValue());
      }
    }
    assertEquals(Set.of(0, 2), firstRows);
  }

  @ParameterizedTest
  @CsvSource({ "false,0", "false,1", "true,1" })
  public void testMapred(boolean filter, int limit) throws Exception {
    JobConf conf = new JobConf(jobConf(filter, limit));
    org.apache.hadoop.hbase.mapred.TableSnapshotInputFormat input =
      new org.apache.hadoop.hbase.mapred.TableSnapshotInputFormat();
    org.apache.hadoop.mapred.InputSplit[] splits = input.getSplits(conf, 0);
    assertEquals(2, splits.length);
    Set<Integer> firstRows = new HashSet<>();
    for (org.apache.hadoop.mapred.InputSplit split : splits) {
      try (org.apache.hadoop.mapred.RecordReader<ImmutableBytesWritable, Result> reader =
        input.getRecordReader(split, conf, Reporter.NULL)) {
        ImmutableBytesWritable key = reader.createKey();
        Result result = reader.createValue();
        int count = 0;
        int firstRow = -1;
        int columns = filter ? 2 : COLUMNS;
        while (reader.next(key, result)) {
          if (firstRow < 0) {
            firstRow = Bytes.toInt(result.getRow());
            firstRows.add(firstRow);
          }
          assertArrayEquals(result.getRow(), key.copyBytes());
          count = verifyResult(result, count, firstRow, columns, true, filter);
        }
        assertEquals((limit > 0 ? limit : 2) * FAMILIES.length * columns, count);
        assertFalse(reader.next(key, result));
      }
    }
    assertEquals(Set.of(0, 2), firstRows);
  }

  @Test
  public void testLargeRows() throws Exception {
    Configuration conf = new Configuration(UTIL.getConfiguration());
    conf.setLong(HConstants.TABLE_MAX_ROWSIZE_KEY, 8 * 1024);
    conf.setLong(HConstants.HBASE_CLIENT_SCANNER_MAX_RESULT_SIZE_KEY, 4096);
    Path restoreDir = UTIL.getDataTestDirOnTestFS("largeRows");
    try (TableSnapshotScanner scanner =
      new TableSnapshotScanner(new Configuration(conf), restoreDir, SNAPSHOT, scan(false, false))) {
      assertThrows(RowTooBigException.class, scanner::next);
    }
    for (Scan scan : new Scan[] { new Scan().setAllowPartialResults(true),
      new Scan().setAllowPartialResults(true).setMaxResultSize(2048),
      new Scan().setAllowPartialResults(true).setBatch(2), new Scan().setBatch(2) }) {
      try (TableSnapshotScanner scanner =
        new TableSnapshotScanner(new Configuration(conf), restoreDir, SNAPSHOT, scan)) {
        int count = 0;
        for (Result result; (result = scanner.next()) != null;) {
          assertTrue(result.size() <= (scan.getBatch() > 0 ? 2 : 4));
          for (Cell cell : result.rawCells()) {
            assertArrayEquals(Bytes.toBytes(count / (FAMILIES.length * COLUMNS)),
              CellUtil.cloneRow(cell));
            assertArrayEquals(FAMILIES[count / COLUMNS % FAMILIES.length],
              CellUtil.cloneFamily(cell));
            assertArrayEquals(Bytes.toBytes(count % COLUMNS), CellUtil.cloneQualifier(cell));
            assertArrayEquals(VALUE, CellUtil.cloneValue(cell));
            count++;
          }
          assertEquals(count % (FAMILIES.length * COLUMNS) != 0, result.mayHaveMoreCellsInRow());
        }
        assertEquals(4 * FAMILIES.length * COLUMNS, count);
      }
    }
  }

  @Test
  public void testWholeRowFilter() throws Exception {
    Scan scan = scan(true, false).setFilter(
      new SingleColumnValueFilter(FAMILIES[0], Bytes.toBytes(0), CompareOperator.EQUAL, VALUE));
    try (TableSnapshotScanner scanner =
      new TableSnapshotScanner(new Configuration(UTIL.getConfiguration()),
        UTIL.getDataTestDirOnTestFS("wholeRowFilter"), SNAPSHOT, scan)) {
      int count = 0;
      for (Result result; (result = scanner.next()) != null;) {
        count = verifyResult(result, count, 0, COLUMNS, false, false);
      }
      assertEquals(4 * FAMILIES.length * COLUMNS, count);
    }
  }
}
