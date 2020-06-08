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
package org.apache.hadoop.hbase.mapreduce;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.mapreduce.SyncTable.SyncMapper.Counter;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Counters;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.base.Throwables;

/**
 * Basic test for the SyncTable M/R tool
 */
@Category(LargeTests.class)
public class TestSyncTable {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestSyncTable.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestSyncTable.class);

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  @Rule
  public TestName name = new TestName();

  @BeforeClass
  public static void beforeClass() throws Exception {
    TEST_UTIL.startMiniCluster(3);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    TEST_UTIL.cleanupDataTestDirOnTestFS();
    TEST_UTIL.shutdownMiniCluster();
  }

  private static byte[][] generateSplits(int numRows, int numRegions) {
    byte[][] splitRows = new byte[numRegions-1][];
    for (int i = 1; i < numRegions; i++) {
      splitRows[i-1] = Bytes.toBytes(numRows * i / numRegions);
    }
    return splitRows;
  }

  @Test
  public void testSyncTable() throws Exception {
    final TableName sourceTableName = TableName.valueOf(name.getMethodName() + "_source");
    final TableName targetTableName = TableName.valueOf(name.getMethodName() + "_target");
    Path testDir = TEST_UTIL.getDataTestDirOnTestFS("testSyncTable");

    writeTestData(sourceTableName, targetTableName);
    hashSourceTable(sourceTableName, testDir);
    Counters syncCounters = syncTables(sourceTableName, targetTableName, testDir);
    assertEqualTables(90, sourceTableName, targetTableName, false);

    assertEquals(60, syncCounters.findCounter(Counter.ROWSWITHDIFFS).getValue());
    assertEquals(10, syncCounters.findCounter(Counter.SOURCEMISSINGROWS).getValue());
    assertEquals(10, syncCounters.findCounter(Counter.TARGETMISSINGROWS).getValue());
    assertEquals(50, syncCounters.findCounter(Counter.SOURCEMISSINGCELLS).getValue());
    assertEquals(50, syncCounters.findCounter(Counter.TARGETMISSINGCELLS).getValue());
    assertEquals(20, syncCounters.findCounter(Counter.DIFFERENTCELLVALUES).getValue());

    TEST_UTIL.deleteTable(sourceTableName);
    TEST_UTIL.deleteTable(targetTableName);
  }

  @Test
  public void testSyncTableDoDeletesFalse() throws Exception {
    final TableName sourceTableName = TableName.valueOf(name.getMethodName() + "_source");
    final TableName targetTableName = TableName.valueOf(name.getMethodName() + "_target");
    Path testDir = TEST_UTIL.getDataTestDirOnTestFS("testSyncTableDoDeletesFalse");

    writeTestData(sourceTableName, targetTableName);
    hashSourceTable(sourceTableName, testDir);
    Counters syncCounters = syncTables(sourceTableName, targetTableName,
        testDir, "--doDeletes=false");
    assertTargetDoDeletesFalse(100, sourceTableName, targetTableName);

    assertEquals(60, syncCounters.findCounter(Counter.ROWSWITHDIFFS).getValue());
    assertEquals(10, syncCounters.findCounter(Counter.SOURCEMISSINGROWS).getValue());
    assertEquals(10, syncCounters.findCounter(Counter.TARGETMISSINGROWS).getValue());
    assertEquals(50, syncCounters.findCounter(Counter.SOURCEMISSINGCELLS).getValue());
    assertEquals(50, syncCounters.findCounter(Counter.TARGETMISSINGCELLS).getValue());
    assertEquals(20, syncCounters.findCounter(Counter.DIFFERENTCELLVALUES).getValue());

    TEST_UTIL.deleteTable(sourceTableName);
    TEST_UTIL.deleteTable(targetTableName);
  }

  @Test
  public void testSyncTableDoPutsFalse() throws Exception {
    final TableName sourceTableName = TableName.valueOf(name.getMethodName() + "_source");
    final TableName targetTableName = TableName.valueOf(name.getMethodName() + "_target");
    Path testDir = TEST_UTIL.getDataTestDirOnTestFS("testSyncTableDoPutsFalse");

    writeTestData(sourceTableName, targetTableName);
    hashSourceTable(sourceTableName, testDir);
    Counters syncCounters = syncTables(sourceTableName, targetTableName,
        testDir, "--doPuts=false");
    assertTargetDoPutsFalse(70, sourceTableName, targetTableName);

    assertEquals(60, syncCounters.findCounter(Counter.ROWSWITHDIFFS).getValue());
    assertEquals(10, syncCounters.findCounter(Counter.SOURCEMISSINGROWS).getValue());
    assertEquals(10, syncCounters.findCounter(Counter.TARGETMISSINGROWS).getValue());
    assertEquals(50, syncCounters.findCounter(Counter.SOURCEMISSINGCELLS).getValue());
    assertEquals(50, syncCounters.findCounter(Counter.TARGETMISSINGCELLS).getValue());
    assertEquals(20, syncCounters.findCounter(Counter.DIFFERENTCELLVALUES).getValue());

    TEST_UTIL.deleteTable(sourceTableName);
    TEST_UTIL.deleteTable(targetTableName);
  }

  @Test
  public void testSyncTableIgnoreTimestampsTrue() throws Exception {
    final TableName sourceTableName = TableName.valueOf(name.getMethodName() + "_source");
    final TableName targetTableName = TableName.valueOf(name.getMethodName() + "_target");
    Path testDir = TEST_UTIL.getDataTestDirOnTestFS("testSyncTableIgnoreTimestampsTrue");
    long current = System.currentTimeMillis();
    writeTestData(sourceTableName, targetTableName, current - 1000, current);
    hashSourceTable(sourceTableName, testDir, "--ignoreTimestamps=true");
    Counters syncCounters = syncTables(sourceTableName, targetTableName,
      testDir, "--ignoreTimestamps=true");
    assertEqualTables(90, sourceTableName, targetTableName, true);

    assertEquals(50, syncCounters.findCounter(Counter.ROWSWITHDIFFS).getValue());
    assertEquals(10, syncCounters.findCounter(Counter.SOURCEMISSINGROWS).getValue());
    assertEquals(10, syncCounters.findCounter(Counter.TARGETMISSINGROWS).getValue());
    assertEquals(30, syncCounters.findCounter(Counter.SOURCEMISSINGCELLS).getValue());
    assertEquals(30, syncCounters.findCounter(Counter.TARGETMISSINGCELLS).getValue());
    assertEquals(20, syncCounters.findCounter(Counter.DIFFERENTCELLVALUES).getValue());

    TEST_UTIL.deleteTable(sourceTableName);
    TEST_UTIL.deleteTable(targetTableName);
  }

  private void assertEqualTables(int expectedRows, TableName sourceTableName,
      TableName targetTableName, boolean ignoreTimestamps) throws Exception {
    Table sourceTable = TEST_UTIL.getConnection().getTable(sourceTableName);
    Table targetTable = TEST_UTIL.getConnection().getTable(targetTableName);

    ResultScanner sourceScanner = sourceTable.getScanner(new Scan());
    ResultScanner targetScanner = targetTable.getScanner(new Scan());

    for (int i = 0; i < expectedRows; i++) {
      Result sourceRow = sourceScanner.next();
      Result targetRow = targetScanner.next();

      LOG.debug("SOURCE row: " + (sourceRow == null ? "null" : Bytes.toInt(sourceRow.getRow()))
          + " cells:" + sourceRow);
      LOG.debug("TARGET row: " + (targetRow == null ? "null" : Bytes.toInt(targetRow.getRow()))
          + " cells:" + targetRow);

      if (sourceRow == null) {
        Assert.fail("Expected " + expectedRows
            + " source rows but only found " + i);
      }
      if (targetRow == null) {
        Assert.fail("Expected " + expectedRows
            + " target rows but only found " + i);
      }
      Cell[] sourceCells = sourceRow.rawCells();
      Cell[] targetCells = targetRow.rawCells();
      if (sourceCells.length != targetCells.length) {
        LOG.debug("Source cells: " + Arrays.toString(sourceCells));
        LOG.debug("Target cells: " + Arrays.toString(targetCells));
        Assert.fail("Row " + Bytes.toInt(sourceRow.getRow())
            + " has " + sourceCells.length
            + " cells in source table but " + targetCells.length
            + " cells in target table");
      }
      for (int j = 0; j < sourceCells.length; j++) {
        Cell sourceCell = sourceCells[j];
        Cell targetCell = targetCells[j];
        try {
          if (!CellUtil.matchingRows(sourceCell, targetCell)) {
            Assert.fail("Rows don't match");
          }
          if (!CellUtil.matchingFamily(sourceCell, targetCell)) {
            Assert.fail("Families don't match");
          }
          if (!CellUtil.matchingQualifier(sourceCell, targetCell)) {
            Assert.fail("Qualifiers don't match");
          }
          if (!ignoreTimestamps && !CellUtil.matchingTimestamp(sourceCell, targetCell)) {
            Assert.fail("Timestamps don't match");
          }
          if (!CellUtil.matchingValue(sourceCell, targetCell)) {
            Assert.fail("Values don't match");
          }
        } catch (Throwable t) {
          LOG.debug("Source cell: " + sourceCell + " target cell: " + targetCell);
          Throwables.propagate(t);
        }
      }
    }
    Result sourceRow = sourceScanner.next();
    if (sourceRow != null) {
      Assert.fail("Source table has more than " + expectedRows
          + " rows.  Next row: " + Bytes.toInt(sourceRow.getRow()));
    }
    Result targetRow = targetScanner.next();
    if (targetRow != null) {
      Assert.fail("Target table has more than " + expectedRows
          + " rows.  Next row: " + Bytes.toInt(targetRow.getRow()));
    }
    sourceScanner.close();
    targetScanner.close();
    sourceTable.close();
    targetTable.close();
  }

  private void assertTargetDoDeletesFalse(int expectedRows, TableName sourceTableName,
      TableName targetTableName) throws Exception {
    Table sourceTable = TEST_UTIL.getConnection().getTable(sourceTableName);
    Table targetTable = TEST_UTIL.getConnection().getTable(targetTableName);

    ResultScanner sourceScanner = sourceTable.getScanner(new Scan());
    ResultScanner targetScanner = targetTable.getScanner(new Scan());
    Result targetRow = targetScanner.next();
    Result sourceRow = sourceScanner.next();
    int rowsCount = 0;
    while (targetRow != null) {
      rowsCount++;
      //only compares values for existing rows, skipping rows existing on
      //target only that were not deleted given --doDeletes=false
      if (Bytes.toInt(sourceRow.getRow()) != Bytes.toInt(targetRow.getRow())) {
        targetRow = targetScanner.next();
        continue;
      }

      LOG.debug("SOURCE row: " + (sourceRow == null ? "null"
          : Bytes.toInt(sourceRow.getRow()))
          + " cells:" + sourceRow);
      LOG.debug("TARGET row: " + (targetRow == null ? "null"
          : Bytes.toInt(targetRow.getRow()))
          + " cells:" + targetRow);

      Cell[] sourceCells = sourceRow.rawCells();
      Cell[] targetCells = targetRow.rawCells();
      int targetRowKey = Bytes.toInt(targetRow.getRow());
      if (targetRowKey >= 70 && targetRowKey < 80) {
        if (sourceCells.length == targetCells.length) {
          LOG.debug("Source cells: " + Arrays.toString(sourceCells));
          LOG.debug("Target cells: " + Arrays.toString(targetCells));
          Assert.fail("Row " + targetRowKey + " should have more cells in "
              + "target than in source");
        }

      } else {
        if (sourceCells.length != targetCells.length) {
          LOG.debug("Source cells: " + Arrays.toString(sourceCells));
          LOG.debug("Target cells: " + Arrays.toString(targetCells));
          Assert.fail("Row " + Bytes.toInt(sourceRow.getRow())
              + " has " + sourceCells.length
              + " cells in source table but " + targetCells.length
              + " cells in target table");
        }
      }
      for (int j = 0; j < sourceCells.length; j++) {
        Cell sourceCell = sourceCells[j];
        Cell targetCell = targetCells[j];
        try {
          if (!CellUtil.matchingRow(sourceCell, targetCell)) {
            Assert.fail("Rows don't match");
          }
          if (!CellUtil.matchingFamily(sourceCell, targetCell)) {
            Assert.fail("Families don't match");
          }
          if (!CellUtil.matchingQualifier(sourceCell, targetCell)) {
            Assert.fail("Qualifiers don't match");
          }
          if (targetRowKey < 80 && targetRowKey >= 90){
            if (!CellUtil.matchingTimestamp(sourceCell, targetCell)) {
              Assert.fail("Timestamps don't match");
            }
          }
          if (!CellUtil.matchingValue(sourceCell, targetCell)) {
            Assert.fail("Values don't match");
          }
        } catch (Throwable t) {
          LOG.debug("Source cell: " + sourceCell + " target cell: "
              + targetCell);
          Throwables.propagate(t);
        }
      }
      targetRow = targetScanner.next();
      sourceRow = sourceScanner.next();
    }
    assertEquals("Target expected rows does not match.",expectedRows,
        rowsCount);
    sourceScanner.close();
    targetScanner.close();
    sourceTable.close();
    targetTable.close();
  }

  private void assertTargetDoPutsFalse(int expectedRows, TableName sourceTableName,
      TableName targetTableName) throws Exception {
    Table sourceTable = TEST_UTIL.getConnection().getTable(sourceTableName);
    Table targetTable = TEST_UTIL.getConnection().getTable(targetTableName);

    ResultScanner sourceScanner = sourceTable.getScanner(new Scan());
    ResultScanner targetScanner = targetTable.getScanner(new Scan());
    Result targetRow = targetScanner.next();
    Result sourceRow = sourceScanner.next();
    int rowsCount = 0;

    while (targetRow!=null) {
      //only compares values for existing rows, skipping rows existing on
      //source only that were not added to target given --doPuts=false
      if (Bytes.toInt(sourceRow.getRow()) != Bytes.toInt(targetRow.getRow())) {
        sourceRow = sourceScanner.next();
        continue;
      }

      LOG.debug("SOURCE row: " + (sourceRow == null ?
          "null" :
          Bytes.toInt(sourceRow.getRow()))
          + " cells:" + sourceRow);
      LOG.debug("TARGET row: " + (targetRow == null ?
          "null" :
          Bytes.toInt(targetRow.getRow()))
          + " cells:" + targetRow);

      LOG.debug("rowsCount: " + rowsCount);

      Cell[] sourceCells = sourceRow.rawCells();
      Cell[] targetCells = targetRow.rawCells();
      int targetRowKey = Bytes.toInt(targetRow.getRow());
      if (targetRowKey >= 40 && targetRowKey < 60) {
        LOG.debug("Source cells: " + Arrays.toString(sourceCells));
        LOG.debug("Target cells: " + Arrays.toString(targetCells));
        Assert.fail("There shouldn't exist any rows between 40 and 60, since "
            + "Puts are disabled and Deletes are enabled.");
      } else if (targetRowKey >= 60 && targetRowKey < 70) {
        if (sourceCells.length == targetCells.length) {
          LOG.debug("Source cells: " + Arrays.toString(sourceCells));
          LOG.debug("Target cells: " + Arrays.toString(targetCells));
          Assert.fail("Row " + Bytes.toInt(sourceRow.getRow())
              + " shouldn't have same number of cells.");
        }
      } else if (targetRowKey >= 80 && targetRowKey < 90) {
        LOG.debug("Source cells: " + Arrays.toString(sourceCells));
        LOG.debug("Target cells: " + Arrays.toString(targetCells));
        Assert.fail("There should be no rows between 80 and 90 on target, as "
            + "these had different timestamps and should had been deleted.");
      } else if (targetRowKey >= 90 && targetRowKey < 100) {
        for (int j = 0; j < sourceCells.length; j++) {
          Cell sourceCell = sourceCells[j];
          Cell targetCell = targetCells[j];
          if (CellUtil.matchingValue(sourceCell, targetCell)) {
            Assert.fail("Cells values should not match for rows between "
                + "90 and 100. Target row id: " + (Bytes.toInt(targetRow
                .getRow())));
          }
        }
      } else {
        for (int j = 0; j < sourceCells.length; j++) {
          Cell sourceCell = sourceCells[j];
          Cell targetCell = targetCells[j];
          try {
            if (!CellUtil.matchingRow(sourceCell, targetCell)) {
              Assert.fail("Rows don't match");
            }
            if (!CellUtil.matchingFamily(sourceCell, targetCell)) {
              Assert.fail("Families don't match");
            }
            if (!CellUtil.matchingQualifier(sourceCell, targetCell)) {
              Assert.fail("Qualifiers don't match");
            }
            if (!CellUtil.matchingTimestamp(sourceCell, targetCell)) {
              Assert.fail("Timestamps don't match");
            }
            if (!CellUtil.matchingValue(sourceCell, targetCell)) {
              Assert.fail("Values don't match");
            }
          } catch (Throwable t) {
            LOG.debug(
                "Source cell: " + sourceCell + " target cell: " + targetCell);
            Throwables.propagate(t);
          }
        }
      }
      rowsCount++;
      targetRow = targetScanner.next();
      sourceRow = sourceScanner.next();
    }
    assertEquals("Target expected rows does not match.",expectedRows,
        rowsCount);
    sourceScanner.close();
    targetScanner.close();
    sourceTable.close();
    targetTable.close();
  }

  private Counters syncTables(TableName sourceTableName, TableName targetTableName,
      Path testDir, String... options) throws Exception {
    SyncTable syncTable = new SyncTable(TEST_UTIL.getConfiguration());
    String[] args = Arrays.copyOf(options, options.length+3);
    args[options.length] = testDir.toString();
    args[options.length+1] = sourceTableName.getNameAsString();
    args[options.length+2] = targetTableName.getNameAsString();
    int code = syncTable.run(args);
    assertEquals("sync table job failed", 0, code);

    LOG.info("Sync tables completed");
    return syncTable.counters;
  }

  private void hashSourceTable(TableName sourceTableName, Path testDir, String... options)
      throws Exception {
    int numHashFiles = 3;
    long batchSize = 100;  // should be 2 batches per region
    int scanBatch = 1;
    HashTable hashTable = new HashTable(TEST_UTIL.getConfiguration());
    String[] args = Arrays.copyOf(options, options.length+5);
    args[options.length] = "--batchsize=" + batchSize;
    args[options.length + 1] = "--numhashfiles=" + numHashFiles;
    args[options.length + 2] = "--scanbatch=" + scanBatch;
    args[options.length + 3] = sourceTableName.getNameAsString();
    args[options.length + 4] = testDir.toString();
    int code = hashTable.run(args);
    assertEquals("hash table job failed", 0, code);

    FileSystem fs = TEST_UTIL.getTestFileSystem();

    HashTable.TableHash tableHash = HashTable.TableHash.read(fs.getConf(), testDir);
    assertEquals(sourceTableName.getNameAsString(), tableHash.tableName);
    assertEquals(batchSize, tableHash.batchSize);
    assertEquals(numHashFiles, tableHash.numHashFiles);
    assertEquals(numHashFiles - 1, tableHash.partitions.size());

    LOG.info("Hash table completed");
  }

  private void writeTestData(TableName sourceTableName, TableName targetTableName,
      long... timestamps) throws Exception {
    final byte[] family = Bytes.toBytes("family");
    final byte[] column1 = Bytes.toBytes("c1");
    final byte[] column2 = Bytes.toBytes("c2");
    final byte[] value1 = Bytes.toBytes("val1");
    final byte[] value2 = Bytes.toBytes("val2");
    final byte[] value3 = Bytes.toBytes("val3");

    int numRows = 100;
    int sourceRegions = 10;
    int targetRegions = 6;
    if (ArrayUtils.isEmpty(timestamps)) {
      long current = System.currentTimeMillis();
      timestamps = new long[]{current,current};
    }

    Table sourceTable = TEST_UTIL.createTable(sourceTableName,
        family, generateSplits(numRows, sourceRegions));

    Table targetTable = TEST_UTIL.createTable(targetTableName,
        family, generateSplits(numRows, targetRegions));

    int rowIndex = 0;
    // a bunch of identical rows
    for (; rowIndex < 40; rowIndex++) {
      Put sourcePut = new Put(Bytes.toBytes(rowIndex));
      sourcePut.addColumn(family, column1, timestamps[0], value1);
      sourcePut.addColumn(family, column2, timestamps[0], value2);
      sourceTable.put(sourcePut);

      Put targetPut = new Put(Bytes.toBytes(rowIndex));
      targetPut.addColumn(family, column1, timestamps[1], value1);
      targetPut.addColumn(family, column2, timestamps[1], value2);
      targetTable.put(targetPut);
    }
    // some rows only in the source table
    // ROWSWITHDIFFS: 10
    // TARGETMISSINGROWS: 10
    // TARGETMISSINGCELLS: 20
    for (; rowIndex < 50; rowIndex++) {
      Put put = new Put(Bytes.toBytes(rowIndex));
      put.addColumn(family, column1, timestamps[0], value1);
      put.addColumn(family, column2, timestamps[0], value2);
      sourceTable.put(put);
    }
    // some rows only in the target table
    // ROWSWITHDIFFS: 10
    // SOURCEMISSINGROWS: 10
    // SOURCEMISSINGCELLS: 20
    for (; rowIndex < 60; rowIndex++) {
      Put put = new Put(Bytes.toBytes(rowIndex));
      put.addColumn(family, column1, timestamps[1], value1);
      put.addColumn(family, column2, timestamps[1], value2);
      targetTable.put(put);
    }
    // some rows with 1 missing cell in target table
    // ROWSWITHDIFFS: 10
    // TARGETMISSINGCELLS: 10
    for (; rowIndex < 70; rowIndex++) {
      Put sourcePut = new Put(Bytes.toBytes(rowIndex));
      sourcePut.addColumn(family, column1, timestamps[0], value1);
      sourcePut.addColumn(family, column2, timestamps[0], value2);
      sourceTable.put(sourcePut);

      Put targetPut = new Put(Bytes.toBytes(rowIndex));
      targetPut.addColumn(family, column1, timestamps[1], value1);
      targetTable.put(targetPut);
    }
    // some rows with 1 missing cell in source table
    // ROWSWITHDIFFS: 10
    // SOURCEMISSINGCELLS: 10
    for (; rowIndex < 80; rowIndex++) {
      Put sourcePut = new Put(Bytes.toBytes(rowIndex));
      sourcePut.addColumn(family, column1, timestamps[0], value1);
      sourceTable.put(sourcePut);

      Put targetPut = new Put(Bytes.toBytes(rowIndex));
      targetPut.addColumn(family, column1, timestamps[1], value1);
      targetPut.addColumn(family, column2, timestamps[1], value2);
      targetTable.put(targetPut);
    }
    // some rows differing only in timestamp
    // ROWSWITHDIFFS: 10
    // SOURCEMISSINGCELLS: 20
    // TARGETMISSINGCELLS: 20
    for (; rowIndex < 90; rowIndex++) {
      Put sourcePut = new Put(Bytes.toBytes(rowIndex));
      sourcePut.addColumn(family, column1, timestamps[0], column1);
      sourcePut.addColumn(family, column2, timestamps[0], value2);
      sourceTable.put(sourcePut);

      Put targetPut = new Put(Bytes.toBytes(rowIndex));
      targetPut.addColumn(family, column1, timestamps[1]+1, column1);
      targetPut.addColumn(family, column2, timestamps[1]-1, value2);
      targetTable.put(targetPut);
    }
    // some rows with different values
    // ROWSWITHDIFFS: 10
    // DIFFERENTCELLVALUES: 20
    for (; rowIndex < numRows; rowIndex++) {
      Put sourcePut = new Put(Bytes.toBytes(rowIndex));
      sourcePut.addColumn(family, column1, timestamps[0], value1);
      sourcePut.addColumn(family, column2, timestamps[0], value2);
      sourceTable.put(sourcePut);

      Put targetPut = new Put(Bytes.toBytes(rowIndex));
      targetPut.addColumn(family, column1, timestamps[1], value3);
      targetPut.addColumn(family, column2, timestamps[1], value3);
      targetTable.put(targetPut);
    }

    sourceTable.close();
    targetTable.close();
  }
}
