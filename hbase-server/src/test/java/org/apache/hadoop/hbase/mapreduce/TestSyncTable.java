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

import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CategoryBasedTimeout;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTable;
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
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestRule;

import com.google.common.base.Throwables;

/**
 * Basic test for the SyncTable M/R tool
 */
@Category(LargeTests.class)
public class TestSyncTable {
  @Rule public final TestRule timeout = CategoryBasedTimeout.builder().
      withTimeout(this.getClass()).withLookingForStuckThread(true).build();
  private static final Log LOG = LogFactory.getLog(TestSyncTable.class);
  
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();  
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    TEST_UTIL.startMiniCluster(3);
    TEST_UTIL.startMiniMapReduceCluster();
  }
  
  @AfterClass
  public static void afterClass() throws Exception {
    TEST_UTIL.shutdownMiniMapReduceCluster();
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
    String sourceTableName = "testSourceTable";
    String targetTableName = "testTargetTable";
    Path testDir = TEST_UTIL.getDataTestDirOnTestFS("testSyncTable");
    
    writeTestData(sourceTableName, targetTableName);
    hashSourceTable(sourceTableName, testDir);
    Counters syncCounters = syncTables(sourceTableName, targetTableName, testDir);
    assertEqualTables(90, sourceTableName, targetTableName);
    
    assertEquals(60, syncCounters.findCounter(Counter.ROWSWITHDIFFS).getValue());
    assertEquals(10, syncCounters.findCounter(Counter.SOURCEMISSINGROWS).getValue());
    assertEquals(10, syncCounters.findCounter(Counter.TARGETMISSINGROWS).getValue());
    assertEquals(50, syncCounters.findCounter(Counter.SOURCEMISSINGCELLS).getValue());
    assertEquals(50, syncCounters.findCounter(Counter.TARGETMISSINGCELLS).getValue());
    assertEquals(20, syncCounters.findCounter(Counter.DIFFERENTCELLVALUES).getValue());
    
    TEST_UTIL.deleteTable(sourceTableName);
    TEST_UTIL.deleteTable(targetTableName);
    TEST_UTIL.cleanupDataTestDirOnTestFS();
  }

  private void assertEqualTables(int expectedRows, String sourceTableName, String targetTableName) 
      throws Exception {
    Table sourceTable = TEST_UTIL.getConnection().getTable(TableName.valueOf(sourceTableName));
    Table targetTable = TEST_UTIL.getConnection().getTable(TableName.valueOf(targetTableName));
    
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

  private Counters syncTables(String sourceTableName, String targetTableName,
      Path testDir) throws Exception {
    SyncTable syncTable = new SyncTable(TEST_UTIL.getConfiguration());
    int code = syncTable.run(new String[] { 
        testDir.toString(),
        sourceTableName,
        targetTableName
        });
    assertEquals("sync table job failed", 0, code);
    
    LOG.info("Sync tables completed");
    return syncTable.counters;
  }

  private void hashSourceTable(String sourceTableName, Path testDir)
      throws Exception, IOException {
    int numHashFiles = 3;
    long batchSize = 100;  // should be 2 batches per region
    int scanBatch = 1;
    HashTable hashTable = new HashTable(TEST_UTIL.getConfiguration());
    int code = hashTable.run(new String[] { 
        "--batchsize=" + batchSize,
        "--numhashfiles=" + numHashFiles,
        "--scanbatch=" + scanBatch,
        sourceTableName,
        testDir.toString()});
    assertEquals("hash table job failed", 0, code);
    
    FileSystem fs = TEST_UTIL.getTestFileSystem();
    
    HashTable.TableHash tableHash = HashTable.TableHash.read(fs.getConf(), testDir);
    assertEquals(sourceTableName, tableHash.tableName);
    assertEquals(batchSize, tableHash.batchSize);
    assertEquals(numHashFiles, tableHash.numHashFiles);
    assertEquals(numHashFiles - 1, tableHash.partitions.size());

    LOG.info("Hash table completed");
  }

  private void writeTestData(String sourceTableName, String targetTableName)
      throws Exception {
    final byte[] family = Bytes.toBytes("family");
    final byte[] column1 = Bytes.toBytes("c1");
    final byte[] column2 = Bytes.toBytes("c2");
    final byte[] value1 = Bytes.toBytes("val1");
    final byte[] value2 = Bytes.toBytes("val2");
    final byte[] value3 = Bytes.toBytes("val3");
    
    int numRows = 100;
    int sourceRegions = 10;
    int targetRegions = 6;
    
    HTable sourceTable = TEST_UTIL.createTable(TableName.valueOf(sourceTableName),
        family, generateSplits(numRows, sourceRegions));

    HTable targetTable = TEST_UTIL.createTable(TableName.valueOf(targetTableName),
        family, generateSplits(numRows, targetRegions));

    long timestamp = 1430764183454L;

    int rowIndex = 0;
    // a bunch of identical rows
    for (; rowIndex < 40; rowIndex++) {
      Put sourcePut = new Put(Bytes.toBytes(rowIndex));
      sourcePut.addColumn(family, column1, timestamp, value1);
      sourcePut.addColumn(family, column2, timestamp, value2);
      sourceTable.put(sourcePut);
     
      Put targetPut = new Put(Bytes.toBytes(rowIndex));
      targetPut.addColumn(family, column1, timestamp, value1);
      targetPut.addColumn(family, column2, timestamp, value2);
      targetTable.put(targetPut);
    }
    // some rows only in the source table
    // ROWSWITHDIFFS: 10
    // TARGETMISSINGROWS: 10
    // TARGETMISSINGCELLS: 20
    for (; rowIndex < 50; rowIndex++) {
      Put put = new Put(Bytes.toBytes(rowIndex));
      put.addColumn(family, column1, timestamp, value1);
      put.addColumn(family, column2, timestamp, value2);
      sourceTable.put(put);
    }
    // some rows only in the target table
    // ROWSWITHDIFFS: 10
    // SOURCEMISSINGROWS: 10
    // SOURCEMISSINGCELLS: 20
    for (; rowIndex < 60; rowIndex++) {
      Put put = new Put(Bytes.toBytes(rowIndex));
      put.addColumn(family, column1, timestamp, value1);
      put.addColumn(family, column2, timestamp, value2);
      targetTable.put(put);
    }
    // some rows with 1 missing cell in target table
    // ROWSWITHDIFFS: 10
    // TARGETMISSINGCELLS: 10
    for (; rowIndex < 70; rowIndex++) {
      Put sourcePut = new Put(Bytes.toBytes(rowIndex));
      sourcePut.addColumn(family, column1, timestamp, value1);
      sourcePut.addColumn(family, column2, timestamp, value2);
      sourceTable.put(sourcePut);

      Put targetPut = new Put(Bytes.toBytes(rowIndex));
      targetPut.addColumn(family, column1, timestamp, value1);
      targetTable.put(targetPut);
    }
    // some rows with 1 missing cell in source table
    // ROWSWITHDIFFS: 10
    // SOURCEMISSINGCELLS: 10
    for (; rowIndex < 80; rowIndex++) {
      Put sourcePut = new Put(Bytes.toBytes(rowIndex));
      sourcePut.addColumn(family, column1, timestamp, value1);
      sourceTable.put(sourcePut);

      Put targetPut = new Put(Bytes.toBytes(rowIndex));
      targetPut.addColumn(family, column1, timestamp, value1);
      targetPut.addColumn(family, column2, timestamp, value2);
      targetTable.put(targetPut);
    }
    // some rows differing only in timestamp
    // ROWSWITHDIFFS: 10
    // SOURCEMISSINGCELLS: 20
    // TARGETMISSINGCELLS: 20
    for (; rowIndex < 90; rowIndex++) {
      Put sourcePut = new Put(Bytes.toBytes(rowIndex));
      sourcePut.addColumn(family, column1, timestamp, column1);
      sourcePut.addColumn(family, column2, timestamp, value2);
      sourceTable.put(sourcePut);

      Put targetPut = new Put(Bytes.toBytes(rowIndex));
      targetPut.addColumn(family, column1, timestamp+1, column1);
      targetPut.addColumn(family, column2, timestamp-1, value2);
      targetTable.put(targetPut);
    }
    // some rows with different values
    // ROWSWITHDIFFS: 10
    // DIFFERENTCELLVALUES: 20
    for (; rowIndex < numRows; rowIndex++) {
      Put sourcePut = new Put(Bytes.toBytes(rowIndex));
      sourcePut.addColumn(family, column1, timestamp, value1);
      sourcePut.addColumn(family, column2, timestamp, value2);
      sourceTable.put(sourcePut);
      
      Put targetPut = new Put(Bytes.toBytes(rowIndex));
      targetPut.addColumn(family, column1, timestamp, value3);
      targetPut.addColumn(family, column2, timestamp, value3);
      targetTable.put(targetPut);
    }
    
    sourceTable.close();
    targetTable.close();
  }
  

}
