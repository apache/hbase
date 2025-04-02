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
package org.apache.hadoop.hbase.coprocessor.example.row.stats;

import static org.apache.hadoop.hbase.util.TestRegionSplitCalculator.TEST_UTIL;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.SingleProcessHBaseCluster;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.example.row.stats.recorder.RowStatisticsRecorder;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestRowStatisticsCompactionObserver {

  public static final TestableRowStatisticsRecorder RECORDER = new TestableRowStatisticsRecorder();
  private static final TableName TABLE_NAME = TableName.valueOf("test-table");
  private static final byte[] FAMILY = Bytes.toBytes("0");
  private static SingleProcessHBaseCluster cluster;
  private static Connection connection;
  private static Table table;

  @BeforeClass
  public static void setUpClass() throws Exception {
    cluster = TEST_UTIL.startMiniCluster(1);
    connection = ConnectionFactory.createConnection(cluster.getConf());
    table = TEST_UTIL.createTable(TABLE_NAME, new byte[][] { FAMILY }, 1,
      HConstants.DEFAULT_BLOCKSIZE, TestableRowStatisticsCompactionObserver.class.getName());
  }

  @Before
  public void setUp() throws Exception {
    RECORDER.clear();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    cluster.close();
    TEST_UTIL.shutdownMiniCluster();
    table.close();
    connection.close();
  }

  @Test
  public void itRecordsStats() throws IOException, InterruptedException {
    int numRows = 10;
    int largestRowNum = -1;
    int largestRowSize = 0;

    int largestCellRowNum = -1;
    int largestCellColNum = -1;
    long largestCellSize = 0;

    for (int i = 0; i < numRows; i++) {
      int cells = ThreadLocalRandom.current().nextInt(1000) + 10;

      Put p = new Put(Bytes.toBytes(i));
      for (int j = 0; j < cells; j++) {
        byte[] val = new byte[ThreadLocalRandom.current().nextInt(100) + 1];
        p.addColumn(FAMILY, Bytes.toBytes(j), val);
      }

      int rowSize = 0;
      CellScanner cellScanner = p.cellScanner();
      int j = 0;
      while (cellScanner.advance()) {
        Cell current = cellScanner.current();
        int serializedSize = current.getSerializedSize();
        if (serializedSize > largestCellSize) {
          largestCellSize = serializedSize;
          largestCellRowNum = i;
          largestCellColNum = j;
        }
        rowSize += serializedSize;
        j++;
      }

      if (rowSize > largestRowSize) {
        largestRowNum = i;
        largestRowSize = rowSize;
      }

      table.put(p);
      connection.getAdmin().flush(table.getName());
    }

    for (int i = 0; i < numRows; i++) {
      Delete d = new Delete(Bytes.toBytes(i));
      d.addColumn(FAMILY, Bytes.toBytes(0));
      table.delete(d);
    }

    System.out.println("Final flush");
    connection.getAdmin().flush(table.getName());
    Thread.sleep(5000);
    System.out.println("Compacting");

    RowStatisticsImpl lastStats = RECORDER.getLastStats(); // Just initialize
    Boolean lastIsMajor = RECORDER.getLastIsMajor();
    connection.getAdmin().compact(table.getName());
    while (lastStats == null) {
      Thread.sleep(1000);

      System.out.println("Checking stats");
      lastStats = RECORDER.getLastStats();
      lastIsMajor = RECORDER.getLastIsMajor();
    }
    assertFalse(lastIsMajor);
    assertEquals(lastStats.getTotalDeletesCount(), 10);
    assertEquals(lastStats.getTotalRowsCount(), 10);

    RECORDER.clear();
    lastStats = RECORDER.getLastStats();
    lastIsMajor = RECORDER.getLastIsMajor();
    connection.getAdmin().majorCompact(table.getName());

    // Must wait for async majorCompaction to complete
    while (lastStats == null) {
      Thread.sleep(1000);

      System.out.println("Checking stats");
      lastStats = RECORDER.getLastStats();
      lastIsMajor = RECORDER.getLastIsMajor();
    }
    assertTrue(lastIsMajor);
    // no deletes after major compact
    assertEquals(lastStats.getTotalDeletesCount(), 0);
    assertEquals(lastStats.getTotalRowsCount(), 10);
    // can only check largest values after major compact, since the above minor compact might not
    // contain all storefiles
    assertEquals(Bytes.toInt(lastStats.getLargestRow()), largestRowNum);
    assertEquals(
      Bytes.toInt(lastStats.getLargestCell().getRowArray(),
        lastStats.getLargestCell().getRowOffset(), lastStats.getLargestCell().getRowLength()),
      largestCellRowNum);
    assertEquals(Bytes.toInt(lastStats.getLargestCell().getQualifierArray(),
      lastStats.getLargestCell().getQualifierOffset(),
      lastStats.getLargestCell().getQualifierLength()), largestCellColNum);
  }

  public static class TestableRowStatisticsCompactionObserver
    extends RowStatisticsCompactionObserver {

    public TestableRowStatisticsCompactionObserver() {
      super(TestRowStatisticsCompactionObserver.RECORDER);
    }
  }

  public static class TestableRowStatisticsRecorder implements RowStatisticsRecorder {

    private volatile RowStatisticsImpl lastStats = null;
    private volatile Boolean lastIsMajor = null;

    @Override
    public void record(RowStatisticsImpl stats, Optional<byte[]> fullRegionName) {
      System.out.println("Record called with isMajor=" + stats.isMajor() + ", stats=" + stats
        + ", fullRegionName=" + fullRegionName);
      lastStats = stats;
    }

    public void clear() {
      lastStats = null;
      lastIsMajor = null;
    }

    public RowStatisticsImpl getLastStats() {
      return lastStats;
    }

    public Boolean getLastIsMajor() {
      return lastIsMajor;
    }
  }
}
