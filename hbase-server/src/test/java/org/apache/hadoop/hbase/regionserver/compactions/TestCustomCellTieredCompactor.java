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
package org.apache.hadoop.hbase.regionserver.compactions;

import static org.apache.hadoop.hbase.regionserver.CustomTieringMultiFileWriter.CUSTOM_TIERING_TIME_RANGE;
import static org.apache.hadoop.hbase.regionserver.compactions.CustomCellTieringValueProvider.TIERING_CELL_QUALIFIER;
import static org.apache.hadoop.hbase.regionserver.compactions.CustomTieredCompactor.TIERING_VALUE_PROVIDER;
import static org.apache.hadoop.hbase.regionserver.compactions.RowKeyDateTieringValueProvider.TIERING_KEY_DATE_FORMAT;
import static org.apache.hadoop.hbase.regionserver.compactions.RowKeyDateTieringValueProvider.TIERING_KEY_DATE_PATTERN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.regionserver.CustomTieredStoreEngine;
import org.apache.hadoop.hbase.regionserver.TimeRangeTracker;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ RegionServerTests.class, SmallTests.class })
public class TestCustomCellTieredCompactor {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestCustomCellTieredCompactor.class);

  public static final byte[] FAMILY = Bytes.toBytes("cf");

  protected HBaseTestingUtility utility;

  protected Admin admin;

  @Before
  public void setUp() throws Exception {
    utility = new HBaseTestingUtility();
    utility.getConfiguration().setInt("hbase.hfile.compaction.discharger.interval", 10);
    utility.startMiniCluster();
  }

  @After
  public void tearDown() throws Exception {
    utility.shutdownMiniCluster();
  }

  @Test
  public void testCustomCellTieredCompactor() throws Exception {
    ColumnFamilyDescriptorBuilder clmBuilder = ColumnFamilyDescriptorBuilder.newBuilder(FAMILY);
    clmBuilder.setValue("hbase.hstore.engine.class", CustomTieredStoreEngine.class.getName());
    clmBuilder.setValue(TIERING_CELL_QUALIFIER, "date");
    TableName tableName = TableName.valueOf("testCustomCellTieredCompactor");
    TableDescriptorBuilder tblBuilder = TableDescriptorBuilder.newBuilder(tableName);
    tblBuilder.setColumnFamily(clmBuilder.build());
    utility.getAdmin().createTable(tblBuilder.build());
    utility.waitTableAvailable(tableName);
    Connection connection = utility.getConnection();
    Table table = connection.getTable(tableName);
    long recordTime = System.currentTimeMillis();
    // write data and flush multiple store files:
    for (int i = 0; i < 6; i++) {
      List<Put> puts = new ArrayList<>(2);
      Put put = new Put(Bytes.toBytes(i));
      put.addColumn(FAMILY, Bytes.toBytes("val"), Bytes.toBytes("v" + i));
      put.addColumn(FAMILY, Bytes.toBytes("date"),
        Bytes.toBytes(recordTime - (11L * 366L * 24L * 60L * 60L * 1000L)));
      puts.add(put);
      put = new Put(Bytes.toBytes(i + 1000));
      put.addColumn(FAMILY, Bytes.toBytes("val"), Bytes.toBytes("v" + (i + 1000)));
      put.addColumn(FAMILY, Bytes.toBytes("date"), Bytes.toBytes(recordTime));
      puts.add(put);
      table.put(puts);
      utility.flush(tableName);
    }
    table.close();
    long firstCompactionTime = System.currentTimeMillis();
    utility.getAdmin().majorCompact(tableName);
    Waiter.waitFor(utility.getConfiguration(), 5000,
      () -> utility.getMiniHBaseCluster().getMaster().getLastMajorCompactionTimestamp(tableName)
          > firstCompactionTime);
    long numHFiles = utility.getNumHFiles(tableName, FAMILY);
    // The first major compaction would have no means to detect more than one tier,
    // because without the min/max values available in the file info portion of the selected files
    // for compaction, CustomCellDateTieredCompactionPolicy has no means
    // to calculate the proper boundaries.
    assertEquals(1, numHFiles);
    utility.getMiniHBaseCluster().getRegions(tableName).get(0).getStore(FAMILY).getStorefiles()
      .forEach(file -> {
        byte[] rangeBytes = file.getMetadataValue(CUSTOM_TIERING_TIME_RANGE);
        assertNotNull(rangeBytes);
        try {
          TimeRangeTracker timeRangeTracker = TimeRangeTracker.parseFrom(rangeBytes);
          assertEquals((recordTime - (11L * 366L * 24L * 60L * 60L * 1000L)),
            timeRangeTracker.getMin());
          assertEquals(recordTime, timeRangeTracker.getMax());
        } catch (IOException e) {
          fail(e.getMessage());
        }
      });
    // now do major compaction again, to make sure we write two separate files
    long secondCompactionTime = System.currentTimeMillis();
    utility.getAdmin().majorCompact(tableName);
    Waiter.waitFor(utility.getConfiguration(), 5000,
      () -> utility.getMiniHBaseCluster().getMaster().getLastMajorCompactionTimestamp(tableName)
          > secondCompactionTime);
    numHFiles = utility.getNumHFiles(tableName, FAMILY);
    assertEquals(2, numHFiles);
    utility.getMiniHBaseCluster().getRegions(tableName).get(0).getStore(FAMILY).getStorefiles()
      .forEach(file -> {
        byte[] rangeBytes = file.getMetadataValue(CUSTOM_TIERING_TIME_RANGE);
        assertNotNull(rangeBytes);
        try {
          TimeRangeTracker timeRangeTracker = TimeRangeTracker.parseFrom(rangeBytes);
          assertEquals(timeRangeTracker.getMin(), timeRangeTracker.getMax());
        } catch (IOException e) {
          fail(e.getMessage());
        }
      });
  }

  @Test
  public void testCustomCellTieredCompactorWithRowKeyDateTieringValue() throws Exception {
    // Restart mini cluster with RowKeyDateTieringValueProvider
    utility.shutdownMiniCluster();
    utility.getConfiguration().set(TIERING_VALUE_PROVIDER,
      RowKeyDateTieringValueProvider.class.getName());
    utility.startMiniCluster();

    ColumnFamilyDescriptorBuilder clmBuilder = ColumnFamilyDescriptorBuilder.newBuilder(FAMILY);
    clmBuilder.setValue("hbase.hstore.engine.class", CustomTieredStoreEngine.class.getName());

    // Table 1: Date at end with format yyyyMMddHHmmssSSS
    TableName table1Name = TableName.valueOf("testTable1");
    TableDescriptorBuilder tbl1Builder = TableDescriptorBuilder.newBuilder(table1Name);
    tbl1Builder.setColumnFamily(clmBuilder.build());
    tbl1Builder.setValue(TIERING_KEY_DATE_PATTERN, "(\\d{17})$");
    tbl1Builder.setValue(TIERING_KEY_DATE_FORMAT, "yyyyMMddHHmmssSSS");
    utility.getAdmin().createTable(tbl1Builder.build());
    utility.waitTableAvailable(table1Name);

    // Table 2: Date at beginning with format yyyy-MM-dd HH:mm:ss
    TableName table2Name = TableName.valueOf("testTable2");
    TableDescriptorBuilder tbl2Builder = TableDescriptorBuilder.newBuilder(table2Name);
    tbl2Builder.setColumnFamily(clmBuilder.build());
    tbl2Builder.setValue(TIERING_KEY_DATE_PATTERN, "^(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2})");
    tbl2Builder.setValue(TIERING_KEY_DATE_FORMAT, "yyyy-MM-dd HH:mm:ss");
    utility.getAdmin().createTable(tbl2Builder.build());
    utility.waitTableAvailable(table2Name);

    Connection connection = utility.getConnection();
    long recordTime = System.currentTimeMillis();
    long oldTime = recordTime - (11L * 366L * 24L * 60L * 60L * 1000L);

    SimpleDateFormat sdf1 = new SimpleDateFormat("yyyyMMddHHmmssSSS");
    SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    // Write to Table 1 with date at end
    Table table1 = connection.getTable(table1Name);
    for (int i = 0; i < 6; i++) {
      List<Put> puts = new ArrayList<>(2);

      // Old data
      String oldDate = sdf1.format(new Date(oldTime));
      Put put = new Put(Bytes.toBytes("row_" + i + "_" + oldDate));
      put.addColumn(FAMILY, Bytes.toBytes("val"), Bytes.toBytes("v" + i));
      puts.add(put);

      // Recent data
      String recentDate = sdf1.format(new Date(recordTime));
      put = new Put(Bytes.toBytes("row_" + (i + 1000) + "_" + recentDate));
      put.addColumn(FAMILY, Bytes.toBytes("val"), Bytes.toBytes("v" + (i + 1000)));
      puts.add(put);

      table1.put(puts);
      utility.flush(table1Name);
    }
    table1.close();

    // Write to Table 2 with date at beginning
    Table table2 = connection.getTable(table2Name);
    for (int i = 0; i < 6; i++) {
      List<Put> puts = new ArrayList<>(2);

      // Old data
      String oldDate = sdf2.format(new Date(oldTime));
      Put put = new Put(Bytes.toBytes(oldDate + "_row_" + i));
      put.addColumn(FAMILY, Bytes.toBytes("val"), Bytes.toBytes("v" + i));
      puts.add(put);

      // Recent data
      String recentDate = sdf2.format(new Date(recordTime));
      put = new Put(Bytes.toBytes(recentDate + "_row_" + (i + 1000)));
      put.addColumn(FAMILY, Bytes.toBytes("val"), Bytes.toBytes("v" + (i + 1000)));
      puts.add(put);

      table2.put(puts);
      utility.flush(table2Name);
    }
    table2.close();

    // First compaction for Table 1
    long compactionTime1 = System.currentTimeMillis();
    utility.getAdmin().majorCompact(table1Name);
    Waiter.waitFor(utility.getConfiguration(), 5000,
      () -> utility.getMiniHBaseCluster().getMaster().getLastMajorCompactionTimestamp(table1Name)
          > compactionTime1);

    assertEquals(1, utility.getNumHFiles(table1Name, FAMILY));

    utility.getMiniHBaseCluster().getRegions(table1Name).get(0).getStore(FAMILY).getStorefiles()
      .forEach(file -> {
        byte[] rangeBytes = file.getMetadataValue(CUSTOM_TIERING_TIME_RANGE);
        assertNotNull(rangeBytes);
        try {
          TimeRangeTracker timeRangeTracker = TimeRangeTracker.parseFrom(rangeBytes);
          assertEquals(oldTime, timeRangeTracker.getMin());
          assertEquals(recordTime, timeRangeTracker.getMax());
        } catch (IOException e) {
          fail(e.getMessage());
        }
      });

    // Second compaction for Table 1
    long secondCompactionTime1 = System.currentTimeMillis();
    utility.getAdmin().majorCompact(table1Name);
    Waiter.waitFor(utility.getConfiguration(), 5000,
      () -> utility.getMiniHBaseCluster().getMaster().getLastMajorCompactionTimestamp(table1Name)
          > secondCompactionTime1);

    assertEquals(2, utility.getNumHFiles(table1Name, FAMILY));

    utility.getMiniHBaseCluster().getRegions(table1Name).get(0).getStore(FAMILY).getStorefiles()
      .forEach(file -> {
        byte[] rangeBytes = file.getMetadataValue(CUSTOM_TIERING_TIME_RANGE);
        assertNotNull(rangeBytes);
        try {
          TimeRangeTracker timeRangeTracker = TimeRangeTracker.parseFrom(rangeBytes);
          assertEquals(timeRangeTracker.getMin(), timeRangeTracker.getMax());
        } catch (IOException e) {
          fail(e.getMessage());
        }
      });

    // First compaction for Table 2
    long compactionTime2 = System.currentTimeMillis();
    utility.getAdmin().majorCompact(table2Name);
    Waiter.waitFor(utility.getConfiguration(), 5000,
      () -> utility.getMiniHBaseCluster().getMaster().getLastMajorCompactionTimestamp(table2Name)
          > compactionTime2);

    assertEquals(1, utility.getNumHFiles(table2Name, FAMILY));

    utility.getMiniHBaseCluster().getRegions(table2Name).get(0).getStore(FAMILY).getStorefiles()
      .forEach(file -> {
        byte[] rangeBytes = file.getMetadataValue(CUSTOM_TIERING_TIME_RANGE);
        assertNotNull(rangeBytes);
        try {
          TimeRangeTracker timeRangeTracker = TimeRangeTracker.parseFrom(rangeBytes);
          // Table 2 uses yyyy-MM-dd HH:mm:ss format, so we need to account for second precision
          // The parsed time will be truncated to second precision (no milliseconds)
          long expectedOldTime = (oldTime / 1000) * 1000;
          long expectedRecentTime = (recordTime / 1000) * 1000;
          assertEquals(expectedOldTime, timeRangeTracker.getMin());
          assertEquals(expectedRecentTime, timeRangeTracker.getMax());
        } catch (IOException e) {
          fail(e.getMessage());
        }
      });

    // Second compaction for Table 2
    long secondCompactionTime2 = System.currentTimeMillis();
    utility.getAdmin().majorCompact(table2Name);
    Waiter.waitFor(utility.getConfiguration(), 5000,
      () -> utility.getMiniHBaseCluster().getMaster().getLastMajorCompactionTimestamp(table2Name)
          > secondCompactionTime2);

    assertEquals(2, utility.getNumHFiles(table2Name, FAMILY));

    utility.getMiniHBaseCluster().getRegions(table2Name).get(0).getStore(FAMILY).getStorefiles()
      .forEach(file -> {
        byte[] rangeBytes = file.getMetadataValue(CUSTOM_TIERING_TIME_RANGE);
        assertNotNull(rangeBytes);
        try {
          TimeRangeTracker timeRangeTracker = TimeRangeTracker.parseFrom(rangeBytes);
          assertEquals(timeRangeTracker.getMin(), timeRangeTracker.getMax());
        } catch (IOException e) {
          fail(e.getMessage());
        }
      });
  }
}
