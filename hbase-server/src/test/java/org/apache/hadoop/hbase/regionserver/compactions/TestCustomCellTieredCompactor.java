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

import org.apache.hadoop.hbase.HBaseClassTestRule;

import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.regionserver.CustomCellTieredStoreEngine;
import org.apache.hadoop.hbase.regionserver.TimeRangeTracker;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import static org.apache.hadoop.hbase.regionserver.CustomTieringMultiFileWriter.TIERING_CELL_TIME_RANGE;
import static org.apache.hadoop.hbase.regionserver.compactions.CustomCellDateTieredCompactionPolicy.TIERING_CELL_QUALIFIER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

@Category({ RegionServerTests.class, SmallTests.class })
public class TestCustomCellTieredCompactor {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestCustomCellTieredCompactor.class);

  public static final byte[] FAMILY = Bytes.toBytes("cf");

  protected HBaseTestingUtil utility;

  protected Admin admin;

  @Before
  public void setUp() throws Exception {
    utility = new HBaseTestingUtil();
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
    clmBuilder.setValue("hbase.hstore.engine.class", CustomCellTieredStoreEngine.class.getName());
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
        Bytes.toBytes(recordTime - (11L*366L*24L*60L*60L*1000L)));
      puts.add(put);
      put = new Put(Bytes.toBytes(i+1000));
      put.addColumn(FAMILY, Bytes.toBytes("val"), Bytes.toBytes("v" + (i + 1000)));
      put.addColumn(FAMILY, Bytes.toBytes("date"),
        Bytes.toBytes(recordTime));
      puts.add(put);
      table.put(puts);
      utility.flush(tableName);
    }
    table.close();
    long firstCompactionTime = System.currentTimeMillis();
    utility.getAdmin().majorCompact(tableName);
    Waiter.waitFor(utility.getConfiguration(), 5000,
      () -> utility.getMiniHBaseCluster().getMaster()
        .getLastMajorCompactionTimestamp(tableName) > firstCompactionTime);
    long numHFiles = utility.getNumHFiles(tableName, FAMILY);
    //The first major compaction would have no means to detect more than one tier,
    // because without the min/max values available in the file info portion of the selected files
    // for compaction, CustomCellDateTieredCompactionPolicy has no means
    // to calculate the proper boundaries.
    assertEquals(1, numHFiles);
    utility.getMiniHBaseCluster().getRegions(tableName).get(0).getStore(FAMILY).getStorefiles().forEach(
      file -> {
        byte[] rangeBytes = file.getMetadataValue(TIERING_CELL_TIME_RANGE);
        assertNotNull(rangeBytes);
        try {
          TimeRangeTracker timeRangeTracker = TimeRangeTracker.parseFrom(rangeBytes);
          assertEquals((recordTime - (11L*366L*24L*60L*60L*1000L)), timeRangeTracker.getMin());
          assertEquals(recordTime, timeRangeTracker.getMax());
        } catch (IOException e) {
          fail(e.getMessage());
        }
      }
    );
    //now do major compaction again, to make sure we write two separate files
    long secondCompactionTime = System.currentTimeMillis();
    utility.getAdmin().majorCompact(tableName);
    Waiter.waitFor(utility.getConfiguration(), 5000,
      () -> utility.getMiniHBaseCluster().getMaster()
        .getLastMajorCompactionTimestamp(tableName) > secondCompactionTime);
    numHFiles = utility.getNumHFiles(tableName, FAMILY);
    assertEquals(2, numHFiles);
    utility.getMiniHBaseCluster().getRegions(tableName).get(0).getStore(FAMILY).getStorefiles().forEach(
      file -> {
        byte[] rangeBytes = file.getMetadataValue(TIERING_CELL_TIME_RANGE);
        assertNotNull(rangeBytes);
        try {
          TimeRangeTracker timeRangeTracker = TimeRangeTracker.parseFrom(rangeBytes);
          assertEquals(timeRangeTracker.getMin(), timeRangeTracker.getMax());
        } catch (IOException e) {
          fail(e.getMessage());
        }
      }
    );
  }
}
