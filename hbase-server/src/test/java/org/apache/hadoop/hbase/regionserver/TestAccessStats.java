/**
 *
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

package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.stats.AccessStats;
import org.apache.hadoop.hbase.regionserver.stats.AccessStats.AccessStatsGranularity;
import org.apache.hadoop.hbase.regionserver.stats.AccessStats.AccessStatsType;
import org.apache.hadoop.hbase.regionserver.stats.AccessStatsRecorderConstants;
import org.apache.hadoop.hbase.regionserver.stats.AccessStatsRecorderTableImpl;
import org.apache.hadoop.hbase.regionserver.stats.AccessStatsRecorderUtils;
import org.apache.hadoop.hbase.regionserver.stats.IAccessStatsRecorder;
import org.apache.hadoop.hbase.regionserver.stats.RegionAccessStats;
import org.apache.hadoop.hbase.regionserver.stats.UidGenerator;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;;

@Category(LargeTests.class)
public class TestAccessStats {
  private static Configuration conf = HBaseConfiguration.create();
  private static HBaseTestingUtility utility;
  private static UidGenerator uidGenerator;
  private static int iterationDurationInMinutes = 1;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    conf.setInt(AccessStatsRecorderConstants.REGION_STATS_RECORDER_IN_MINUTES_KEY,
      iterationDurationInMinutes);

    utility = new HBaseTestingUtility(conf);
    utility.startMiniCluster();

    AccessStatsRecorderUtils.createInstance(conf);
    AccessStatsRecorderUtils.getInstance().createTableToStoreStats();

    uidGenerator = new UidGenerator(conf);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    uidGenerator.close();

    utility.shutdownMiniCluster();
  }
  
  @Test
  public void testStatsRecordWriteAndGet() throws Exception {
    TableName testTable = TableName.valueOf("TestTable");
    try (IAccessStatsRecorder accessStatsRecorder = new AccessStatsRecorderTableImpl(conf)) {
      long epochTime = AccessStatsRecorderUtils.getInstance().getNormalizedTimeCurrent();
      Random random = new Random();
      List<AccessStats> accessStatsList = new ArrayList<>();

      String regionName = "Region 1";

      long count = random.nextInt(99);
      if (count < 0) count = count * -1;

      RegionAccessStats regionAccessStats =
          new RegionAccessStats(testTable, AccessStatsType.READCOUNT, Bytes.toBytes("StartKey"),
              Bytes.toBytes("EndKey"), epochTime, count);
      regionAccessStats.setRegionName(regionName);
      accessStatsList.add(regionAccessStats);

      accessStatsRecorder.writeAccessStats(accessStatsList);

      accessStatsList = accessStatsRecorder.readAccessStats(testTable, AccessStatsType.READCOUNT,
        AccessStatsGranularity.REGION, Instant.now().toEpochMilli(), 10);

      Assert.assertEquals("AccessStatsList size is not as expected", 1, accessStatsList.size());
      for (AccessStats accessStats : accessStatsList) {
        regionAccessStats = (RegionAccessStats) accessStats;
        Assert.assertEquals("AccessStats type is not as expected", AccessStatsType.READCOUNT.toString(),
          Bytes.toString(regionAccessStats.getAccessStatsType()));
        Assert.assertEquals("AccessStats value is not as expected", count,
          regionAccessStats.getValue());
        Assert.assertEquals("AccessStats regionName is not as expected", regionName,
          regionAccessStats.getRegionName());
        Assert.assertEquals("AccessStats StartKey is not as expected", "StartKey",
          Bytes.toString(regionAccessStats.getKeyRangeStart()));
        Assert.assertEquals("AccessStats EndKey is not as expected", "EndKey",
          Bytes.toString(regionAccessStats.getKeyRangeEnd()));
      }
    }
  }

  @Test
  public void testUIDGeneration() throws Exception {
    generateAndVerifyUIDs("String1", 1);
    generateAndVerifyUIDs("String2", 1);

    generateAndVerifyUIDs("String1", 2);

    generateAndVerifyUIDs("String2", 3);
    generateAndVerifyUIDs("String3", 3);
    generateAndVerifyUIDs("String4", 3);

    generateAndVerifyUIDs("String2", 4);

    generateAndVerifyUIDs("String5", 1);
  }

  @Test
  public void testStatsGeneration() {
    try {
      Connection connection = ConnectionFactory.createConnection(conf);

      TableName tableName = TableName.valueOf("TestTable");

      HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
      HColumnDescriptor columnDescriptor = new HColumnDescriptor(Bytes.toBytes("CF"));
      tableDescriptor.addFamily(columnDescriptor);
      Admin admin = connection.getAdmin();
      admin.createTable(tableDescriptor);
      admin.close();

      HTable table = (HTable) connection.getTable(tableName);

      sleep();

      int readOperation = 2;
      int writeOperation = 3;

      perforReadAndWriteOperations(table, readOperation, writeOperation);

      sleep();

      int[] expectedReadCountArray1 = { readOperation };
      int[] expectedWriteCountArray1 = { writeOperation };

      vallidateStatsStored(tableName, expectedReadCountArray1, AccessStatsType.READCOUNT, 1);
      vallidateStatsStored(tableName, expectedWriteCountArray1, AccessStatsType.WRITECOUNT, 1);

      readOperation = 7;
      writeOperation = 10;

      perforReadAndWriteOperations(table, readOperation, writeOperation);

      sleep();

      int[] expectedReadCountArray2 = { readOperation, 2 };
      int[] expectedWriteCountArray2 = { writeOperation, 3 };

      vallidateStatsStored(tableName, expectedReadCountArray2, AccessStatsType.READCOUNT, 2);
      vallidateStatsStored(tableName, expectedWriteCountArray2, AccessStatsType.WRITECOUNT, 2);

    } catch (Exception e) {
      Assert.fail("Failed with exception message -"+ e.getMessage());
    }
  }

  private void vallidateStatsStored(TableName tableName, int[] expectedCounts,
      AccessStatsType accessStatsType, int iteration) throws IOException {
    try (IAccessStatsRecorder accessStatsRecorder = new AccessStatsRecorderTableImpl(conf)) {
      List<AccessStats> accessStatsList = accessStatsRecorder.readAccessStats(tableName,
        accessStatsType, AccessStatsGranularity.REGION, Instant.now().toEpochMilli(), iteration);

      int i = 0;
      for (AccessStats accessStats : accessStatsList) {
        RegionAccessStats regionAccessStats = (RegionAccessStats) accessStats;
        long value = regionAccessStats.getValue();

        Assert.assertEquals("Stats is not as expected.", expectedCounts[i++], value);
      }
    }
  }

  private void perforReadAndWriteOperations(HTable table, int readOperation, int writeOperation)
      throws IOException {
    for (int i = 0; i < readOperation; i++) {
      performReadOperation(table);
    }

    for (int i = 0; i < writeOperation; i++) {
      performWriteOperation(table);
    }
  }

  private void sleep() throws InterruptedException {
    Thread.sleep(iterationDurationInMinutes * 60 * 1000 + 10 * 1000);
  }

  private void performWriteOperation(HTable table) throws IOException {
    long now = Instant.now().toEpochMilli();

    Put put = new Put(Bytes.toBytes(now));
    put.addColumn(Bytes.toBytes("CF"), Bytes.toBytes("C"), Bytes.toBytes(now));

    table.put(put);
  }

  private void performReadOperation(HTable table) throws IOException {
    table.get(new Get(Bytes.toBytes(Instant.now().toEpochMilli())));
  }

  private void generateAndVerifyUIDs(String str, int uidLengthInBytes)
      throws Exception, InterruptedException {
    byte[] uidByteArray = uidGenerator.getUIDForString(str, uidLengthInBytes);
    String strEquivalentUid = uidGenerator.getStringForUID(uidByteArray);

    Assert.assertEquals("Generated UID byte array is not of expected length", uidLengthInBytes,
      uidByteArray.length);
    Assert.assertEquals("String and corresponding UID equivalent are not same", str,
      strEquivalentUid);

    byte[] uidByteArraySecond = uidGenerator.getUIDForString(str, uidLengthInBytes);
    Assert.assertEquals("Different UID has been returned for same string",
      Bytes.toString(uidByteArray), Bytes.toString(uidByteArraySecond));
  }
}