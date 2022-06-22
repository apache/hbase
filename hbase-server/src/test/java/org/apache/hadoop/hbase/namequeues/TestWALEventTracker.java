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
package org.apache.hadoop.hbase.namequeues;

import static org.apache.hadoop.hbase.master.waleventtracker.WALEventTrackerTableCreator.WAL_EVENT_TRACKER_ENABLED_KEY;
import static org.apache.hadoop.hbase.namequeues.NamedQueueServiceChore.NAMED_QUEUE_CHORE_DURATION_KEY;
import static org.apache.hadoop.hbase.namequeues.WALEventTrackerTableAccessor.RS_COLUMN;
import static org.apache.hadoop.hbase.namequeues.WALEventTrackerTableAccessor.TIMESTAMP_COLUMN;
import static org.apache.hadoop.hbase.namequeues.WALEventTrackerTableAccessor.WAL_EVENT_TRACKER_TABLE_NAME;
import static org.apache.hadoop.hbase.namequeues.WALEventTrackerTableAccessor.WAL_LENGTH_COLUMN;
import static org.apache.hadoop.hbase.namequeues.WALEventTrackerTableAccessor.WAL_NAME_COLUMN;
import static org.apache.hadoop.hbase.namequeues.WALEventTrackerTableAccessor.WAL_STATE_COLUMN;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.regionserver.wal.AbstractFSWAL;
import org.apache.hadoop.hbase.regionserver.wal.WALEventTrackerListener;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WAL;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({ RegionServerTests.class, MediumTests.class })
public class TestWALEventTracker {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestWALEventTracker.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestNamedQueueRecorder.class);
  private static HBaseTestingUtil TEST_UTIL;
  public static Configuration CONF;

  @BeforeClass
  public static void setup() throws Exception {
    CONF = HBaseConfiguration.create();
    CONF.setBoolean(WAL_EVENT_TRACKER_ENABLED_KEY, true);
    // Set the chore for less than a second.
    CONF.setInt(NAMED_QUEUE_CHORE_DURATION_KEY, 900);
    CONF.setLong(WALEventTrackerTableAccessor.SLEEP_INTERVAL_KEY, 100);
    TEST_UTIL = new HBaseTestingUtil(CONF);
    TEST_UTIL.startMiniCluster();
  }

  @AfterClass
  public static void teardown() throws Exception {
    LOG.info("Calling teardown");
    TEST_UTIL.shutdownMiniHBaseCluster();
  }

  @Before
  public void waitForWalEventTrackerTableCreation() {
    Waiter.waitFor(CONF, 10000,
      (Waiter.Predicate) () -> TEST_UTIL.getAdmin().tableExists(WAL_EVENT_TRACKER_TABLE_NAME));
  }

  @Test
  public void testWALRolling() throws Exception {
    Connection connection = TEST_UTIL.getMiniHBaseCluster().getRegionServer(0).getConnection();
    waitForWALEventTrackerTable(connection);
    List<WAL> wals = TEST_UTIL.getMiniHBaseCluster().getRegionServer(0).getWALs();
    assertEquals(1, wals.size());
    AbstractFSWAL wal = (AbstractFSWAL) wals.get(0);
    Path wal1Path = wal.getOldPath();
    wal.rollWriter(true);

    FileSystem fs = TEST_UTIL.getTestFileSystem();
    long wal1Length = fs.getFileStatus(wal1Path).getLen();
    Path wal2Path = wal.getOldPath();
    String hostName =
      TEST_UTIL.getMiniHBaseCluster().getRegionServer(0).getServerName().getHostname();

    TEST_UTIL.waitFor(5000, () -> getTableCount(connection) >= 3);
    List<WALEventTrackerPayload> walEventsList = getRows(hostName, connection);

    // There should be atleast 2 events for wal1Name, with ROLLING and ROLLED state. Most of the
    // time we will lose ACTIVE event for the first wal creates since hmaster will take some time
    // to create hbase:waleventtracker table and by that time RS will already create the first wal
    // and will try to persist it.
    compareEvents(hostName, wal1Path.getName(), walEventsList,
      new ArrayList<>(Arrays.asList(WALEventTrackerListener.WalState.ROLLING.name(),
        WALEventTrackerListener.WalState.ROLLED.name())),
      false);

    // There should be only 1 event for wal2Name which is current wal, with ACTIVE state
    compareEvents(hostName, wal2Path.getName(), walEventsList,
      new ArrayList<>(Arrays.asList(WALEventTrackerListener.WalState.ACTIVE.name())), true);

    // Check that event with wal1Path and state ROLLED has the wal length set.
    checkWALRolledEventHasSize(walEventsList, wal1Path.getName(), wal1Length);
  }

  private void checkWALRolledEventHasSize(List<WALEventTrackerPayload> walEvents, String walName,
    long actualSize) {
    List<WALEventTrackerPayload> eventsFilteredByNameState = new ArrayList<>();
    // Filter the list by walName and wal state.
    for (WALEventTrackerPayload event : walEvents) {
      if (
        walName.equals(event.getWalName())
          && WALEventTrackerListener.WalState.ROLLED.name().equals(event.getState())
      ) {
        eventsFilteredByNameState.add(event);
      }
    }

    assertEquals(1, eventsFilteredByNameState.size());
    // We are not comparing the size of the WAL in the tracker table with actual size.
    // For AsyncWAL implementation, since the WAL file is closed in an async fashion, the WAL length
    // will always be incorrect.
    // For FSHLog implementation, we close the WAL in an executor thread. So there will always be
    // a difference of trailer size bytes.
    // assertEquals(actualSize, eventsFilteredByNameState.get(0).getWalLength());
  }

  /**
   * Compare the events from @{@link WALEventTrackerTableAccessor#WAL_EVENT_TRACKER_TABLE_NAME}
   * @param hostName       hostname
   * @param walName        walname
   * @param walEvents      event from table
   * @param expectedStates expected states for the hostname and wal name
   * @param strict         whether to check strictly or not. Sometimes we lose the ACTIVE state
   *                       event for the first wal since it takes some time for hmaster to create
   *                       the table and by that time RS already creates the first WAL and will try
   *                       to persist ACTIVE event to waleventtracker table.
   */
  private void compareEvents(String hostName, String walName,
    List<WALEventTrackerPayload> walEvents, List<String> expectedStates, boolean strict) {
    List<WALEventTrackerPayload> eventsFilteredByWalName = new ArrayList<>();

    // Assert that all the events have the same host name i.e they came from the same RS.
    for (WALEventTrackerPayload event : walEvents) {
      assertEquals(hostName, event.getRsName());
    }

    // Filter the list by walName.
    for (WALEventTrackerPayload event : walEvents) {
      if (walName.equals(event.getWalName())) {
        eventsFilteredByWalName.add(event);
      }
    }

    // Assert that the list of events after filtering by walName should be same as expected states.
    if (strict) {
      assertEquals(expectedStates.size(), eventsFilteredByWalName.size());
    }

    for (WALEventTrackerPayload event : eventsFilteredByWalName) {
      expectedStates.remove(event.getState());
    }
    assertEquals(0, expectedStates.size());
  }

  private void waitForWALEventTrackerTable(Connection connection) throws IOException {
    TEST_UTIL.waitFor(5000, () -> TEST_UTIL.getAdmin().tableExists(WAL_EVENT_TRACKER_TABLE_NAME));
  }

  private List<WALEventTrackerPayload> getRows(String rowKeyPrefix, Connection connection)
    throws IOException {
    List<WALEventTrackerPayload> list = new ArrayList<>();
    Scan scan = new Scan();
    scan.withStartRow(Bytes.toBytes(rowKeyPrefix));
    Table table = connection.getTable(WAL_EVENT_TRACKER_TABLE_NAME);
    ResultScanner scanner = table.getScanner(scan);

    Result r;
    while ((r = scanner.next()) != null) {
      List<Cell> cells = r.listCells();
      list.add(getPayload(cells));
    }
    return list;
  }

  private WALEventTrackerPayload getPayload(List<Cell> cells) {
    String rsName = null, walName = null, walState = null;
    long timestamp = 0L, walLength = 0L;
    for (Cell cell : cells) {
      byte[] qualifier = CellUtil.cloneQualifier(cell);
      byte[] value = CellUtil.cloneValue(cell);
      String qualifierStr = Bytes.toString(qualifier);

      if (RS_COLUMN.equals(qualifierStr)) {
        rsName = Bytes.toString(value);
      } else if (WAL_NAME_COLUMN.equals(qualifierStr)) {
        walName = Bytes.toString(value);
      } else if (WAL_STATE_COLUMN.equals(qualifierStr)) {
        walState = Bytes.toString(value);
      } else if (TIMESTAMP_COLUMN.equals(qualifierStr)) {
        timestamp = Bytes.toLong(value);
      } else if (WAL_LENGTH_COLUMN.equals(qualifierStr)) {
        walLength = Bytes.toLong(value);
      }
    }
    return new WALEventTrackerPayload(rsName, walName, timestamp, walState, walLength);
  }

  private int getTableCount(Connection connection) throws Exception {
    Table table = connection.getTable(WAL_EVENT_TRACKER_TABLE_NAME);
    ResultScanner resultScanner = table.getScanner(new Scan().setReadType(Scan.ReadType.STREAM));
    int count = 0;
    while (resultScanner.next() != null) {
      count++;
    }
    LOG.info("Table count: " + count);
    return count;
  }
}
