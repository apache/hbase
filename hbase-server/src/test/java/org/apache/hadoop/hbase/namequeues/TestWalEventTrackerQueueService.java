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
import static org.apache.hadoop.hbase.namequeues.WALEventTrackerTableAccessor.WAL_EVENT_TRACKER_TABLE_NAME;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.regionserver.wal.WALEventTrackerListener;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

@Category(SmallTests.class)
public class TestWalEventTrackerQueueService {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestWalEventTrackerQueueService.class);

  @Rule
  public TestName name = new TestName();

  /*
   * Test whether wal event tracker metrics are being incremented.
   */
  @Test
  public void testMetrics() throws Exception {
    String rsName = "test-region-server";
    String walName = "test-wal-0";
    long timeStamp = EnvironmentEdgeManager.currentTime();
    String walState = WALEventTrackerListener.WalState.ACTIVE.name();
    long walLength = 100L;
    WALEventTrackerPayload payload =
      new WALEventTrackerPayload(rsName, walName, timeStamp, walState, walLength);
    Configuration conf = HBaseConfiguration.create();
    conf.setBoolean(WAL_EVENT_TRACKER_ENABLED_KEY, true);
    conf.setLong(WALEventTrackerTableAccessor.SLEEP_INTERVAL_KEY, 100);
    MetricsWALEventTrackerSourceImpl source = new MetricsWALEventTrackerSourceImpl(
      name.getMethodName(), name.getMethodName(), name.getMethodName(), name.getMethodName());
    WALEventTrackerQueueService service = new WALEventTrackerQueueService(conf, source);
    service.addToQueue(payload);
    Connection mockConnection = mock(Connection.class);
    doReturn(conf).when(mockConnection).getConfiguration();
    // Always throw IOException whenever mock connection is being used.
    doThrow(new IOException()).when(mockConnection).getTable(WAL_EVENT_TRACKER_TABLE_NAME);
    assertEquals(0L, source.getFailedPuts());
    assertEquals(0L, source.getNumRecordsFailedPuts());
    // Persist all the events.
    service.persistAll(mockConnection);
    assertEquals(1L, source.getFailedPuts());
    assertEquals(1L, source.getNumRecordsFailedPuts());
    // Verify that we tried MAX_RETRY_ATTEMPTS retry attempts to persist.
    verify(mockConnection, times(1 + WALEventTrackerTableAccessor.DEFAULT_MAX_ATTEMPTS))
      .getTable(WAL_EVENT_TRACKER_TABLE_NAME);
  }
}
