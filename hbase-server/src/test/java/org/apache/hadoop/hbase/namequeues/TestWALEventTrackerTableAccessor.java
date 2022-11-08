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

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.regionserver.wal.WALEventTrackerListener;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestWALEventTrackerTableAccessor {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestWALEventTrackerTableAccessor.class);

  /*
   * Tests that rowkey is getting constructed correctly.
   */
  @Test
  public void testRowKey() {
    String rsName = "test-region-server";
    String walName = "test-wal-0";
    long timeStamp = EnvironmentEdgeManager.currentTime();
    String walState = WALEventTrackerListener.WalState.ACTIVE.name();
    long walLength = 100L;
    WALEventTrackerPayload payload =
      new WALEventTrackerPayload(rsName, walName, timeStamp, walState, walLength);
    byte[] rowKeyBytes = WALEventTrackerTableAccessor.getRowKey(payload);

    String rowKeyBytesStr = Bytes.toString(rowKeyBytes);
    String[] fields = rowKeyBytesStr.split(WALEventTrackerTableAccessor.DELIMITER, -1);
    // This is the format of rowkey: walName_timestamp_walState;
    assertEquals(walName, fields[0]);
    assertEquals(timeStamp, Long.valueOf(fields[1]).longValue());
    assertEquals(walState, fields[2]);
  }
}
