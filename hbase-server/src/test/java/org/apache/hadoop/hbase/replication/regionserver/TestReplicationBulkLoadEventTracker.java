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
package org.apache.hadoop.hbase.replication.regionserver;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.ReplicationTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(ReplicationTests.TAG)
@Tag(MediumTests.TAG)
public class TestReplicationBulkLoadEventTracker {

  private static final HBaseTestingUtil UTIL = new HBaseTestingUtil();
  private static final TableName TABLE = TableName.valueOf("testBulkLoadEventTracker");
  private static final byte[] REGION_NAME = Bytes.toBytes("region-1");
  private static final long WRITE_TIME = 123456789L;
  private static final long SEQ_NUM = 100L;

  private static ZKWatcher zkw1;
  private static ZKWatcher zkw2;

  @BeforeAll
  public static void setUpBeforeClass() throws Exception {
    UTIL.startMiniZKCluster();
    zkw1 = new ZKWatcher(UTIL.getConfiguration(), "tracker-1", null);
    zkw2 = new ZKWatcher(UTIL.getConfiguration(), "tracker-2", null);
  }

  @AfterAll
  public static void tearDownAfterClass() throws Exception {
    if (zkw2 != null) {
      zkw2.close();
    }
    if (zkw1 != null) {
      zkw1.close();
    }
    UTIL.shutdownMiniZKCluster();
  }

  @Test
  public void testSecondSinkWaitsForDoneFromFirstSink() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    conf.setLong(ZKReplicationBulkLoadEventTracker.WAIT_TIMEOUT_MS_KEY, 5000);
    conf.setLong(ZKReplicationBulkLoadEventTracker.WAIT_INTERVAL_MS_KEY, 50);

    ReplicationBulkLoadEventTracker tracker1 = new ZKReplicationBulkLoadEventTracker(conf, zkw1);
    ReplicationBulkLoadEventTracker tracker2 = new ZKReplicationBulkLoadEventTracker(conf, zkw2);
    ReplicationBulkLoadEventTracker.Event event =
      tracker1.newEvent("cluster-A", TABLE, REGION_NAME, SEQ_NUM, WRITE_TIME);

    assertEquals(ReplicationBulkLoadEventTracker.ClaimResult.CLAIMED, tracker1.claim(event));
    assertTrue(tracker1.isInProgress(event));
    assertFalse(tracker2.isDone(event));

    Thread marker = new Thread(() -> {
      try {
        Thread.sleep(200);
        tracker1.markDone(event);
        tracker1.release(event);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
    marker.start();
    assertEquals(ReplicationBulkLoadEventTracker.ClaimResult.COMPLETED, tracker2.claim(event));
    marker.join();

    assertFalse(tracker1.isInProgress(event));
    assertTrue(tracker2.isDone(event));
  }

  @Test
  public void testDoneMarkerUsesStableBucketFromWalWriteTime() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    conf.setLong(ZKReplicationBulkLoadEventTracker.BUCKET_WIDTH_MS_KEY, 1000);
    ReplicationBulkLoadEventTracker tracker = new ZKReplicationBulkLoadEventTracker(conf, zkw1);

    ReplicationBulkLoadEventTracker.Event first =
      tracker.newEvent("cluster-A", TABLE, REGION_NAME, SEQ_NUM, WRITE_TIME);
    ReplicationBulkLoadEventTracker.Event retry =
      tracker.newEvent("cluster-A", TABLE, REGION_NAME, SEQ_NUM, WRITE_TIME);

    assertEquals(first.getBucket(), retry.getBucket());
    assertEquals(first.getEventId(), retry.getEventId());
  }
}
