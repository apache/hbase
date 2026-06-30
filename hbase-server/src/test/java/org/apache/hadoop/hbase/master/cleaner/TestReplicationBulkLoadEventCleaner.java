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
package org.apache.hadoop.hbase.master.cleaner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.replication.regionserver.ReplicationBulkLoadEventTracker;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.ReplicationTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.hadoop.hbase.zookeeper.ZNodePaths;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(ReplicationTests.TAG)
@Tag(MediumTests.TAG)
public class TestReplicationBulkLoadEventCleaner {

  private static final HBaseTestingUtil UTIL = new HBaseTestingUtil();
  private static final TableName TABLE = TableName.valueOf("testBulkLoadEventCleaner");
  private static final byte[] REGION_NAME = Bytes.toBytes("region-1");
  private static final long WRITE_TIME = 123456789L;
  private static final long SEQ_NUM = 100L;

  private static ZKWatcher zkw;

  @BeforeAll
  public static void setUpBeforeClass() throws Exception {
    UTIL.startMiniZKCluster();
    zkw = new ZKWatcher(UTIL.getConfiguration(), "bulkload-event-cleaner", null);
  }

  @AfterAll
  public static void tearDownAfterClass() throws Exception {
    if (zkw != null) {
      zkw.close();
    }
    UTIL.shutdownMiniZKCluster();
  }

  @Test
  public void testCleanerRemovesExpiredDoneWithoutInProgress() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    conf.setLong(ReplicationBulkLoadEventTracker.BUCKET_WIDTH_MS_KEY, 1000);
    conf.setLong(ReplicationBulkLoadEventCleaner.DONE_TTL_MS_KEY, 1);

    ReplicationBulkLoadEventTracker tracker = new ReplicationBulkLoadEventTracker(conf, zkw);
    ReplicationBulkLoadEventTracker.Event event =
      tracker.newEvent("cluster-A", TABLE, REGION_NAME, SEQ_NUM, WRITE_TIME);
    tracker.markDone(event);
    assertTrue(tracker.isDone(event));

    Thread.sleep(5);
    ReplicationBulkLoadEventCleaner cleaner =
      new ReplicationBulkLoadEventCleaner(conf, new NeverStopped(), zkw);
    cleaner.choreForTesting();

    assertFalse(tracker.isDone(event));
  }

  @Test
  public void testCleanerKeepsDoneWhileInProgressExists() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    conf.setLong(ReplicationBulkLoadEventTracker.BUCKET_WIDTH_MS_KEY, 1000);
    conf.setLong(ReplicationBulkLoadEventCleaner.DONE_TTL_MS_KEY, 1);

    ReplicationBulkLoadEventTracker tracker = new ReplicationBulkLoadEventTracker(conf, zkw);
    ReplicationBulkLoadEventTracker.Event event =
      tracker.newEvent("cluster-A", TABLE, REGION_NAME, SEQ_NUM + 1, WRITE_TIME);
    assertTrue(tracker.claim(event).isClaimed());
    tracker.markDone(event);

    Thread.sleep(5);
    ReplicationBulkLoadEventCleaner cleaner =
      new ReplicationBulkLoadEventCleaner(conf, new NeverStopped(), zkw);
    cleaner.choreForTesting();

    assertTrue(tracker.isDone(event));
    tracker.release(event);
  }

  @Test
  public void testCleanerRemovesEmptyBucketsAfterExpiredDone() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    conf.setLong(ReplicationBulkLoadEventTracker.BUCKET_WIDTH_MS_KEY, 1000);
    conf.setLong(ReplicationBulkLoadEventCleaner.DONE_TTL_MS_KEY, 1);

    ReplicationBulkLoadEventTracker tracker = new ReplicationBulkLoadEventTracker(conf, zkw);
    ReplicationBulkLoadEventTracker.Event event =
      tracker.newEvent("cluster-A", TABLE, REGION_NAME, SEQ_NUM + 2, WRITE_TIME);
    assertTrue(tracker.claim(event).isClaimed());
    tracker.markDone(event);
    tracker.release(event);

    String eventsRoot = ZNodePaths.joinZNode(zkw.getZNodePaths().replicationZNode,
      ReplicationBulkLoadEventTracker.ZNODE_DEFAULT);
    String doneBucketPath =
      ZNodePaths.joinZNode(ZNodePaths.joinZNode(eventsRoot, "done"), event.getBucket());
    String inProgressBucketPath =
      ZNodePaths.joinZNode(ZNodePaths.joinZNode(eventsRoot, "in-progress"), event.getBucket());

    assertTrue(ZKUtil.checkExists(zkw, doneBucketPath) != -1);
    assertTrue(ZKUtil.checkExists(zkw, inProgressBucketPath) != -1);

    Thread.sleep(5);
    ReplicationBulkLoadEventCleaner cleaner =
      new ReplicationBulkLoadEventCleaner(conf, new NeverStopped(), zkw);
    cleaner.choreForTesting();

    assertEquals(-1, ZKUtil.checkExists(zkw, doneBucketPath));
    assertEquals(-1, ZKUtil.checkExists(zkw, inProgressBucketPath));
  }

  private static final class NeverStopped implements Stoppable {
    @Override
    public void stop(String why) {
    }

    @Override
    public boolean isStopped() {
      return false;
    }
  }
}
