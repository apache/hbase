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

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.testclassification.ReplicationTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

@Category({ SmallTests.class, ReplicationTests.class })
public class TestReplicationSourceLogQueue {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestReplicationSourceLogQueue.class);

  /*
   * Testing enqueue and dequeuing of wal
   */
  @Test
  public void testEnqueueDequeue() {
    String walGroupId1 = "fake-walgroup-id-1";
    String walGroupId2 = "fake-walgroup-id-2";

    MetricsSource metrics = new MetricsSource("1");
    Configuration conf = HBaseConfiguration.create();
    ReplicationSource source = mock(ReplicationSource.class);
    Mockito.doReturn("peer").when(source).logPeerId();
    ReplicationSourceLogQueue logQueue = new ReplicationSourceLogQueue(conf, metrics, source);
    final Path log1 = new Path("log-walgroup-a.8");

    logQueue.enqueueLog(log1, walGroupId1);
    assertEquals(1, logQueue.getQueue(walGroupId1).size());
    final Path log2 = new Path("log-walgroup-b.4");
    logQueue.enqueueLog(log2, walGroupId2);
    assertEquals(1, logQueue.getQueue(walGroupId2).size());
    assertEquals(2, logQueue.getNumQueues());

    // Remove an element from walGroupId2.
    // After this op, there will be only one element in the queue log-walgroup-a.8
    logQueue.remove(walGroupId2);
    assertEquals(0, logQueue.getQueue(walGroupId2).size());
    // Remove last element from the queue.
    logQueue.remove(walGroupId1);
    assertEquals(0, logQueue.getQueue(walGroupId1).size());
    // This will test the case where there are no elements in the queue.
  }
}
