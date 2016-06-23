/*
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
package org.apache.hadoop.hbase.replication;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.ReplicationTests;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests ReplicationTableBase behavior when the Master startup is delayed. The table initialization
 * should be non-blocking, but any method calls that access the table should be blocking.
 */
@Category({ReplicationTests.class, MediumTests.class})
public class TestReplicationTableBase {

  private static long SLEEP_MILLIS = 5000;
  private static long TIME_OUT_MILLIS = 3000;
  private static Configuration conf;
  private static HBaseTestingUtility utility;
  private static ZooKeeperWatcher zkw;
  private static ReplicationTableBase rb;
  private static ReplicationQueues rq;
  private static ReplicationQueuesClient rqc;
  private volatile boolean asyncRequestSuccess = false;

  @Test
  public void testSlowStartup() throws Exception{
    utility = new HBaseTestingUtility();
    utility.startMiniZKCluster();
    conf = utility.getConfiguration();
    conf.setClass("hbase.region.replica.replication.ReplicationQueuesType",
      TableBasedReplicationQueuesImpl.class, ReplicationQueues.class);
    conf.setClass("hbase.region.replica.replication.ReplicationQueuesClientType",
      TableBasedReplicationQueuesClientImpl.class, ReplicationQueuesClient.class);
    zkw = HBaseTestingUtility.getZooKeeperWatcher(utility);
    utility.waitFor(0, TIME_OUT_MILLIS, new Waiter.ExplainingPredicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        rb = new ReplicationTableBase(conf, zkw) {};
        rq = ReplicationFactory.getReplicationQueues(new ReplicationQueuesArguments(
          conf, zkw, zkw));
        rqc = ReplicationFactory.getReplicationQueuesClient(
          new ReplicationQueuesClientArguments(conf, zkw, zkw));
        return true;
      }
      @Override
      public String explainFailure() throws Exception {
        return "Failed to initialize ReplicationTableBase, TableBasedReplicationQueuesClient and " +
          "TableBasedReplicationQueues after a timeout=" + TIME_OUT_MILLIS +
          " ms. Their initialization " + "should be non-blocking";
      }
    });
    final RequestReplicationQueueData async = new RequestReplicationQueueData();
    async.start();
    Thread.sleep(SLEEP_MILLIS);
    // Test that the Replication Table has not been assigned and the methods are blocking
    assertFalse(rb.getInitializationStatus());
    assertFalse(asyncRequestSuccess);
    utility.startMiniCluster();
    // Test that the methods do return the correct results after getting the table
    utility.waitFor(0, TIME_OUT_MILLIS, new Waiter.ExplainingPredicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        async.join();
        return true;
      }
      @Override
      public String explainFailure() throws Exception {
        return "ReplicationQueue failed to return list of replicators even after Replication Table "
          + "was initialized timeout=" + TIME_OUT_MILLIS + " ms";
      }
    });
    assertTrue(asyncRequestSuccess);
  }

  public class RequestReplicationQueueData extends Thread {
    @Override
    public void run() {
      assertEquals(0, rq.getListOfReplicators().size());
      asyncRequestSuccess = true;
    }
  }
}
