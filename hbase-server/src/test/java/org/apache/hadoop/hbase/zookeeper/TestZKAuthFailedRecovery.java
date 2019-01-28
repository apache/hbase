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
package org.apache.hadoop.hbase.zookeeper;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.common.base.Throwables;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HConnectionTestingUtility;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.zookeeper.ZKUtil.ZooKeeperFactory;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category(MediumTests.class)
public class TestZKAuthFailedRecovery {
  final Logger LOG = LoggerFactory.getLogger(getClass());
  protected final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  public static class AuthFailingZooKeeperFactory implements ZooKeeperFactory {
    @Override
    public RecoverableZooKeeper create(String quorumServers, int sessionTimeout, Watcher watcher,
        int maxRetries, int retryIntervalMillis, int maxSleepTime, String identifier,
        int authFailedRetries, int authFailedPause) throws IOException {
      return new AuthFailingRecoverableZooKeeper(quorumServers, sessionTimeout, watcher, maxRetries,
          retryIntervalMillis, maxSleepTime, identifier, authFailedRetries, authFailedPause);
    }
  }

  private static final int FAILURES_BEFORE_SUCCESS = 3;

  public static class SelfHealingZooKeeperFactory implements ZooKeeperFactory {
    @Override
    public RecoverableZooKeeper create(String quorumServers, int sessionTimeout, Watcher watcher,
        int maxRetries, int retryIntervalMillis, int maxSleepTime, String identifier,
        int authFailedRetries, int authFailedPause) throws IOException {
      return new SelfHealingRecoverableZooKeeper(quorumServers, sessionTimeout, watcher, maxRetries,
          retryIntervalMillis, maxSleepTime, identifier, authFailedRetries, authFailedPause,
          FAILURES_BEFORE_SUCCESS);
    }
  }

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setBoolean("hbase.table.sanity.checks", true); // enable for below tests
    TEST_UTIL.startMiniCluster(1);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testFaultyClientZK() throws Exception {
    Configuration conf = new Configuration(TEST_UTIL.getConfiguration());
    conf.setClass("zookeeper.factory.class", AuthFailingZooKeeperFactory.class,
        ZooKeeperFactory.class);
    LOG.debug("Reading meta first time");
    final Connection conn = ConnectionFactory.createConnection(conf);
    try (Table t = conn.getTable(TableName.valueOf("hbase:meta"))) {
      LOG.info(TEST_UTIL.countRows(t) + " rows in meta");
    }
    // Make sure we got our custom ZK wrapper class from the HConn
    ZooKeeper zk = HConnectionTestingUtility.unwrapZK(conn).checkZk();
    assertEquals(AuthFailingZooKeeper.class, zk.getClass());

    ((AuthFailingZooKeeper) zk).triggerAuthFailed();
    // Clear out the region cache to force a read to meta (and thus, a read to ZK)
    HConnectionTestingUtility.clearRegionCache(conn);

    // Use the HConnection in a way that will talk to ZK
    ExecutorService svc = Executors.newSingleThreadExecutor();
    Future<Boolean> res = svc.submit(new Callable<Boolean>() {
      public Boolean call() {
        LOG.debug("Reading meta after clearing the Region caches");
        try (Table t = conn.getTable(TableName.valueOf("hbase:meta"))) {
          LOG.info(TEST_UTIL.countRows(t) + " rows in meta");
          return true;
        } catch (Exception e) {
          LOG.error("Failed to read hbase:meta", e);
          return false;
        }
      }
    });
    // Without proper handling of AUTH_FAILED, this would spin indefinitely. With
    // the change introduced with this test, we should get a fresh ZK instance that
    // won't fail repeatedly.
    try {
      res.get(30, TimeUnit.SECONDS);
    } catch (ExecutionException e) {
      LOG.error("Failed to execute task", e);
      Assert.fail("Failed to recover from AUTH_FAILED state in zookeeper client");
    } catch (TimeoutException e) {
      LOG.error("Task timed out instead of recovering", e);
      Assert.fail("Failed to recover from AUTH_FAILED state in zookeeper client");
    }
  }

  @Test
  public void eventuallyRecoveringZKClient() throws Exception {
    Configuration conf = new Configuration(TEST_UTIL.getConfiguration());
    conf.setClass("zookeeper.factory.class", SelfHealingZooKeeperFactory.class,
        ZooKeeperFactory.class);
    // Retry one more time than we fail, and validate that we succeed
    conf.setInt(ZKUtil.AUTH_FAILED_RETRIES_KEY, FAILURES_BEFORE_SUCCESS + 1);
    // Don't bother waiting
    conf.setInt(ZKUtil.AUTH_FAILED_PAUSE_KEY, 0);

    final Connection conn = ConnectionFactory.createConnection(conf);

    // Make sure we got our custom ZK wrapper class from the HConn
    RecoverableZooKeeper recoverableZk = HConnectionTestingUtility.unwrapZK(conn);
    assertEquals(SelfHealingRecoverableZooKeeper.class, recoverableZk.getClass());
    ZooKeeper zk = recoverableZk.checkZk();
    assertEquals(AuthFailingZooKeeper.class, zk.getClass());

    try (Table t = conn.getTable(TableName.valueOf("hbase:meta"))) {
      LOG.info(TEST_UTIL.countRows(t) + " rows in meta");
    }
  }

  @Test
  public void retriesExceededOnAuthFailed() throws Exception {
    Configuration conf = new Configuration(TEST_UTIL.getConfiguration());
    conf.setClass("zookeeper.factory.class", SelfHealingZooKeeperFactory.class,
        ZooKeeperFactory.class);
    // Retry one more time than we fail, and validate that we succeed
    conf.setInt(ZKUtil.AUTH_FAILED_RETRIES_KEY, FAILURES_BEFORE_SUCCESS - 1);
    // Don't bother waiting
    conf.setInt(ZKUtil.AUTH_FAILED_PAUSE_KEY, 0);

    Connection conn = null;
    try {
      conn = ConnectionFactory.createConnection(conf);
    } catch (Exception e) {
      // Our first comms with ZK is to read the clusterId when creating the connection
      LOG.info("Caught exception, validating it", e);
      Throwable rootCause = Throwables.getRootCause(e);
      assertEquals(RuntimeException.class, rootCause.getClass());
      assertTrue("Expected the exception to contain the text 'AUTH_FAILED'",
          rootCause.getMessage().contains("AUTH_FAILED"));
    } finally {
      if (conn != null) {
        conn.close();
      }
    }
  }
}
