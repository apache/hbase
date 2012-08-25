/**
 * Copyright The Apache Software Foundation
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

package org.apache.hadoop.hbase.client;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.MultithreadedTestUtil;
import org.apache.hadoop.hbase.MultithreadedTestUtil.RepeatingTestThread;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HConnectionManager.HConnectionImplementation;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Various tests of using HConnection
 */
@Category(MediumTests.class)
public class TestHConnection {
  final Log LOG = LogFactory.getLog(getClass());
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private final long MILLIS_TO_WAIT_FOR_RACE = 1000;

  /**
   * @throws java.lang.Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniCluster();
  }

  /**
   * @throws java.lang.Exception
   */
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  /**
   * Thread that periodically aborts the connection
   */
  class AbortThread extends RepeatingTestThread {
    final HConnection connection;

    public AbortThread(MultithreadedTestUtil.TestContext ctx, HConnection connection) {
      super(ctx);
      this.connection = connection;
    }

    @Override
    public void doAnAction() throws IOException {
      connection.abort("test session expired", new KeeperException.SessionExpiredException());
    }
  };

  class HConnectionRaceTester extends HConnectionImplementation {
    public HConnectionRaceTester(Configuration configuration, boolean managed) throws ZooKeeperConnectionException {
      super(configuration, managed);
    }

    /**
     * Sleep after calling getZookeeperWatcher to attempt to trigger a race condition.
     * If the HConnection retrieves the ZooKeeperWatcher but does not cache the value,
     * by the time the new watcher is used, it could be null if the connection was aborted.
     * This sleep will increase the time between when the watcher is retrieved and when it is used.
     */
    public ZooKeeperWatcher getZooKeeperWatcher() throws ZooKeeperConnectionException {
      ZooKeeperWatcher zkw = super.getZooKeeperWatcher();
      try {
        Thread.sleep(10);
      } catch (InterruptedException ie) {
        // Ignore
      }
      return zkw;
    }
  }

  /**
   * Test that a connection that is aborted while calling isTableDisabled doesn't NPE
   */
  @Test
  public void testTableDisabledRace() throws Exception {
    final HConnection connection = new HConnectionRaceTester(TEST_UTIL.getConfiguration(), true);
    MultithreadedTestUtil.TestContext ctx =
      new MultithreadedTestUtil.TestContext(TEST_UTIL.getConfiguration());
    RepeatingTestThread disabledChecker = new RepeatingTestThread(ctx) {
      @Override
      public void doAnAction() throws IOException {
        try {
          connection.isTableDisabled(Bytes.toBytes("tableToCheck"));
        } catch (IOException ioe) {
          // Ignore.  ZK can legitimately fail, only care if we get a NullPointerException
        }
      }
    };
    AbortThread abortThread = new AbortThread(ctx, connection);

    ctx.addThread(disabledChecker);
    ctx.addThread(abortThread);
    ctx.startThreads();
    ctx.waitFor(MILLIS_TO_WAIT_FOR_RACE);
    ctx.stop();
  }

  /**
   * Test that a connection that is aborted while calling getCurrentNrNRS doesn't NPE
   */
  @Test
  public void testGetCurrentNrHRSRace() throws Exception {
    final HConnection connection = new HConnectionRaceTester(TEST_UTIL.getConfiguration(), true);
    MultithreadedTestUtil.TestContext ctx =
      new MultithreadedTestUtil.TestContext(TEST_UTIL.getConfiguration());
    RepeatingTestThread getCurrentNrHRSCaller = new RepeatingTestThread(ctx) {
      @Override
      public void doAnAction() throws IOException {
        try {
          connection.getCurrentNrHRS();
        } catch (IOException ioe) {
          // Ignore.  ZK can legitimately fail, only care if we get a NullPointerException
        }
      }
    };
    AbortThread abortThread = new AbortThread(ctx, connection);

    ctx.addThread(getCurrentNrHRSCaller);
    ctx.addThread(abortThread);
    ctx.startThreads();
    ctx.waitFor(MILLIS_TO_WAIT_FOR_RACE);
    ctx.stop();
  }

  @org.junit.Rule
  public org.apache.hadoop.hbase.ResourceCheckerJUnitRule cu =
    new org.apache.hadoop.hbase.ResourceCheckerJUnitRule();
}
