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
package org.apache.hadoop.hbase.master;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test cases for master to recover from ZK session expiry.
 */
public class TestMasterZKSessionRecovery {
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  /**
   * The default timeout is 5 minutes.
   * Shorten it so that the test won't wait for too long.
   */
  static {
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setLong("hbase.master.zksession.recover.timeout", 50000);
  }

  @Before
  public void setUp() throws Exception {
    // Start a cluster of one regionserver.
    TEST_UTIL.startMiniCluster(1);
  }

  @After
  public void tearDown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  /**
   * Negative test of master recovery from zk session expiry.
   * <p>
   * Starts with one master. Fakes the master zk session expired.
   * Ensures the master cannot recover the expired zk session since
   * the master zk node is still there.
   * @throws Exception
   */
  @Test(timeout=10000)
  public void testMasterZKSessionRecoveryFailure() throws Exception {
    MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
    HMaster m = cluster.getMaster();
    m.abort("Test recovery from zk session expired",
      new KeeperException.SessionExpiredException());
    assertTrue(m.isStopped());
  }

  /**
   * Positive test of master recovery from zk session expiry.
   * <p>
   * Starts with one master. Closes the master zk session.
   * Ensures the master can recover the expired zk session.
   * @throws Exception
   */
  @Test(timeout=60000)
  public void testMasterZKSessionRecoverySuccess() throws Exception {
    MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
    HMaster m = cluster.getMaster();
    m.getZooKeeperWatcher().close();
    m.abort("Test recovery from zk session expired",
      new KeeperException.SessionExpiredException());
    assertFalse(m.isStopped());
  }
}

