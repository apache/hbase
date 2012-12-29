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
package org.apache.hadoop.hbase.zookeeper;


import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.zookeeper.ZKTableReadOnly;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MediumTests.class)
public class TestZKTableReadOnly {
  private static final Log LOG = LogFactory.getLog(TestZooKeeperNodeTracker.class);
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniZKCluster();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniZKCluster();
  }

  Abortable abortable = new Abortable() {
    @Override
    public void abort(String why, Throwable e) {
      LOG.info(why, e);
    }

    @Override
    public boolean isAborted() {
      return false;
    }
  };

  private boolean enableAndCheckEnabled(ZooKeeperWatcher zkw, String tableName) throws Exception {
    // set the table to enabled, as that is the only state that differs
    // between the two formats
    ZKTable zkt = new ZKTable(zkw);
    zkt.setEnabledTable(tableName);
    return ZKTableReadOnly.isEnabledTable(zkw, tableName);
  }

  private void runClientCompatiblityWith92ZNodeTest(String tableName, Configuration conf)
  throws Exception {
    ZooKeeperWatcher zkw = new ZooKeeperWatcher(conf,
      tableName, abortable, true);
    assertTrue(enableAndCheckEnabled(zkw, tableName));
  }
  /**
   * Test that client ZK reader can handle the 0.92 table format znode.
   */
  @Test
  public void testClientCompatibilityWith92ZNode() throws Exception {
    // test without useMulti
    String tableName = "testClientCompatibilityWith92ZNode";
    // Set the client to read from the 0.92 table znode format
    Configuration conf = HBaseConfiguration.create(TEST_UTIL.getConfiguration());
    String znode92 = conf.get("zookeeper.znode.masterTableEnableDisable92", "table92");
    conf.set("zookeeper.znode.clientTableEnableDisable", znode92);
    runClientCompatiblityWith92ZNodeTest(tableName, conf);

    // test with useMulti
    tableName = "testClientCompatibilityWith92ZNodeUseMulti";
    conf.setBoolean(HConstants.ZOOKEEPER_USEMULTI, true);
    runClientCompatiblityWith92ZNodeTest(tableName, conf);
  }

  private void runClientCompatibilityWith94ZNodeTest(String tableName, Configuration conf)
  throws Exception {
    ZooKeeperWatcher zkw = new ZooKeeperWatcher(TEST_UTIL.getConfiguration(),
      tableName, abortable, true);
    assertTrue(enableAndCheckEnabled(zkw, tableName));
  }

  /**
   * Test that client ZK reader can handle the current (0.94) table format znode.
   */
  @Test
  public void testClientCompatibilityWith94ZNode() throws Exception {
    String tableName = "testClientCompatibilityWith94ZNode";

    // without useMulti
    runClientCompatibilityWith94ZNodeTest(tableName, TEST_UTIL.getConfiguration());

    // with useMulti
    tableName = "testClientCompatiblityWith94ZNodeUseMulti";
    Configuration conf = HBaseConfiguration.create(TEST_UTIL.getConfiguration());
    conf.setBoolean(HConstants.ZOOKEEPER_USEMULTI, true);
    runClientCompatibilityWith94ZNodeTest(tableName, conf);
  }

  @org.junit.Rule
  public org.apache.hadoop.hbase.ResourceCheckerJUnitRule cu =
    new org.apache.hadoop.hbase.ResourceCheckerJUnitRule();
}
