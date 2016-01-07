/**
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

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.zookeeper.ZKTable.TableState;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

@Category(MediumTests.class)
public class TestZKTable {
  private static final Log LOG = LogFactory.getLog(TestZKTable.class);
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

  @Test
  public void testTableStates()
  throws ZooKeeperConnectionException, IOException, KeeperException {
    final String name = "testDisabled";

    ZooKeeperWatcher zkw = new ZooKeeperWatcher(TEST_UTIL.getConfiguration(),
      name, abortable, true);
    ZKTable zkt = new ZKTable(zkw);
    assertFalse(zkt.isEnabledTable(name));
    assertFalse(zkt.isDisablingTable(name));
    assertFalse(zkt.isDisabledTable(name));
    assertFalse(zkt.isEnablingTable(name));
    assertFalse(zkt.isDisablingOrDisabledTable(name));
    assertFalse(zkt.isDisabledOrEnablingTable(name));
    assertFalse(zkt.isTablePresent(name));
    zkt.setDisablingTable(name);
    assertTrue(zkt.isDisablingTable(name));
    assertTrue(zkt.isDisablingOrDisabledTable(name));
    assertFalse(zkt.getDisabledTables().contains(name));
    assertTrue(zkt.isTablePresent(name));
    zkt.setDisabledTable(name);
    assertTrue(zkt.isDisabledTable(name));
    assertTrue(zkt.isDisablingOrDisabledTable(name));
    assertFalse(zkt.isDisablingTable(name));
    assertTrue(zkt.getDisabledTables().contains(name));
    assertTrue(zkt.isTablePresent(name));
    zkt.setEnablingTable(name);
    assertTrue(zkt.isEnablingTable(name));
    assertTrue(zkt.isDisabledOrEnablingTable(name));
    assertFalse(zkt.isDisabledTable(name));
    assertFalse(zkt.getDisabledTables().contains(name));
    assertTrue(zkt.isTablePresent(name));
    zkt.setEnabledTable(name);
    assertTrue(zkt.isEnabledTable(name));
    assertFalse(zkt.isEnablingTable(name));
    assertTrue(zkt.isTablePresent(name));
    zkt.setDeletedTable(name);
    assertFalse(zkt.isEnabledTable(name));
    assertFalse(zkt.isDisablingTable(name));
    assertFalse(zkt.isDisabledTable(name));
    assertFalse(zkt.isEnablingTable(name));
    assertFalse(zkt.isDisablingOrDisabledTable(name));
    assertFalse(zkt.isDisabledOrEnablingTable(name));
    assertFalse(zkt.isTablePresent(name));
  }

  private void runTest9294CompatibilityTest(String tableName, Configuration conf)
  throws Exception {
    ZooKeeperWatcher zkw = new ZooKeeperWatcher(conf,
      tableName, abortable, true);
    ZKTable zkt = new ZKTable(zkw);
    zkt.setEnabledTable(tableName);
    // check that current/0.94 format table has proper ENABLED format
    assertTrue(
      ZKTableReadOnly.getTableState(zkw, zkw.masterTableZNode,  tableName) == TableState.ENABLED);
    // check that 0.92 format table is null, as expected by 0.92.0/0.92.1 clients
    assertTrue(ZKTableReadOnly.getTableState(zkw, zkw.masterTableZNode92, tableName) == null);
  }

  /**
   * Test that ZK table writes table state in formats expected by 0.92 and 0.94 clients
   */
  @Test
  public void test9294Compatibility() throws Exception {
    // without useMulti
    String tableName = "test9294Compatibility";
    runTest9294CompatibilityTest(tableName, TEST_UTIL.getConfiguration());

    // with useMulti
    tableName = "test9294CompatibilityWithMulti";
    Configuration conf = HBaseConfiguration.create(TEST_UTIL.getConfiguration());
    conf.setBoolean(HConstants.ZOOKEEPER_USEMULTI, true);
    runTest9294CompatibilityTest(tableName, conf);
  }

  /**
   * RecoverableZookeeper that throws a KeeperException after throwExceptionInNumOperations
   */
  class ThrowingRecoverableZookeeper extends RecoverableZooKeeper {
    private ZooKeeperWatcher zkw;
    private int throwExceptionInNumOperations;

    public ThrowingRecoverableZookeeper(ZooKeeperWatcher zkw) throws Exception {
      super(ZKConfig.getZKQuorumServersString(TEST_UTIL.getConfiguration()),
        HConstants.DEFAULT_ZK_SESSION_TIMEOUT, zkw, 3, 1000);
      this.zkw = zkw;
      this.throwExceptionInNumOperations = 0; // indicate not to throw an exception
    }

    public void setThrowExceptionInNumOperations(int throwExceptionInNumOperations) {
      this.throwExceptionInNumOperations = throwExceptionInNumOperations;
    }

    private void checkThrowKeeperException() throws KeeperException {
      if (throwExceptionInNumOperations == 1) {
        throwExceptionInNumOperations = 0;
        throw new KeeperException.DataInconsistencyException();
      }
      if(throwExceptionInNumOperations > 0) throwExceptionInNumOperations--;
    }

    public Stat setData(String path, byte[] data, int version)
    throws KeeperException, InterruptedException {
      checkThrowKeeperException();
      return zkw.getRecoverableZooKeeper().setData(path, data, version);
    }

    public void delete(String path, int version)
    throws InterruptedException, KeeperException {
      checkThrowKeeperException();
      zkw.getRecoverableZooKeeper().delete(path, version);
    }
  }
  /**
   * Because two ZooKeeper nodes are written for each table state transition
   * {@link ZooKeeperWatcher#masterTableZNode} and {@link ZooKeeperWatcher#masterTableZNode92}
   * it is possible that we fail in between the two operations and are left with
   * inconsistent state (when hbase.zookeeper.useMulti is false).
   * Check that we can get back to a consistent state by retrying the operation.
   */
  @Test
  public void testDisableTableRetry() throws Exception {
    final String tableName = "testDisableTableRetry";

    Configuration conf = TEST_UTIL.getConfiguration();
    // test only relevant if useMulti is false
    conf.setBoolean(HConstants.ZOOKEEPER_USEMULTI, false);
    ZooKeeperWatcher zkw = new ZooKeeperWatcher(conf,
      tableName, abortable, true);
    ThrowingRecoverableZookeeper throwing = new ThrowingRecoverableZookeeper(zkw);
    ZooKeeperWatcher spyZookeeperWatcher = Mockito.spy(zkw);
    Mockito.doReturn(throwing).when(spyZookeeperWatcher).getRecoverableZooKeeper();

    ZKTable zkt = new ZKTable(spyZookeeperWatcher);
    zkt.setEnabledTable(tableName);
    assertTrue(zkt.isEnabledOrDisablingTable(tableName));
    boolean caughtExpectedException = false;
    try {
      // throw an exception on the second ZK operation, which means the first will succeed.
      throwing.setThrowExceptionInNumOperations(2);
      zkt.setDisabledTable(tableName);
    } catch (KeeperException ke) {
      caughtExpectedException = true;
    }
    assertTrue(caughtExpectedException);
    assertFalse(zkt.isDisabledTable(tableName));
    // try again, ensure table is disabled
    zkt.setDisabledTable(tableName);
    // ensure disabled from master perspective
    assertTrue(zkt.isDisabledTable(tableName));
    // ensure disabled from client perspective
    assertTrue(ZKTableReadOnly.isDisabledTable(zkw, tableName));
  }

  /**
   * Same as above, but with enableTable
   */
  @Test
  public void testEnableTableRetry() throws Exception {
    final String tableName = "testEnableTableRetry";

    Configuration conf = TEST_UTIL.getConfiguration();
    // test only relevant if useMulti is false
    conf.setBoolean(HConstants.ZOOKEEPER_USEMULTI, false);
    ZooKeeperWatcher zkw = new ZooKeeperWatcher(conf,
      tableName, abortable, true);
    ThrowingRecoverableZookeeper throwing = new ThrowingRecoverableZookeeper(zkw);
    ZooKeeperWatcher spyZookeeperWatcher = Mockito.spy(zkw);
    Mockito.doReturn(throwing).when(spyZookeeperWatcher).getRecoverableZooKeeper();

    ZKTable zkt = new ZKTable(spyZookeeperWatcher);
    zkt.setDisabledTable(tableName);
    assertTrue(zkt.isDisabledTable(tableName));
    boolean caughtExpectedException = false;
    try {
      // throw an exception on the second ZK operation, which means the first will succeed.
      throwing.throwExceptionInNumOperations = 2;
      zkt.setEnabledTable(tableName);
    } catch (KeeperException ke) {
      caughtExpectedException = true;
    }
    assertTrue(caughtExpectedException);
    assertFalse(zkt.isEnabledTable(tableName));
    // try again, ensure table is enabled
    zkt.setEnabledTable(tableName);
    // ensure enabled from master perspective
    assertTrue(zkt.isEnabledTable(tableName));
    // ensure enabled from client perspective
    assertTrue(ZKTableReadOnly.isEnabledTable(zkw, tableName));
  }

  /**
   * Same as above, but with deleteTable
   */
  @Test
  public void testDeleteTableRetry() throws Exception {
    final String tableName = "testEnableTableRetry";

    Configuration conf = TEST_UTIL.getConfiguration();
    // test only relevant if useMulti is false
    conf.setBoolean(HConstants.ZOOKEEPER_USEMULTI, false);
    ZooKeeperWatcher zkw = new ZooKeeperWatcher(conf,
      tableName, abortable, true);
    ThrowingRecoverableZookeeper throwing = new ThrowingRecoverableZookeeper(zkw);
    ZooKeeperWatcher spyZookeeperWatcher = Mockito.spy(zkw);
    Mockito.doReturn(throwing).when(spyZookeeperWatcher).getRecoverableZooKeeper();

    ZKTable zkt = new ZKTable(spyZookeeperWatcher);
    zkt.setDisabledTable(tableName);
    assertTrue(zkt.isDisabledTable(tableName));
    boolean caughtExpectedException = false;
    try {
      // throw an exception on the second ZK operation, which means the first will succeed.
      throwing.setThrowExceptionInNumOperations(2);
      zkt.setDeletedTable(tableName);
    } catch (KeeperException ke) {
      caughtExpectedException = true;
    }
    assertTrue(caughtExpectedException);
    assertTrue(zkt.isTablePresent(tableName));
    // try again, ensure table is deleted
    zkt.setDeletedTable(tableName);
    // ensure deleted from master perspective
    assertFalse(zkt.isTablePresent(tableName));
    // ensure deleted from client perspective
    assertFalse(ZKTableReadOnly.getDisabledTables(zkw).contains(tableName));
  }

  @org.junit.Rule
  public org.apache.hadoop.hbase.ResourceCheckerJUnitRule cu =
    new org.apache.hadoop.hbase.ResourceCheckerJUnitRule();
}
