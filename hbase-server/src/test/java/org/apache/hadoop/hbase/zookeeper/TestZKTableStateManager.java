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

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.CoordinatedStateException;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableStateManager;
import org.apache.zookeeper.KeeperException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.apache.hadoop.hbase.protobuf.generated.ZooKeeperProtos.Table;

@Category(MediumTests.class)
public class TestZKTableStateManager {
  private static final Log LOG = LogFactory.getLog(TestZKTableStateManager.class);
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniZKCluster();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniZKCluster();
  }

  @Test
  public void testTableStates()
      throws CoordinatedStateException, IOException, KeeperException, InterruptedException {
    final TableName name =
        TableName.valueOf("testDisabled");
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
    ZooKeeperWatcher zkw = new ZooKeeperWatcher(TEST_UTIL.getConfiguration(),
      name.getNameAsString(), abortable, true);
    TableStateManager zkt = new ZKTableStateManager(zkw);
    assertFalse(zkt.isTableState(name, Table.State.ENABLED));
    assertFalse(zkt.isTableState(name, Table.State.DISABLING));
    assertFalse(zkt.isTableState(name, Table.State.DISABLED));
    assertFalse(zkt.isTableState(name, Table.State.ENABLING));
    assertFalse(zkt.isTableState(name, Table.State.DISABLED, Table.State.DISABLING));
    assertFalse(zkt.isTableState(name, Table.State.DISABLED, Table.State.ENABLING));
    assertFalse(zkt.isTablePresent(name));
    zkt.setTableState(name, Table.State.DISABLING);
    assertTrue(zkt.isTableState(name, Table.State.DISABLING));
    assertTrue(zkt.isTableState(name, Table.State.DISABLED, Table.State.DISABLING));
    assertFalse(zkt.getTablesInStates(Table.State.DISABLED).contains(name));
    assertTrue(zkt.isTablePresent(name));
    zkt.setTableState(name, Table.State.DISABLED);
    assertTrue(zkt.isTableState(name, Table.State.DISABLED));
    assertTrue(zkt.isTableState(name, Table.State.DISABLED, Table.State.DISABLING));
    assertFalse(zkt.isTableState(name, Table.State.DISABLING));
    assertTrue(zkt.getTablesInStates(Table.State.DISABLED).contains(name));
    assertTrue(zkt.isTablePresent(name));
    zkt.setTableState(name, Table.State.ENABLING);
    assertTrue(zkt.isTableState(name, Table.State.ENABLING));
    assertTrue(zkt.isTableState(name, Table.State.DISABLED, Table.State.ENABLING));
    assertFalse(zkt.isTableState(name, Table.State.DISABLED));
    assertFalse(zkt.getTablesInStates(Table.State.DISABLED).contains(name));
    assertTrue(zkt.isTablePresent(name));
    zkt.setTableState(name, Table.State.ENABLED);
    assertTrue(zkt.isTableState(name, Table.State.ENABLED));
    assertFalse(zkt.isTableState(name, Table.State.ENABLING));
    assertTrue(zkt.isTablePresent(name));
    zkt.setDeletedTable(name);
    assertFalse(zkt.isTableState(name, Table.State.ENABLED));
    assertFalse(zkt.isTableState(name, Table.State.DISABLING));
    assertFalse(zkt.isTableState(name, Table.State.DISABLED));
    assertFalse(zkt.isTableState(name, Table.State.ENABLING));
    assertFalse(zkt.isTableState(name, Table.State.DISABLED, Table.State.DISABLING));
    assertFalse(zkt.isTableState(name, Table.State.DISABLED, Table.State.ENABLING));
    assertFalse(zkt.isTablePresent(name));
  }
}
