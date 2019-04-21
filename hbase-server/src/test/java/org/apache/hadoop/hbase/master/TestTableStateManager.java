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
package org.apache.hadoop.hbase.master;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.TableState;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.apache.hadoop.hbase.util.Threads;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;


import static junit.framework.TestCase.assertTrue;

/**
 * Tests the default table lock manager
 */
@Category({ MasterTests.class, LargeTests.class })
public class TestTableStateManager {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestTableStateManager.class);

  private final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  @Rule
  public TestName name = new TestName();

  @Before
  public void before() throws Exception {
    TEST_UTIL.startMiniCluster();
  }

  @After
  public void after() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testMigration() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    TEST_UTIL.createTable(tableName, HConstants.CATALOG_FAMILY_STR);
    TEST_UTIL.getAdmin().disableTable(tableName);
    // Table is disabled. Now remove the DISABLED column from the hbase:meta for this table's
    // region. We want to see if Master will read the DISABLED from zk and make use of it as
    // though it were reading the zk table state written by a hbase-1.x cluster.
    TableState state = MetaTableAccessor.getTableState(TEST_UTIL.getConnection(), tableName);
    assertTrue("State=" + state, state.getState().equals(TableState.State.DISABLED));
    MetaTableAccessor.deleteTableState(TEST_UTIL.getConnection(), tableName);
    assertTrue(MetaTableAccessor.getTableState(TEST_UTIL.getConnection(), tableName) == null);
    // Now kill Master so a new one can come up and run through the zk migration.
    HMaster master = TEST_UTIL.getMiniHBaseCluster().getMaster();
    master.stop("Restarting");
    while (!master.isStopped()) {
      Threads.sleep(1);
    }
    assertTrue(master.isStopped());
    JVMClusterUtil.MasterThread newMasterThread = TEST_UTIL.getMiniHBaseCluster().startMaster();
    master = newMasterThread.getMaster();
    while (!master.isInitialized()) {
      Threads.sleep(1);
    }
    assertTrue(MetaTableAccessor.getTableState(TEST_UTIL.getConnection(),
        tableName).getState().equals(TableState.State.DISABLED));
  }
}
