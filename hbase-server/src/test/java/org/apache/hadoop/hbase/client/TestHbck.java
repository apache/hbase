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

package org.apache.hadoop.hbase.client;

import static junit.framework.TestCase.assertTrue;

import java.io.IOException;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class to test HBaseHbck.
 * Spins up the minicluster once at test start and then takes it down afterward.
 * Add any testing of HBaseHbck functionality here.
 */
@Category({LargeTests.class, ClientTests.class})
public class TestHbck {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestHbck.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestHbck.class);
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private Admin admin;
  private Hbck hbck;

  @Rule
  public TestName name = new TestName();

  private static final TableName tableName = TableName.valueOf(TestHbck.class.getSimpleName());

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.getConfiguration().setInt("hbase.regionserver.msginterval", 100);
    TEST_UTIL.getConfiguration().setInt("hbase.client.pause", 250);
    TEST_UTIL.getConfiguration().setInt("hbase.client.retries.number", 6);
    TEST_UTIL.getConfiguration().setBoolean("hbase.master.enabletable.roundrobin", true);
    TEST_UTIL.startMiniCluster(3);

    TEST_UTIL.createTable(tableName, "family1");
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Before
  public void setUp() throws Exception {
    this.admin = TEST_UTIL.getAdmin();
    this.hbck = TEST_UTIL.getHbck();
  }

  @After
  public void tearDown() throws Exception {
    for (HTableDescriptor htd : this.admin.listTables()) {
      TEST_UTIL.deleteTable(htd.getTableName());
    }
    this.hbck.close();
  }

  @Test
  public void testSetTableStateInMeta() throws IOException {
    // set table state to DISABLED
    hbck.setTableStateInMeta(new TableState(tableName, TableState.State.DISABLED));
    // Method {@link Hbck#setTableStateInMeta()} returns previous state, which in this case
    // will be DISABLED
    TableState prevState =
        hbck.setTableStateInMeta(new TableState(tableName, TableState.State.ENABLED));
    assertTrue("Incorrect previous state! expeced=DISABLED, found=" + prevState.getState(),
        prevState.isDisabled());
  }
}
