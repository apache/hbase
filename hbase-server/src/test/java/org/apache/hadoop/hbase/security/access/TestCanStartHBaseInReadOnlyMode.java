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
package org.apache.hadoop.hbase.security.access;

import static org.apache.hadoop.hbase.HConstants.HBASE_CLIENT_RETRIES_NUMBER;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.SecurityTests;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

@Category({ SecurityTests.class, LargeTests.class })
@SuppressWarnings("deprecation")
public class TestCanStartHBaseInReadOnlyMode {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestCanStartHBaseInReadOnlyMode.class);

  private static final String READ_ONLY_CONTROLLER_NAME = ReadOnlyController.class.getName();
  private static final HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();
  private static Configuration conf;

  @Rule
  public TestName name = new TestName();

  @BeforeClass
  public static void beforeClass() throws Exception {
    conf = TEST_UTIL.getConfiguration();

    // Shorten the run time of failed unit tests by limiting retries and the session timeout
    // threshold
    conf.setInt(HBASE_CLIENT_RETRIES_NUMBER, 0);
    conf.setInt("hbase.master.init.timeout.localHBaseCluster", 10000);

    // Enable Read-Only mode to prove the cluster can be started in this state
    conf.setBoolean(HConstants.HBASE_GLOBAL_READONLY_ENABLED_KEY, true);

    // Add ReadOnlyController coprocessors to the master, region server, and regions
    conf.set(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY, READ_ONLY_CONTROLLER_NAME);
    conf.set(CoprocessorHost.REGIONSERVER_COPROCESSOR_CONF_KEY, READ_ONLY_CONTROLLER_NAME);
    conf.set(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY, READ_ONLY_CONTROLLER_NAME);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testCanStartHBaseInReadOnlyMode() throws Exception {
    TEST_UTIL.startMiniCluster(1);
  }
}
