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
package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.testclassification.LargeTests;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category(LargeTests.class)
public class TestCompactionOffloadSwitch {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestCompactionOffloadSwitch.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestCompactionOffloadSwitch.class);
  private static HBaseTestingUtility TEST_UTIL;
  private static Configuration conf = HBaseConfiguration.create();
  private static Connection connection;
  private static Admin admin;

  @Before
  public void setUp() throws Exception {
    TEST_UTIL = new HBaseTestingUtility(conf);
    TEST_UTIL.startMiniCluster();
    admin = TEST_UTIL.getAdmin();
    connection = ConnectionFactory.createConnection(TEST_UTIL.getConfiguration());
  }

  @After
  public void tearDown() throws Exception {
    if (admin != null) {
      admin.close();
    }
    if (connection != null) {
      connection.close();
    }
    TEST_UTIL.shutdownMiniCluster();
  }

  private void testSwitchCompactionOffload(Admin admin, boolean oldCompactionOffload,
      boolean newCompactionOffload) throws IOException {
    boolean state = admin.switchCompactionOffload(newCompactionOffload);
    Assert.assertEquals(oldCompactionOffload, state);
    Assert.assertEquals(newCompactionOffload, admin.isCompactionOffloadEnabled());
    TEST_UTIL.getHBaseCluster().getRegionServerThreads().forEach(rs -> Assert.assertEquals(
      newCompactionOffload,
      rs.getRegionServer().getRegionServerCompactionOffloadManager().isCompactionOffloadEnabled()));
  }

  @Test
  public void testSwitchCompactionOffload() throws IOException {
    Admin admin = TEST_UTIL.getAdmin();
    testSwitchCompactionOffload(admin, false, true);
    testSwitchCompactionOffload(admin, true, false);
    testSwitchCompactionOffload(admin, false, false);
    testSwitchCompactionOffload(admin, false, true);
  }
}
