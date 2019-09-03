/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.quotas;


import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category(MediumTests.class)
public class TestSpaceQuotasAtNamespaceLevel {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestSpaceQuotasAtNamespaceLevel.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestSpaceQuotasAtNamespaceLevel.class);
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  @Rule
  public TestName testName = new TestName();
  private SpaceQuotaHelperForTests helper;

  @BeforeClass
  public static void setUp() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    SpaceQuotaHelperForTests.updateConfigForQuotas(conf);
    TEST_UTIL.startMiniCluster(1);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Before
  public void removeAllQuotas() throws Exception {
    helper = new SpaceQuotaHelperForTests(TEST_UTIL, testName, new AtomicLong(0));
    helper.removeAllQuotas();
  }

  @After
  public void removeQuotas() throws Exception {
    helper.removeAllQuotas();
  }

  @Test
  public void testSetNamespaceQuotaAndRemove() throws Exception {
    NamespaceDescriptor nd = helper.createNamespace();
    TableName table = helper.createTableInNamespace(nd);

    // Set quota on namespace.
    helper.setQuotaLimitNamespace(nd.getName(), SpaceViolationPolicy.NO_WRITES, 2L);

    // Sufficient time for all the chores to run
    Thread.sleep(5000);

    // Get Current Snapshot from 'hbase:quota'
    Map<TableName, SpaceQuotaSnapshot> snapshotMap =
        QuotaTableUtil.getSnapshots(TEST_UTIL.getConnection());

    // After setting quota on namespace, 'hbase:quota' should have some entries present.
    Assert.assertEquals(1, snapshotMap.size());

    helper.removeQuotaFromNamespace(nd.getName());

    // Get Current Snapshot from 'hbase:quota'
    snapshotMap = QuotaTableUtil.getSnapshots(TEST_UTIL.getConnection());

    // After removing quota on namespace, 'hbase:quota' should not have any entry present.
    Assert.assertEquals(0, snapshotMap.size());

    // drop table and namespace.
    TEST_UTIL.getAdmin().disableTable(table);
    TEST_UTIL.getAdmin().deleteTable(table);
    TEST_UTIL.getAdmin().deleteNamespace(nd.getName());
  }

  @Test
  public void testDropTableInNamespaceQuota() throws Exception {
    NamespaceDescriptor nd = helper.createNamespace();
    TableName table = helper.createTableInNamespace(nd);

    // Set quota on namespace.
    helper.setQuotaLimitNamespace(nd.getName(), SpaceViolationPolicy.NO_WRITES, 2L);

    // write some data.
    helper.writeData(table,SpaceQuotaHelperForTests.ONE_KILOBYTE);

    // Sufficient time for all the chores to run
    Thread.sleep(5000);

    // Get Current Snapshot from 'hbase:quota'
    Map<TableName, SpaceQuotaSnapshot> snapshotMap =
        QuotaTableUtil.getSnapshots(TEST_UTIL.getConnection());

    // Table before drop should have entry in 'hbase:quota'
    Assert.assertTrue(snapshotMap.containsKey(table));

    TEST_UTIL.getAdmin().disableTable(table);
    TEST_UTIL.getAdmin().deleteTable(table);

    // Get Current Snapshot from 'hbase:quota'
    snapshotMap = QuotaTableUtil.getSnapshots(TEST_UTIL.getConnection());

    // Table after drop should not have entry in 'hbase:quota'
    Assert.assertFalse(snapshotMap.containsKey(table));

    //drop  Namepsace.
    TEST_UTIL.getAdmin().deleteNamespace(nd.getName());
  }
}
