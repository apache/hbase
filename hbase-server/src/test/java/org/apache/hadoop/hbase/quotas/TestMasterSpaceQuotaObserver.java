/**
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
package org.apache.hadoop.hbase.quotas;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.MasterCoprocessorHost;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

/**
 * Test class for {@link MasterSpaceQuotaObserver}.
 */
@Category(MediumTests.class)
public class TestMasterSpaceQuotaObserver {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestMasterSpaceQuotaObserver.class);

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static SpaceQuotaHelperForTests helper;

  @Rule
  public TestName testName = new TestName();

  @BeforeClass
  public static void setUp() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setBoolean(QuotaUtil.QUOTA_CONF_KEY, true);
    TEST_UTIL.startMiniCluster(1);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Before
  public void removeAllQuotas() throws Exception {
    if (helper == null) {
      helper = new SpaceQuotaHelperForTests(TEST_UTIL, testName, new AtomicLong());
    }
    final Connection conn = TEST_UTIL.getConnection();
    // Wait for the quota table to be created
    if (!conn.getAdmin().tableExists(QuotaUtil.QUOTA_TABLE_NAME)) {
      helper.waitForQuotaTable(conn);
    } else {
      // Or, clean up any quotas from previous test runs.
      helper.removeAllQuotas(conn);
      assertEquals(0, helper.listNumDefinedQuotas(conn));
    }
  }

  @Test
  public void testTableQuotaRemoved() throws Exception {
    final Connection conn = TEST_UTIL.getConnection();
    final Admin admin = conn.getAdmin();
    final TableName tn = TableName.valueOf(testName.getMethodName());
    // Drop the table if it somehow exists
    if (admin.tableExists(tn)) {
      admin.disableTable(tn);
      admin.deleteTable(tn);
    }

    // Create a table
    HTableDescriptor tableDesc = new HTableDescriptor(tn);
    tableDesc.addFamily(new HColumnDescriptor("F1"));
    admin.createTable(tableDesc);
    assertEquals(0, getNumSpaceQuotas());

    // Set a quota
    QuotaSettings settings = QuotaSettingsFactory.limitTableSpace(
        tn, 1024L, SpaceViolationPolicy.NO_INSERTS);
    admin.setQuota(settings);
    assertEquals(1, getNumSpaceQuotas());

    // Delete the table and observe the quota being automatically deleted as well
    admin.disableTable(tn);
    admin.deleteTable(tn);
    assertEquals(0, getNumSpaceQuotas());
  }

  @Test
  public void testNamespaceQuotaRemoved() throws Exception {
    final Connection conn = TEST_UTIL.getConnection();
    final Admin admin = conn.getAdmin();
    final String ns = testName.getMethodName();
    // Drop the ns if it somehow exists
    if (namespaceExists(ns)) {
      admin.deleteNamespace(ns);
    }

    // Create the ns
    NamespaceDescriptor desc = NamespaceDescriptor.create(ns).build();
    admin.createNamespace(desc);
    assertEquals(0, getNumSpaceQuotas());

    // Set a quota
    QuotaSettings settings = QuotaSettingsFactory.limitNamespaceSpace(
        ns, 1024L, SpaceViolationPolicy.NO_INSERTS);
    admin.setQuota(settings);
    assertEquals(1, getNumSpaceQuotas());

    // Delete the table and observe the quota being automatically deleted as well
    admin.deleteNamespace(ns);
    assertEquals(0, getNumSpaceQuotas());
  }

  @Test
  public void testObserverAddedByDefault() throws Exception {
    final HMaster master = TEST_UTIL.getHBaseCluster().getMaster();
    final MasterCoprocessorHost cpHost = master.getMasterCoprocessorHost();
    Set<String> coprocessorNames = cpHost.getCoprocessors();
    assertTrue(
        "Did not find MasterSpaceQuotaObserver in list of CPs: " + coprocessorNames,
        coprocessorNames.contains(MasterSpaceQuotaObserver.class.getSimpleName()));
  }

  public boolean namespaceExists(String ns) throws IOException {
    NamespaceDescriptor[] descs = TEST_UTIL.getAdmin().listNamespaceDescriptors();
    for (NamespaceDescriptor desc : descs) {
      if (ns.equals(desc.getName())) {
        return true;
      }
    }
    return false;
  }

  public int getNumSpaceQuotas() throws Exception {
    QuotaRetriever scanner = QuotaRetriever.open(TEST_UTIL.getConfiguration());
    int numSpaceQuotas = 0;
    for (QuotaSettings quotaSettings : scanner) {
      if (quotaSettings.getQuotaType() == QuotaType.SPACE) {
        numSpaceQuotas++;
      }
    }
    return numSpaceQuotas;
  }
}
