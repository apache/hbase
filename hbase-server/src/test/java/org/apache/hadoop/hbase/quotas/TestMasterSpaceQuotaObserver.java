/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.quotas;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

/**
 * Test class for {@link MasterSpaceQuotaObserver}.
 */
@Category(MediumTests.class)
public class TestMasterSpaceQuotaObserver {
  private static final Log LOG = LogFactory.getLog(TestSpaceQuotas.class);
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  @Rule
  public TestName testName = new TestName();

  @BeforeClass
  public static void setUp() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.set(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY, MasterSpaceQuotaObserver.class.getName());
    conf.setBoolean(QuotaUtil.QUOTA_CONF_KEY, true);
    TEST_UTIL.startMiniCluster(1);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Before
  public void removeAllQuotas() throws Exception {
    final Connection conn = TEST_UTIL.getConnection();
    // Wait for the quota table to be created
    if (!conn.getAdmin().tableExists(QuotaUtil.QUOTA_TABLE_NAME)) {
      do {
        LOG.debug("Quota table does not yet exist");
        Thread.sleep(1000);
      } while (!conn.getAdmin().tableExists(QuotaUtil.QUOTA_TABLE_NAME));
    } else {
      // Or, clean up any quotas from previous test runs.
      QuotaRetriever scanner = QuotaRetriever.open(TEST_UTIL.getConfiguration());
      for (QuotaSettings quotaSettings : scanner) {
        final String namespace = quotaSettings.getNamespace();
        final TableName tableName = quotaSettings.getTableName();
        if (null != namespace) {
          LOG.debug("Deleting quota for namespace: " + namespace);
          QuotaUtil.deleteNamespaceQuota(conn, namespace);
        } else {
          assert null != tableName;
          LOG.debug("Deleting quota for table: "+ tableName);
          QuotaUtil.deleteTableQuota(conn, tableName);
        }
      }
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
