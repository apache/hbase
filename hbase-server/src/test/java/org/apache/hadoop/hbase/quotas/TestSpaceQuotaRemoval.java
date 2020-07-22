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

import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

@Category(LargeTests.class)
public class TestSpaceQuotaRemoval {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestSpaceQuotaRemoval.class);

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

  @Test
  public void testSetQuotaAndThenRemoveInOneWithNoInserts() throws Exception {
    setQuotaAndThenRemoveInOneAmongTwoTables(SpaceViolationPolicy.NO_INSERTS);
  }

  @Test
  public void testSetQuotaAndThenRemoveInOneWithNoWrite() throws Exception {
    setQuotaAndThenRemoveInOneAmongTwoTables(SpaceViolationPolicy.NO_WRITES);
  }

  @Test
  public void testSetQuotaAndThenRemoveInOneWithNoWritesCompaction() throws Exception {
    setQuotaAndThenRemoveInOneAmongTwoTables(SpaceViolationPolicy.NO_WRITES_COMPACTIONS);
  }

  @Test
  public void testSetQuotaAndThenRemoveInOneWithDisable() throws Exception {
    setQuotaAndThenRemoveInOneAmongTwoTables(SpaceViolationPolicy.DISABLE);
  }

  @Test
  public void testSetQuotaAndThenRemoveWithNoInserts() throws Exception {
    setQuotaAndThenRemove(SpaceViolationPolicy.NO_INSERTS);
  }

  @Test
  public void testSetQuotaAndThenRemoveWithNoWrite() throws Exception {
    setQuotaAndThenRemove(SpaceViolationPolicy.NO_WRITES);
  }

  @Test
  public void testSetQuotaAndThenRemoveWithNoWritesCompactions() throws Exception {
    setQuotaAndThenRemove(SpaceViolationPolicy.NO_WRITES_COMPACTIONS);
  }

  @Test
  public void testSetQuotaAndThenRemoveWithDisable() throws Exception {
    setQuotaAndThenRemove(SpaceViolationPolicy.DISABLE);
  }

  @Test
  public void testSetQuotaAndThenDisableIncrEnableWithNoInserts() throws Exception {
    setQuotaNextDisableThenIncreaseFinallyEnable(SpaceViolationPolicy.NO_INSERTS);
  }

  @Test
  public void testSetQuotaAndThenDisableIncrEnableWithNoWrite() throws Exception {
    setQuotaNextDisableThenIncreaseFinallyEnable(SpaceViolationPolicy.NO_WRITES);
  }

  @Test
  public void testSetQuotaAndThenDisableIncrEnableWithNoWritesCompaction() throws Exception {
    setQuotaNextDisableThenIncreaseFinallyEnable(SpaceViolationPolicy.NO_WRITES_COMPACTIONS);
  }

  @Test
  public void testSetQuotaAndThenDisableIncrEnableWithDisable() throws Exception {
    setQuotaNextDisableThenIncreaseFinallyEnable(SpaceViolationPolicy.DISABLE);
  }

  private void setQuotaAndThenRemove(SpaceViolationPolicy policy) throws Exception {
    Put put = new Put(Bytes.toBytes("to_reject"));
    put.addColumn(Bytes.toBytes(SpaceQuotaHelperForTests.F1), Bytes.toBytes("to"),
        Bytes.toBytes("reject"));

    // Do puts until we violate space policy
    final TableName tn = helper.writeUntilViolationAndVerifyViolation(policy, put);

    // Now, remove the quota
    helper.removeQuotaFromtable(tn);

    // Put some rows now: should not violate as quota settings removed
    helper.verifyNoViolation(tn, put);
  }

  @Test
  public void testDeleteTableUsageSnapshotsForNamespace() throws Exception {
    Put put = new Put(Bytes.toBytes("to_reject"));
    put.addColumn(Bytes.toBytes(SpaceQuotaHelperForTests.F1), Bytes.toBytes("to"),
      Bytes.toBytes("reject"));

    SpaceViolationPolicy policy = SpaceViolationPolicy.NO_INSERTS;

    //Create a namespace
    String ns1 = "nsnew";
    NamespaceDescriptor nsd = helper.createNamespace(ns1);

    //Create 2nd namespace with name similar to ns1
    String ns2 = ns1 + "test";
    NamespaceDescriptor nsd2 = helper.createNamespace(ns2);

    // Do puts until we violate space policy on table tn1 in namesapce ns1
    final TableName tn1 = helper.writeUntilViolationAndVerifyViolationInNamespace(ns1, policy, put);

    // Do puts until we violate space policy on table tn2 in namespace ns2
    final TableName tn2 = helper.writeUntilViolationAndVerifyViolationInNamespace(ns2, policy, put);

    // Now, remove the quota from namespace ns1 which will remove table usage snapshots for ns1
    helper.removeQuotaFromNamespace(ns1);

    // Verify that table usage snapshot for table tn2 in namespace ns2 exist
    helper.verifyTableUsageSnapshotForSpaceQuotaExist(tn2);

    // Put a new row on tn2: should violate as space quota exists on namespace ns2
    helper.verifyViolation(policy, tn2, put);

    // Put a new row on tn1: should not violate as quota settings removed from namespace ns1
    helper.verifyNoViolation(tn1, put);
  }

  @Test
  public void testSetNamespaceSizeQuotaAndThenRemove() throws Exception {
    Put put = new Put(Bytes.toBytes("to_reject"));
    put.addColumn(Bytes.toBytes(SpaceQuotaHelperForTests.F1), Bytes.toBytes("to"),
            Bytes.toBytes("reject"));

    SpaceViolationPolicy policy = SpaceViolationPolicy.NO_INSERTS;

    //Create namespace
    NamespaceDescriptor nsd = helper.createNamespace();
    String ns = nsd.getName();

    // Do puts until we violate space policy on table tn1
    final TableName tn1 = helper.writeUntilViolationAndVerifyViolationInNamespace(ns, policy, put);

    // Now, remove the quota from namespace
    helper.removeQuotaFromNamespace(ns);

    // Put a new row now on tn1: should not violate as quota settings removed from namespace
    helper.verifyNoViolation(tn1, put);
  }

  private void setQuotaAndThenRemoveInOneAmongTwoTables(SpaceViolationPolicy policy)
      throws Exception {
    Put put = new Put(Bytes.toBytes("to_reject"));
    put.addColumn(Bytes.toBytes(SpaceQuotaHelperForTests.F1), Bytes.toBytes("to"),
        Bytes.toBytes("reject"));

    // Do puts until we violate space policy on table tn1
    final TableName tn1 = helper.writeUntilViolationAndVerifyViolation(policy, put);

    // Do puts until we violate space policy on table tn2
    final TableName tn2 = helper.writeUntilViolationAndVerifyViolation(policy, put);

    // Now, remove the quota from table tn1
    helper.removeQuotaFromtable(tn1);

    // Put a new row now on tn1: should not violate as quota settings removed
    helper.verifyNoViolation(tn1, put);
    // Put a new row now on tn2: should violate as quota settings exists
    helper.verifyViolation(policy, tn2, put);
  }

  private void setQuotaNextDisableThenIncreaseFinallyEnable(SpaceViolationPolicy policy)
      throws Exception {
    Put put = new Put(Bytes.toBytes("to_reject"));
    put.addColumn(Bytes.toBytes(SpaceQuotaHelperForTests.F1), Bytes.toBytes("to"),
        Bytes.toBytes("reject"));

    // Do puts until we violate space policy
    final TableName tn = helper.writeUntilViolationAndVerifyViolation(policy, put);

    // Disable the table; in case of SpaceViolationPolicy.DISABLE already disabled
    if (!policy.equals(SpaceViolationPolicy.DISABLE)) {
      TEST_UTIL.getAdmin().disableTable(tn);
      TEST_UTIL.waitTableDisabled(tn, 10000);
    }

    // Now, increase limit and perform put
    helper.setQuotaLimit(tn, policy, 4L);

    // in case of disable policy quota manager will enable it
    if (!policy.equals(SpaceViolationPolicy.DISABLE)) {
      TEST_UTIL.getAdmin().enableTable(tn);
    }
    TEST_UTIL.waitTableEnabled(tn, 10000);

    // Put some row now: should not violate as quota limit increased
    helper.verifyNoViolation(tn, put);
  }
}
