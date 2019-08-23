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
public class TestSpaceQuotaSwitchPolicies {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestSpaceQuotaSwitchPolicies.class);

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
  public void testSetQuotaFirstWithDisableNextNoWrites() throws Exception {
    setQuotaAndViolateNextSwitchPoliciesAndValidate(SpaceViolationPolicy.DISABLE,
        SpaceViolationPolicy.NO_WRITES);
  }

  @Test
  public void testSetQuotaFirstWithDisableNextAgainDisable() throws Exception {
    setQuotaAndViolateNextSwitchPoliciesAndValidate(SpaceViolationPolicy.DISABLE,
        SpaceViolationPolicy.DISABLE);
  }

  @Test
  public void testSetQuotaFirstWithDisableNextNoInserts() throws Exception {
    setQuotaAndViolateNextSwitchPoliciesAndValidate(SpaceViolationPolicy.DISABLE,
        SpaceViolationPolicy.NO_INSERTS);
  }

  @Test
  public void testSetQuotaFirstWithDisableNextNoWritesCompaction() throws Exception {
    setQuotaAndViolateNextSwitchPoliciesAndValidate(SpaceViolationPolicy.DISABLE,
        SpaceViolationPolicy.NO_WRITES_COMPACTIONS);
  }

  @Test
  public void testSetQuotaFirstWithNoWritesNextWithDisable() throws Exception {
    setQuotaAndViolateNextSwitchPoliciesAndValidate(SpaceViolationPolicy.NO_WRITES,
        SpaceViolationPolicy.DISABLE);
  }

  private void setQuotaAndViolateNextSwitchPoliciesAndValidate(SpaceViolationPolicy policy1,
      SpaceViolationPolicy policy2) throws Exception {
    Put put = new Put(Bytes.toBytes("to_reject"));
    put.addColumn(Bytes.toBytes(SpaceQuotaHelperForTests.F1), Bytes.toBytes("to"),
        Bytes.toBytes("reject"));

    // Do puts until we violate space violation policy1
    final TableName tn = helper.writeUntilViolationAndVerifyViolation(policy1, put);

    // Now, change violation policy to policy2
    helper.setQuotaLimit(tn, policy2, 2L);

    // The table should be in enabled state on changing violation policy
    if (policy1.equals(SpaceViolationPolicy.DISABLE) && !policy1.equals(policy2)) {
      TEST_UTIL.waitTableEnabled(tn, 20000);
    }
    // Put some row now: should still violate as quota limit still violated
    helper.verifyViolation(policy2, tn, put);
  }
}

