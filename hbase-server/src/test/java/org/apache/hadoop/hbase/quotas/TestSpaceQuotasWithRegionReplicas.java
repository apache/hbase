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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Category(LargeTests.class)
public class TestSpaceQuotasWithRegionReplicas {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestSpaceQuotasWithRegionReplicas.class);

  private static final Logger LOG =
      LoggerFactory.getLogger(TestSpaceQuotasWithRegionReplicas.class);
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
  public void testSetQuotaWithRegionReplicaSingleRegion() throws Exception {
    for (SpaceViolationPolicy policy : SpaceViolationPolicy.values()) {
      setQuotaAndVerifyForRegionReplication(1, 2, policy);
    }
  }

  @Test
  public void testSetQuotaWithRegionReplicaMultipleRegion() throws Exception {
    for (SpaceViolationPolicy policy : SpaceViolationPolicy.values()) {
      setQuotaAndVerifyForRegionReplication(6, 3, policy);
    }
  }

  @Test
  public void testSetQuotaWithSingleRegionZeroRegionReplica() throws Exception {
    for (SpaceViolationPolicy policy : SpaceViolationPolicy.values()) {
      setQuotaAndVerifyForRegionReplication(1, 0, policy);
    }
  }

  @Test
  public void testSetQuotaWithMultipleRegionZeroRegionReplicas() throws Exception {
    for (SpaceViolationPolicy policy : SpaceViolationPolicy.values()) {
      setQuotaAndVerifyForRegionReplication(6, 0, policy);
    }
  }

  private void setQuotaAndVerifyForRegionReplication(int region, int replicatedRegion,
      SpaceViolationPolicy policy) throws Exception {
    TableName tn = helper.createTableWithRegions(TEST_UTIL.getAdmin(),
        NamespaceDescriptor.DEFAULT_NAMESPACE_NAME_STR, region, replicatedRegion);
    helper.setQuotaLimit(tn, policy, 5L);
    helper.writeData(tn, 5L * SpaceQuotaHelperForTests.ONE_MEGABYTE);
    Put p = new Put(Bytes.toBytes("to_reject"));
    p.addColumn(Bytes.toBytes(SpaceQuotaHelperForTests.F1), Bytes.toBytes("to"),
        Bytes.toBytes("reject"));
    helper.verifyViolation(policy, tn, p);
  }
}
