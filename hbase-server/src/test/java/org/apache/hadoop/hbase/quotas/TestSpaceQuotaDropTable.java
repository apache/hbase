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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category(LargeTests.class)
public class TestSpaceQuotaDropTable {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestSpaceQuotaDropTable.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestSpaceQuotaDropTable.class);
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
  public void testSetQuotaAndThenDropTableWithNoInserts() throws Exception {
    setQuotaAndThenDropTable(SpaceViolationPolicy.NO_INSERTS);
  }

  @Test
  public void testSetQuotaAndThenDropTableWithNoWrite() throws Exception {
    setQuotaAndThenDropTable(SpaceViolationPolicy.NO_WRITES);
  }

  @Test
  public void testSetQuotaAndThenDropTableWithNoWritesCompactions() throws Exception {
    setQuotaAndThenDropTable(SpaceViolationPolicy.NO_WRITES_COMPACTIONS);
  }

  @Test
  public void testSetQuotaAndThenDropTableWithDisable() throws Exception {
    setQuotaAndThenDropTable(SpaceViolationPolicy.DISABLE);
  }

  private void setQuotaAndThenDropTable(SpaceViolationPolicy policy) throws Exception {
    Put put = new Put(Bytes.toBytes("to_reject"));
    put.addColumn(Bytes.toBytes(SpaceQuotaHelperForTests.F1), Bytes.toBytes("to"),
        Bytes.toBytes("reject"));

    // Do puts until we violate space policy
    final TableName tn = helper.writeUntilViolationAndVerifyViolation(policy, put);

    // Now, drop the table
    TEST_UTIL.deleteTable(tn);
    LOG.debug("Successfully deleted table ", tn);

    // Now re-create the table
    TEST_UTIL.createTable(tn, Bytes.toBytes(SpaceQuotaHelperForTests.F1));
    LOG.debug("Successfully re-created table ", tn);

    // Put some rows now: should not violate as table/quota was dropped
    helper.verifyNoViolation(tn, put);
  }
}
