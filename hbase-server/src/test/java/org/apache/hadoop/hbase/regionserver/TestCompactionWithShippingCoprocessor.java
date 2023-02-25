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

import static org.hamcrest.MatcherAssert.assertThat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.hamcrest.Matchers;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

@Category({ LargeTests.class, ClientTests.class })
public class TestCompactionWithShippingCoprocessor {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestCompactionWithShippingCoprocessor.class);

  protected final static HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();
  private static final byte[] FAMILY = Bytes.toBytes("testFamily");

  @Rule
  public TestName name = new TestName();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 0);// do not retry
    TEST_UTIL.startMiniCluster(1);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testCoprocScannersExtendingShipperGetShipped() throws Exception {
    int shippedCountBefore = DelegatingInternalScanner.SHIPPED_COUNT.get();
    final TableName tableName = TableName.valueOf(name.getMethodName());
    // Create a table with block size as 1024
    final Table table = TEST_UTIL.createTable(tableName, new byte[][] { FAMILY }, 1, 1024,
      NoOpScanPolicyObserver.class.getName());
    TEST_UTIL.loadTable(table, FAMILY);
    TEST_UTIL.flush();
    try {
      // get the block cache and region
      RegionLocator locator = TEST_UTIL.getConnection().getRegionLocator(tableName);
      String regionName = locator.getAllRegionLocations().get(0).getRegion().getEncodedName();
      HRegion region = TEST_UTIL.getRSForFirstRegionInTable(tableName).getRegion(regionName);
      // trigger a major compaction
      TEST_UTIL.compact(true);
      assertThat(DelegatingInternalScanner.SHIPPED_COUNT.get(),
        Matchers.greaterThan(shippedCountBefore));
    } finally {
      table.close();
    }
  }
}
