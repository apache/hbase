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
package org.apache.hadoop.hbase.client;

import static org.apache.hadoop.hbase.client.RegionReplicaTestHelper.testLocator;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionReplicaTestHelper.Locator;
import org.apache.hadoop.hbase.master.balancer.BaseLoadBalancer;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ MediumTests.class, ClientTests.class })
public class TestAsyncMetaRegionLocator {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestAsyncMetaRegionLocator.class);

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private static ConnectionRegistry REGISTRY;

  private static AsyncMetaRegionLocator LOCATOR;

  @BeforeClass
  public static void setUp() throws Exception {
    TEST_UTIL.getConfiguration().setBoolean(BaseLoadBalancer.TABLES_ON_MASTER, false);
    TEST_UTIL.getConfiguration().setInt(HConstants.META_REPLICAS_NUM, 3);
    TEST_UTIL.startMiniCluster(3);
    REGISTRY = ConnectionRegistryFactory.getRegistry(TEST_UTIL.getConfiguration());
    RegionReplicaTestHelper.waitUntilAllMetaReplicasAreReady(TEST_UTIL, REGISTRY);
    TEST_UTIL.getAdmin().balancerSwitch(false, true);
    LOCATOR = new AsyncMetaRegionLocator(REGISTRY);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    IOUtils.closeQuietly(REGISTRY);
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void test() throws Exception {
    testLocator(TEST_UTIL, TableName.META_TABLE_NAME, new Locator() {

      @Override
      public void updateCachedLocationOnError(HRegionLocation loc, Throwable error)
          throws Exception {
        LOCATOR.updateCachedLocationOnError(loc, error);
      }

      @Override
      public RegionLocations getRegionLocations(TableName tableName, int replicaId, boolean reload)
          throws Exception {
        return LOCATOR.getRegionLocations(replicaId, reload).get();
      }
    });
  }
}
