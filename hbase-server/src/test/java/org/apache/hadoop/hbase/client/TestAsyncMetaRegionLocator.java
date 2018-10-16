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

import static org.junit.Assert.assertEquals;

import java.util.Optional;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter.ExplainingPredicate;
import org.apache.hadoop.hbase.master.balancer.BaseLoadBalancer;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
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

  private static AsyncRegistry REGISTRY;

  private static AsyncMetaRegionLocator LOCATOR;

  @BeforeClass
  public static void setUp() throws Exception {
    TEST_UTIL.getConfiguration().set(BaseLoadBalancer.TABLES_ON_MASTER, "none");
    TEST_UTIL.startMiniCluster(3);
    TEST_UTIL.waitUntilAllSystemRegionsAssigned();
    TEST_UTIL.getAdmin().setBalancerRunning(false, true);
    REGISTRY = AsyncRegistryFactory.getRegistry(TEST_UTIL.getConfiguration());
    LOCATOR = new AsyncMetaRegionLocator(REGISTRY);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    IOUtils.closeQuietly(REGISTRY);
    TEST_UTIL.shutdownMiniCluster();
  }

  private Optional<ServerName> getRSCarryingMeta() {
    return TEST_UTIL.getHBaseCluster().getRegionServerThreads().stream()
        .map(t -> t.getRegionServer())
        .filter(rs -> !rs.getRegions(TableName.META_TABLE_NAME).isEmpty()).findAny()
        .map(rs -> rs.getServerName());
  }

  @Test
  public void testReload() throws Exception {
    ServerName serverName = getRSCarryingMeta().get();
    assertEquals(serverName, LOCATOR.getRegionLocation(false).get().getServerName());

    ServerName newServerName = TEST_UTIL.getHBaseCluster().getRegionServerThreads().stream()
        .map(t -> t.getRegionServer().getServerName()).filter(sn -> !sn.equals(serverName))
        .findAny().get();
    TEST_UTIL.getAdmin().move(HRegionInfo.FIRST_META_REGIONINFO.getEncodedNameAsBytes(),
      Bytes.toBytes(newServerName.getServerName()));
    TEST_UTIL.waitFor(30000, new ExplainingPredicate<Exception>() {

      @Override
      public boolean evaluate() throws Exception {
        Optional<ServerName> newServerName = getRSCarryingMeta();
        return newServerName.isPresent() && !newServerName.get().equals(serverName);
      }

      @Override
      public String explainFailure() throws Exception {
        return HRegionInfo.FIRST_META_REGIONINFO.getRegionNameAsString() + " is still on " +
            serverName;
      }
    });
    // The cached location will not change
    assertEquals(serverName, LOCATOR.getRegionLocation(false).get().getServerName());
    // should get the new location when reload = true
    assertEquals(newServerName, LOCATOR.getRegionLocation(true).get().getServerName());
    // the cached location should be replaced
    assertEquals(newServerName, LOCATOR.getRegionLocation(false).get().getServerName());
  }
}
