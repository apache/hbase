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
package org.apache.hadoop.hbase;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.IOException;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.MiniZooKeeperCluster;
import org.apache.hbase.thirdparty.com.google.common.collect.Iterables;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

/**
 * Test whether moved region cache is correct
 */
@Category({ MiscTests.class, MediumTests.class })
public class TestMovedRegionCache {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestMovedRegionCache.class);

  @Rule
  public TestName name = new TestName();

  private HBaseTestingUtility UTIL;
  private MiniZooKeeperCluster zkCluster;
  private HRegionServer source;
  private HRegionServer dest;
  private RegionInfo movedRegionInfo;

  @Before
  public void setup() throws Exception {
    UTIL = new HBaseTestingUtility();
    zkCluster = UTIL.startMiniZKCluster();
    StartMiniClusterOption option = StartMiniClusterOption.builder().numRegionServers(2).build();
    MiniHBaseCluster cluster = UTIL.startMiniHBaseCluster(option);
    source = cluster.getRegionServer(0);
    dest = cluster.getRegionServer(1);
    assertEquals(2, cluster.getRegionServerThreads().size());
    TableName tableName = TableName.valueOf(name.getMethodName());
    UTIL.createTable(tableName, Bytes.toBytes("cf"));
    UTIL.waitTableAvailable(tableName, 30_000);
    movedRegionInfo = Iterables.getOnlyElement(cluster.getRegions(tableName)).getRegionInfo();
    UTIL.getAdmin().move(movedRegionInfo.getEncodedNameAsBytes(), source.getServerName());
    UTIL.waitFor(2000, new Waiter.Predicate<IOException>() {
      @Override
      public boolean evaluate() throws IOException {
        return source.getOnlineRegion(movedRegionInfo.getRegionName()) != null;
      }
    });
  }

  @After
  public void after() throws Exception {
    UTIL.shutdownMiniCluster();
    if (zkCluster != null) {
      zkCluster.shutdown();
    }
  }

  @Test
  public void testMovedRegionsCache() throws IOException, InterruptedException {
    UTIL.getAdmin().move(movedRegionInfo.getEncodedNameAsBytes(), dest.getServerName());
    UTIL.waitFor(2000, new Waiter.Predicate<IOException>() {
      @Override
      public boolean evaluate() throws IOException {
        return dest.getOnlineRegion(movedRegionInfo.getRegionName()) != null;
      }
    });
    assertNotNull("Moved region NOT in the cache!", source.getMovedRegion(
      movedRegionInfo.getEncodedName()));
    Thread.sleep(source.movedRegionCacheExpiredTime());
    assertNull("Expired moved region exist in the cache!", source.getMovedRegion(
      movedRegionInfo.getEncodedName()));
  }
}
