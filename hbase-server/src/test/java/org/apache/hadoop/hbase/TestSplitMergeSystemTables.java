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
package org.apache.hadoop.hbase;

import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.AsyncAdmin;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.quotas.QuotaTableUtil;
import org.apache.hadoop.hbase.quotas.QuotaUtil;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test that we can split and merge select system tables given the presence of various configuration
 * settings.
 */
@Category({ MiscTests.class, LargeTests.class })
@RunWith(Parameterized.class)
public class TestSplitMergeSystemTables {

  private static final Logger LOG = LoggerFactory.getLogger(TestSplitMergeSystemTables.class);

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestSplitMergeSystemTables.class);

  @Parameterized.Parameters(name = "{0}: {1}")
  public static Object[][] params() {
    return new Object[][] {
      // quotas table is only created when the quota system is enabled.
      { QuotaTableUtil.QUOTA_TABLE_NAME, Map.of(QuotaUtil.QUOTA_CONF_KEY, "true") }, };
  }

  private final TableName tableName;

  @Rule
  public final MiniClusterRule miniClusterRule;

  public TestSplitMergeSystemTables(TableName tableName, Map<String, String> configMap) {
    this.tableName = tableName;
    this.miniClusterRule = MiniClusterRule.newBuilder().setConfiguration(() -> {
      Configuration conf = HBaseConfiguration.create();
      conf.setInt(HConstants.HBASE_CLIENT_META_OPERATION_TIMEOUT, 1000);
      conf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 2);
      configMap.forEach(conf::set);
      return conf;
    }).build();

  }

  @Test
  public void testSplitMergeSystemTable() throws Exception {
    HBaseTestingUtil util = miniClusterRule.getTestingUtility();
    util.waitTableAvailable(tableName, 30_000);
    AsyncAdmin admin = util.getAsyncConnection().getAdmin();
    admin.split(tableName, Bytes.toBytes(0x10)).get(30, TimeUnit.SECONDS);
    util.waitFor(30_000, new Waiter.ExplainingPredicate<Exception>() {

      @Override
      public boolean evaluate() throws Exception {
        // can potentially observe the parent and both children via this interface.
        return admin.getRegions(tableName)
          .thenApply(val -> val.stream()
            .filter(info -> info.getReplicaId() == RegionInfo.DEFAULT_REPLICA_ID).toList())
          .get(30, TimeUnit.SECONDS).size() > 1;
      }

      @Override
      public String explainFailure() {
        return "Split has not finished yet";
      }
    });
    util.waitUntilNoRegionsInTransition();
    List<RegionInfo> regionInfos = admin.getRegions(tableName)
      .thenApply(val -> val.stream()
        .filter(info -> info.getReplicaId() == RegionInfo.DEFAULT_REPLICA_ID).toList())
      .get(30, TimeUnit.SECONDS);
    assertEquals(2, regionInfos.size());
    LOG.info("{}", regionInfos);
    admin.mergeRegions(regionInfos.stream().map(RegionInfo::getRegionName).toList(), false).get(30,
      TimeUnit.SECONDS);
    util.waitFor(30000, new Waiter.ExplainingPredicate<Exception>() {

      @Override
      public boolean evaluate() throws Exception {
        // can potentially observe the parent and both children via this interface.
        return admin.getRegions(tableName)
          .thenApply(val -> val.stream()
            .filter(info -> info.getReplicaId() == RegionInfo.DEFAULT_REPLICA_ID).toList())
          .get(30, TimeUnit.SECONDS).size() == 1;
      }

      @Override
      public String explainFailure() {
        return "Merge has not finished yet";
      }
    });
    assertEquals(1, admin.getRegions(tableName).get(30, TimeUnit.SECONDS).size());
  }
}
