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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.AsyncAdmin;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.quotas.QuotaUtil;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExternalResource;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test that we can split and merge the quota table given the presence of various configuration
 * settings.
 */
@Category({ MiscTests.class, LargeTests.class })
@RunWith(Parameterized.class)
public class TestSplitMergeQuotaTable {

  private static final Logger LOG = LoggerFactory.getLogger(TestSplitMergeQuotaTable.class);

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestSplitMergeQuotaTable.class);

  @Parameterized.Parameters(name = "{1}")
  public static Object[][] params() {
    Map<String, String> quotasDisabledMap = new HashMap<>();
    quotasDisabledMap.put(QuotaUtil.QUOTA_CONF_KEY, "false");
    Map<String, String> quotasEnabledMap = new HashMap<>();
    quotasEnabledMap.put(QuotaUtil.QUOTA_CONF_KEY, "true");
    return new Object[][] { { quotasDisabledMap }, { quotasEnabledMap }, };
  }

  private final TableName tableName = QuotaUtil.QUOTA_TABLE_NAME;
  private final MiniClusterRule miniClusterRule;

  @Rule
  public final RuleChain ruleChain;

  public TestSplitMergeQuotaTable(Map<String, String> configMap) {
    this.miniClusterRule = MiniClusterRule.newBuilder().setConfiguration(() -> {
      Configuration conf = HBaseConfiguration.create();
      conf.setInt(HConstants.HBASE_CLIENT_META_OPERATION_TIMEOUT, 1000);
      conf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 2);
      configMap.forEach(conf::set);
      return conf;
    }).build();
    TestRule ensureQuotaTableRule = new ExternalResource() {
      @Override
      protected void before() throws Throwable {
        try (AsyncConnection conn = ConnectionFactory
          .createAsyncConnection(miniClusterRule.getTestingUtility().getConfiguration())
          .get(30, TimeUnit.SECONDS)) {
          AsyncAdmin admin = conn.getAdmin();
          if (!admin.tableExists(QuotaUtil.QUOTA_TABLE_NAME).get(30, TimeUnit.SECONDS)) {
            miniClusterRule.getTestingUtility().getHBaseCluster().getMaster()
              .createSystemTable(QuotaUtil.QUOTA_TABLE_DESC);
          }
        }
      }
    };
    this.ruleChain = RuleChain.outerRule(miniClusterRule).around(ensureQuotaTableRule);
  }

  @Test
  public void testSplitMerge() throws Exception {
    HBaseTestingUtility util = miniClusterRule.getTestingUtility();
    util.waitTableAvailable(tableName, 30_000);
    try (AsyncConnection conn =
      ConnectionFactory.createAsyncConnection(util.getConfiguration()).get(30, TimeUnit.SECONDS)) {
      AsyncAdmin admin = conn.getAdmin();
      admin.split(tableName, Bytes.toBytes(0x10)).get(30, TimeUnit.SECONDS);
      util.waitFor(30_000, new Waiter.ExplainingPredicate<Exception>() {

        @Override
        public boolean evaluate() throws Exception {
          // can potentially observe the parent and both children via this interface.
          return admin.getRegions(tableName)
            .thenApply(val -> val.stream()
              .filter(info -> info.getReplicaId() == RegionInfo.DEFAULT_REPLICA_ID)
              .collect(Collectors.toList()))
            .get(30, TimeUnit.SECONDS).size() > 1;
        }

        @Override
        public String explainFailure() {
          return "Split has not finished yet";
        }
      });
      util.waitUntilNoRegionsInTransition();
      List<RegionInfo> regionInfos = admin.getRegions(tableName)
        .thenApply(
          val -> val.stream().filter(info -> info.getReplicaId() == RegionInfo.DEFAULT_REPLICA_ID)
            .collect(Collectors.toList()))
        .get(30, TimeUnit.SECONDS);
      assertEquals(2, regionInfos.size());
      LOG.info("{}", regionInfos);
      admin
        .mergeRegions(
          regionInfos.stream().map(RegionInfo::getRegionName).collect(Collectors.toList()), false)
        .get(30, TimeUnit.SECONDS);
      util.waitFor(30000, new Waiter.ExplainingPredicate<Exception>() {

        @Override
        public boolean evaluate() throws Exception {
          // can potentially observe the parent and both children via this interface.
          return admin.getRegions(tableName)
            .thenApply(val -> val.stream()
              .filter(info -> info.getReplicaId() == RegionInfo.DEFAULT_REPLICA_ID)
              .collect(Collectors.toList()))
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
}
